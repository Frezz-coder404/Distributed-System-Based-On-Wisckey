// =============================================================================
// kv_master.cc - 基于 Wisckey (LevelDB) 持久化路由表的主节点
// =============================================================================

#include <iostream>      // std::cout, std::cerr
#include <sys/epoll.h>   // epoll_create1(), epoll_ctl(), epoll_wait()
#include <cstring>       // memset() 用于清空内存
#include <unistd.h>      // close(), read(), write()
#include <sys/socket.h>  // socket(), bind(), listen(), accept()
#include <netinet/in.h>  // sockaddr_in, htons(), htonl()
#include <arpa/inet.h>   // inet_ntoa, inet_ntop
#include <errno.h>       // errno, EAGAIN, EWOULDBLOCK
#include <fcntl.h>       // fcntl, F_SETFL, O_NONBLOCK
#include <unordered_map> // std::unordered_map ; map采用BVL实现, unordered_map采用哈希表实现。
#include <string>        // std::string
#include <algorithm>     // std::find_if_not, std::isspace
#include <vector>        // std::vector
#include <csignal>       // signal() 处理退出信号
#include <utility>       // std::pair
#include <sstream>       // std::istringstream

// 引入 Wisckey 的头文件
#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/status.h"
#include "leveldb/slice.h"
#include "leveldb/iterator.h"

const int MAX_EVENTS = 64;           // 最多可容纳的epoll事件数
const int MASTER_CLIENT_PORT = 8888; // master 的连接 client 的端口常量
const int MASTER_WORKER_PORT = 8889; // master 的连接 worker 的端口常量
const int WORKER_COUNT = 3;          // worker 数量

// 为支持多从节点，将单个 g_worker_fd 改为用 vector 存储多个 g_worker_fd
std::vector<int> g_worker_fds;       // 存储所有 worker 连接的文件描述符

// Wisckey 元数据库全局指针, g_meta_db 用于指向数据库
// 对相应的数据库进行各种操作，或者读取其中数据，如
// 持久化的 master 的路由表（key -> ValueLocation）
leveldb::DB* g_meta_db = nullptr;
// 运行标志，供信号处理器使用, 当接收到 退出 信号时，设置为 false 以退出主循环
static volatile bool g_running = true;

// 将一个文件描述符设置为非阻塞模式（ET 模式需要，LT 模式可选但建议）
void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

// 对于每个客户端连接的数据设置:缓冲区
struct Connection {
    std::string send_buffer;   // 用户态发送缓冲区，存储待发送的数据
    std::string recv_buffer;   // 用户态接收缓冲区，存储要接收的数据
};

// 对于每个存储的值，记录其位置信息
struct ValueLocation {
    int worker_index;          // 存储该值的 worker 在 g_worker_fds 中的索引
    std::string value_id;      // worker 内部存储的值的id，不妨设为与用户指定的key相同
};

// --- 初始化空映射表：fd <-> 用户态缓冲区(send_buffer & recv_buffer) ---
// 这行的作用是维护一个映射表，记录每个客户端连接的"用户态发送缓冲区"与"用户态接收缓冲区"。
// 每个客户端的"用户态发送缓冲区"和"用户态接收缓冲区"必须独立，以便在发送时正确地回写到对应的客户端，并在接收时正确识别指令。
// 与回显服务器的功能不同，回显服务器只需要一个"用户态接收缓冲区"即可，哪怕一次只收到一个字符也可以立即回显。
// 但是这里必须将"用户态接收缓冲区"独立，因为指令必须读取完整才能进行下一步，没读完的指令存放在独立的缓冲区中，以防止网络异步乱序造成的指令混乱。
std::unordered_map<int, Connection> connections;

// --- 初始化空映射表：key <-> 路由信息(worker_index & value_id) ---
// 映射表 kv_store 中的值不仅需要存储 worker 内部 id，还需要存储 worker 索引，以便 GET 时能向正确的 worker 发起 FETCH 请求。
// 现在 kv_store 仅作为内存中的缓存加速查询，而 Wisckey 中的数据作为持久化备份
std::unordered_map<std::string, ValueLocation> kv_store;
//int next_key_id = 1;  // 用于生成新键

// 信号处理器
// 若收到 SIGTERM 信号，即意外关闭或kill杀，记为信号1
// 若收到 SIGINT (Ctrl+C) 信号，即正常关闭，记为信号2
void signal_handler(int signum) {
    std::cout << "\n收到退出信号: " << signum << "，正在安全关闭数据库..." << std::endl;
    g_running = false; // 告诉主循环要退出了，该处理数据落盘与数据库关闭了
}

// 序列化/反序列化辅助函数：
// 将 ValueLocation 序列化为字符串，格式为 "worker_index:value_id"
// 便于以字符串形式存储到 Wisckey 中，对路由信息持久化存储
static std::string serialize_value_location(const ValueLocation& loc) {
    return std::to_string(loc.worker_index) + ":" + loc.value_id;
}

// 将序列化字符串反序列化为 ValueLocation
static ValueLocation deserialize_value_location(const std::string& s) {
    ValueLocation loc;
    size_t colon_pos = s.find(':');
    if (colon_pos != std::string::npos) {
        loc.worker_index = std::stoi(s.substr(0, colon_pos));
        loc.value_id = s.substr(colon_pos + 1);
    }
    return loc;
}

// 向指定的 worker 发送 STORE 请求，返回存储的内部 ID（字符串形式）
std::string store_on_worker(int worker_index, const std::string& key, const std::string& value) {
    // 如果 worker_index 小于 0 ，或大于等于容器 g_worker_fds 的大小，即正常连接的worker数，说明连接无效。
    if (worker_index < 0 || worker_index >= (int)g_worker_fds.size()) return "";
    // 从 g_worker_fds 中获取对应 worker 的文件描述符
    int worker_fd = g_worker_fds[worker_index];
    if (worker_fd < 0) return "";

    // 发送请求（阻塞 write，数据量小，通常一次写完）// TODO 改为非阻塞模式
    // 向指定的 worker 发送 STORE <key> <value> 二级命令, worker 处理后会发回 STORED 响应命令。
    std::string request = "STORE " + key + " " + value + "\n";
    ssize_t n = write(worker_fd, request.c_str(), request.size());
    if (n <= 0) {
        std::cerr << "向 worker (index=" << worker_index << ") 写入 STORE 请求失败" << std::endl;
        return "";
    }

    char buf[256] = {0};
    // 读取响应（阻塞read，简单可靠）
    // 读取 worker 发回的 STORED 响应命令。
    n = read(worker_fd, buf, sizeof(buf) - 1);
    if (n <= 0) {
        std::cerr << "从 worker (index=" << worker_index << ") 读取 STORED 响应失败" << std::endl;
        return "";
    }

    // 响应命令 STORED 。
    std::string response(buf, n);
    // 期望格式: "STORED\n"
    if (response.find("STORED") == 0) {
        return key;   // 直接用客户端 key 作为 value_id，因为二者是相同的
    }
    return ""; // NOT_FOUND 或其他错误
}

// 向指定的 worker 发送 FETCH 请求，返回实际值
std::string fetch_from_worker(int worker_index, const std::string& id_str) {
    // 如果 worker_index 小于0 ，或大于等于容器 g_worker_fds 的大小，即正常连接的worker数，说明连接无效。
    if (worker_index < 0 || worker_index >= (int)g_worker_fds.size()) return "";
    // 从 g_worker_fds 中获取对应 worker 的文件描述符
    int worker_fd = g_worker_fds[worker_index];
    if (worker_fd < 0) return "";

    // 发送请求（阻塞 write，数据量小，通常一次写完）// TODO 改为非阻塞模式
    // 向指定的 worker 发送 FETCH <id> 二级命令, worker 处理后会发回 VALUE <value> 响应命令。
    std::string request = "FETCH " + id_str + "\n";
    ssize_t n = write(worker_fd, request.c_str(), request.size());
    if (n <= 0) {
        std::cerr << "向 worker (index=" << worker_index << ") 写入 FETCH 请求失败" << std::endl;
        return "";
    }

    char buf[1024] = {0};
    // 读取响应（阻塞read，简单可靠）
    // 读取 worker 发回的 VALUE <value> 响应命令。
    n = read(worker_fd, buf, sizeof(buf) - 1);
    if (n <= 0) {
        std::cerr << "从 worker (index=" << worker_index << ") 读取 VALUE 响应失败" << std::endl;
        return "";
    }

    // 解析响应命令，从 VALUE <value> 中提取出 <value> 。
    std::string response(buf, n);
    if (response.find("VALUE ") == 0) {
        // 去除前缀 "VALUE "：
        std::string value = response.substr(6);
        // 去除末尾换行 "\n" 和 "\r" ：
        while (!value.empty() && (value.back() == '\n' || value.back() == '\r'))
            value.pop_back();
        // 只剩下 <value> 了，返回这个字符串：
        return value;
    }
    return "";  // NOT_FOUND 或其他错误
}

// 向指定的 worker 发送 DELETE 请求，返回是否成功true/false
bool delete_on_worker(int worker_index, const std::string& id_str) {
    // 如果 worker_index 小于0 ，或大于等于容器 g_worker_fds 的大小，即正常连接的worker数，说明连接无效。
    if (worker_index < 0 || worker_index >= (int)g_worker_fds.size()) return false;
    // 从 g_worker_fds 中获取对应 worker 的文件描述符
    int worker_fd = g_worker_fds[worker_index];
    if (worker_fd < 0) return false;

    // 发送请求（阻塞 write，数据量小，通常一次写完）// TODO 改为非阻塞模式
    // 向指定的 worker 发送 DELETE <id> 二级命令, worker 处理后会发回 DELETED 响应命令。
    std::string request = "DELETE " + id_str + "\n";
    ssize_t n = write(worker_fd, request.c_str(), request.size());
    if (n <= 0) {
        std::cerr << "向 worker (index=" << worker_index << ") 写入 DELETE 请求失败" << std::endl;
        return false;
    }

    char buf[256] = {0};
    // 读取响应（阻塞read，简单可靠）
    // 读取 worker 发回的 DELETED 响应命令。
    n = read(worker_fd, buf, sizeof(buf) - 1);
    if (n <= 0) {
        std::cerr << "从 worker (index=" << worker_index << ") 读取 DELETED 响应失败" << std::endl;
        return false;
    }

    std::string response(buf, n);
    // 期望响应格式: "DELETED\n" 或 "NOT_FOUND\n"
    if (response.find("DELETED") == 0) {
        return true; // 找到DELETED响应，说明删除成功
    }
    return false; // 找到NOT_FOUND响应，说明删除失败
}

// 向指定的 worker 发送 RANGE <start> <end> 请求，返回该区间内所有键值对列表 vector<pair<key, value>>
std::vector<std::pair<std::string, std::string>> range_scan_worker(int worker_index, const std::string& start, const std::string& end) {
    std::vector<std::pair<std::string, std::string>> results;
    // 如果 worker_index 小于0 ，或大于等于容器 g_worker_fds 的大小，即正常连接的worker数，说明连接无效。
    if (worker_index < 0 || worker_index >= (int)g_worker_fds.size()) return results;
    // 从 g_worker_fds 中获取对应 worker 的文件描述符
    int worker_fd = g_worker_fds[worker_index];
    if (worker_fd < 0) return results;

    std::string request = "RANGE " + start + " " + end + "\n";
    ssize_t n = write(worker_fd, request.c_str(), request.size());
    if (n <= 0) {
        std::cerr << "向 worker (index=" << worker_index << ") 写入 RANGE 请求失败" << std::endl;
        return results;
    }

    // 循环读取直到收到来自 worker 的 "END\n" // TODO 改为非阻塞模式
    std::string recv_buf;
    char buf[1024]; // 可能会被写满，但我们会循环读取直到 "END\n" 出现。
    while (true) {
        // 读取 worker 发回的 KVPAIR 响应命令。
        n = read(worker_fd, buf, sizeof(buf) - 1); // 同主函数中一样，read 必须指定一个已初始化的缓冲区，也就是 buf
        if (n <= 0) {
            std::cerr << "从 worker (index=" << worker_index << ") 读取 RANGE 响应失败" << std::endl;
            break;
        }
        recv_buf.append(buf, n); // 再将 buf 中的数据放入 recv_buf 中，同时转换为字符串状态。
        // 查找到 "END\n" 的位置
        size_t end_pos = recv_buf.find("END\n");
        if (end_pos != std::string::npos) {
            // 取出 END 之前的所有数据，并移除已处理部分
            std::string data = recv_buf.substr(0, end_pos); // 提取 "END\n" 之前的所有数据到 data 字符串中
            recv_buf.erase(0, end_pos + 4);                 // 清空 recv_buf 中已处理的部分，保留 "END\n" 之后的数据（如果有的话）
            // 按行解析，每行的格式为"KVPAIR <key> <value>"
            std::istringstream iss(data);  // 创建一个输入字符串流对象 iss，以便逐行读取 data 的内容
            std::string line;                  // 定义一个字符串 line，用于存储从流中读出的每一行文本
            while (std::getline(iss, line)) { // 循环：每次从 iss 中读取一行放入 line
                if (line.empty()) continue; // 跳过可能的空行
                if (line.compare(0, 7, "KVPAIR ") == 0) { // 判断该行是否以 "KVPAIR " 开头
                    std::string kv_part = line.substr(7); // 跳过 "KVPAIR " 开始提取余下的子串，得到key value部分
                    size_t sep = kv_part.find(' '); // 找到第一个空格，依此分离 key 和 value
                    if (sep != std::string::npos) {
                        std::string key = kv_part.substr(0, sep);
                        std::string value = kv_part.substr(sep + 1);
                        results.emplace_back(key, value); // 在 results 容器的末尾添加该键值对元素
                    }
                }
            }
            break;
        }
    }
    return results;
}

// 根据 key 的首字符，采用 ASCII 均分 Range 分区策略分配 worker 索引
// Worker 0: 首字符 ASCII 0 ~ 42  : 从 空字符(0) 到 '*' (42)
// Worker 1: 首字符 ASCII 43 ~ 85 : 从 '+' (43) 到 'U' (85)
// Worker 2: 首字符 ASCII 86 ~ 127: 从 'V' (86) 到 DEL (127)
// TODO 改为动态分区策略
int get_worker_for_key(const std::string& key) {
    if (key.empty()) return 0; // 健壮性保护
    unsigned char ch = static_cast<unsigned char>(key[0]);
    if (ch >= 0 && ch <= 42) {
        return 0;
    } else if (ch >= 43 && ch <= 85) {
        return 1;
    } else { // 任何其它字符，如中文字符，会进入 worker2
        return 2;
    }
}

// 内联函数：去除字符串首尾空白字符（telnet 输入可能带有多余空格）
static inline std::string trim(const std::string &s) {
    auto start = std::find_if_not(s.begin(), s.end(), [](unsigned char ch) {
        return std::isspace(ch);
    });
    auto end = std::find_if_not(s.rbegin(), s.rend(), [](unsigned char ch) {
        return std::isspace(ch);
    }).base();
    return (start < end) ? std::string(start, end) : std::string();
}

// 函数：处理一条完整的命令（以 '\n' 结尾，但传入的 line 已去除换行符）
std::string process_command(const std::string& line) {
    std::string trimmed = trim(line);
    if (trimmed.empty()) return ""; // 忽略空行

    // 找到第一个空格，分离命令cmd和参数arg
    size_t space_pos = trimmed.find(' ');
    std::string cmd, arg;
    if (space_pos != std::string::npos) { // 成功找到命令与参数
        cmd = trimmed.substr(0, space_pos);
        arg = trimmed.substr(space_pos + 1);
    } else {
        cmd = trimmed; // 没有参数，例如仅 "PUT" 或 "GET" 或 "DELETE"
    }

    // 统一转为大写以便比较
    for (auto& c : cmd) c = std::toupper(static_cast<unsigned char>(c));

    // ==================== PUT：写入键值对 ====================
    // * 总而言之：先将值存入worker，再保存路由信息。
    if (cmd == "PUT") {
        // 解析 key 和 value，格式: PUT <key> <value>
        std::string key_value = trim(arg);
        size_t first_space = key_value.find(' ');
        if (first_space == std::string::npos) {
            return "ERROR: PUT requires key and value\r\n";
        }
        std::string key = trim(key_value.substr(0, first_space));
        std::string value = trim(key_value.substr(first_space + 1));
        if (key.empty() || value.empty()) {
            return "ERROR: PUT requires key and value\r\n";
        }

        // 根据 key 首字符 Range 分区选择 worker
        int selected_worker = get_worker_for_key(key);

        // 将值存储到指定 worker，如果存储成功，直接返回 key ，不用再绕远路去返回 id 。
        std::string value_id = store_on_worker(selected_worker, key, value);
        if (value_id.empty()) {
            return "ERROR: failed to store value on worker\r\n";
        }// TODO 若worker写入成功，但master更新映射表前崩溃，可能导致数据丢失。

        // 构造路由信息
        ValueLocation loc = {selected_worker, value_id};

        // 将路由信息同时写入内存 kv_store 和 Wisckey 元数据库
        // 原版仅写入内存: kv_store[key] = {selected_worker, value_id};
        kv_store[key] = loc;
        // 将键值对（key + 路由信息）持久化到 Wisckey
        leveldb::Status s = g_meta_db->Put(leveldb::WriteOptions(),
                                            leveldb::Slice(key),
                                            leveldb::Slice(serialize_value_location(loc))); //序列化为字符串格式再存入
        if (!s.ok()) {
            std::cerr << "警告: 路由信息写入 Wisckey 失败: " << s.ToString() << std::endl;
        }

        return "OK " + key + "\r\n";

    // ==================== GET：读取键值对 ====================
    // * 总而言之：先获取路由信息，再将值从worker中取出。
    } else if (cmd == "GET") {
        std::string key = trim(arg);
        if (key.empty()) {
            return "ERROR: GET requires a key\r\n";
        }

        // 优先查内存缓存，若未命中则查 Wisckey 元数据库
        auto it = kv_store.find(key); // 迭代器，直到找到 <key>

        if (it == kv_store.end() && g_meta_db != nullptr) {
            // 内存缓存未命中，尝试从 Wisckey 恢复路由信息
            std::string loc_str;
            leveldb::Status s = g_meta_db->Get(leveldb::ReadOptions(),
                                                leveldb::Slice(key), &loc_str);
            // 从 Wisckey 找到路由信息，加载到内存缓存，再查内存缓存
            if (s.ok()) {
                ValueLocation loc = deserialize_value_location(loc_str); //将字符串格式的路由信息转换为结构体格式
                kv_store[key] = loc;
                it = kv_store.find(key);
                std::cout << "从 Wisckey 恢复路由: key=" << key
                          << " -> (worker_index=" << loc.worker_index
                          << ", value_id=" << loc.value_id << ")" << std::endl;
            }
        }

        // 根据路由信息向对应的 worker 发起 FETCH 请求，获取实际值
        if (it != kv_store.end()) {
            std::string value = fetch_from_worker(it->second.worker_index, it->second.value_id);
            if (value.empty()) {
                return "ERROR: value not found on worker\r\n"; // !可能在worker中已将值删除，但是master中的键与路由条目未被删除时出现此报错,请确保操作原子完成、数据已落盘。
            }
            return "VALUE " + value + "\r\n";                  // 在找到值得时候出现此回复
        } else {
            return "NOT FOUND\r\n";                            // 在路由信息不存在时，如未添加或被删除时出现此回复
        }

    // ==================== DELETE：删除键值对 ====================
    // * 总而言之：先获取路由信息，再将值从worker中删去值，然后从master中删除路由信息。
    } else if (cmd == "DELETE") {
        std::string key = trim(arg);
        if (key.empty()) {
            return "ERROR: DELETE requires a key\r\n";
        }

        // 优先查内存缓存，若未命中则查 Wisckey 元数据库
        auto it = kv_store.find(key); // 迭代器，直到找到 <key>

        if (it == kv_store.end() && g_meta_db != nullptr) {
            // 内存缓存未命中，尝试从 Wisckey 恢复
            std::string loc_str;
            leveldb::Status s = g_meta_db->Get(leveldb::ReadOptions(),
                                                leveldb::Slice(key), &loc_str);
            // 从 Wisckey 找到路由信息，加载到内存缓存，再查内存缓存
            if (s.ok()) {
                ValueLocation loc = deserialize_value_location(loc_str); //将字符串格式的路由信息转换为结构体格式
                kv_store[key] = loc;
                it = kv_store.find(key);
            }
        }

        // 根据路由信息向对应的 worker 发起 DELETE 请求，删除实际值
        if (it != kv_store.end()) {
            bool deleted = delete_on_worker(it->second.worker_index, it->second.value_id);
            if (deleted) {
                // 同时从内存 kv_store 和 Wisckey 元数据库中删除
                kv_store.erase(it); // TODO 若worker删除成功，但master从映射表中移除前崩溃了，可能导致映射表残留。
                // 从 Wisckey 中删除路由信息
                g_meta_db->Delete(leveldb::WriteOptions(), leveldb::Slice(key));
                // 问：当内存中和磁盘中的某条路由信息被删除后，分别会发生什么？中间空缺的这部分会怎么处理？
                // 答：当数据删除或被覆盖，LSM-Tree中不会将其真的删除，而是贴上删除标记，并在后续的回收过程(合并与恢复操作会调用回收)中真正删除。

                return "DELETED\r\n";
            } else {
                return "ERROR: failed to delete value on worker\r\n";
            }
        } else {
            return "NOT FOUND\r\n";
        }

    // ==================== SCAN：高效范围查询 ====================
    // * 总而言之：直接向所有从节点发送"扫描"请求，将所有结果合并后返回给客户端。
    } else if (cmd == "SCAN") {
        // 解析 first_key 和 last_key，格式：SCAN <first_key> <last_key>
        std::string keys_arg = trim(arg);
        size_t sep = keys_arg.find(' ');
        // 如果没有找到空格，说明缺少两个参数，返回错误提示
        if (sep == std::string::npos) {
            return "ERROR: SCAN requires first_key and last_key\r\n";
        }
        std::string first_key = trim(keys_arg.substr(0, sep));
        std::string last_key = trim(keys_arg.substr(sep + 1));
        // 如果任一键为空，说明缺少一个参数，返回错误提示
        if (first_key.empty() || last_key.empty()) {
            return "ERROR: SCAN requires first_key and last_key\r\n";
        }
        // 如果 <first_key> 大于 <last_key>，说明参数顺序错误，返回错误提示
        if (first_key > last_key) {
            return "ERROR: SCAN first_key must be <= last_key\r\n";
        }

        // 计算可能涉及的 worker 索引范围
        int start_worker = get_worker_for_key(first_key);
        int end_worker = get_worker_for_key(last_key);
        // 调用 range_scan_worker，扫描可能的 worker 的区间内键值对到 all_pairs 列表中
        std::vector<std::pair<std::string, std::string>> all_pairs;
        for (int i = start_worker; i <= end_worker; ++i) {
            auto pairs = range_scan_worker(i, first_key, last_key); // 存储单个worker扫描的结果
            all_pairs.insert(all_pairs.end(), pairs.begin(), pairs.end()); // 存储所有扫描的结果
        }

        // 将键值对列表 all_pairs 转换为一整个字符串，以便向客户端发送。
        std::string result;
        for (const auto& kv : all_pairs) {
            result += kv.first + " " + kv.second + "\r\n";
        }
        result += "END\r\n";
        return result;

    } else {
        return "ERROR: Unknown command. Use PUT <value> or GET <key> or DELETE <key> or SCAN <first_key> <last_key>\r\n";
    }
}

int main() {
    // 注册信号处理器：当收到中断信号(SIGINT/SIGTERM)时，调用函数(signal_handler)。
    std::signal(SIGINT, signal_handler);  // SIGINT的作用是捕获键盘的Ctrl+C信号
    std::signal(SIGTERM, signal_handler); // SIGTERM的作用是捕获进程意外中断信号

    // 打开 Master 的 Wisckey 元数据库
    // Master 使用独立的 Wisckey 数据库目录存储路由元数据
    std::string meta_db_path = "./wisckey_db_master";

    // 设置相关参数：
    leveldb::Options meta_options;
    meta_options.create_if_missing = true;                   // 如果数据库不存在则创建
    // Wisckey 特有参数（控制 vlog 垃圾回收行为）
    meta_options.clean_write_buffer_size = 4 * 1024 * 1024;  // GC 写缓冲区大小（必须大于12），单位：Bytes
    meta_options.clean_threshold = 1000;                     // vlog 垃圾记录条数达到此阈值时开始 GC，单位：条目数
    meta_options.min_clean_threshold = 500;                  // 手动清理时的最小垃圾记录阈值，单位：条目数
    meta_options.log_dropCount_threshold = 100;              // 合并后新产生此数量垃圾记录时持久化 vloginfo，单位：个数
    meta_options.max_vlog_size = 32 * 1024 * 1024;           // 单个 vlog 文件大小上限，单位：Bytes

    // 打开数据库
    leveldb::Status status = leveldb::DB::Open(meta_options, meta_db_path, &g_meta_db);
    if (!status.ok()) {
        std::cerr << "Master: 无法打开 Wisckey 元数据库: " << status.ToString() << std::endl;
        return 1;
    }
    std::cout << "Master: Wisckey 元数据库已打开, 路径: " << meta_db_path << std::endl;

    // 从 Wisckey 恢复 kv_store 路由表：遍历 Wisckey 中的所有键值对
    leveldb::Iterator* it = g_meta_db->NewIterator(leveldb::ReadOptions());
    int recovered_count = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::string key = it->key().ToString();
        // 将路由信息存入内存缓存中
        std::string loc_str = it->value().ToString();
        ValueLocation loc = deserialize_value_location(loc_str);
        kv_store[key] = loc;
        recovered_count++;
    }
    if (!it->status().ok()) {
        std::cerr << "Master: 恢复路由表时遍历 Wisckey 出错: " << it->status().ToString() << std::endl;
    }
    delete it;  // 释放迭代器

    std::cout << "Master: 从 Wisckey 恢复了 " << recovered_count << " 条路由记录" << std::endl;

    // ---------- 1. 创建空套接字 ----------
    // socket()返回值: 失败返回 -1，成功返回一个非负整数，称为"套接字文件描述符"，
    // 即"file descriptor"，通常被写作 _fd ，用于标志一个套接字。

    // 连接到客户端的：// TODO 修改命名
    int master_client_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (master_client_fd < 0) {
        std::cerr << "创建 master_client 套接字失败" << std::endl;
        delete g_meta_db; //关闭数据库
        return 1;
    }
    // 设置端口复用（方便调试，避免"Address already in use"，注：端口复用不会影响吞吐量）
    int opt_c = 1;
    setsockopt(master_client_fd, SOL_SOCKET, SO_REUSEADDR, &opt_c, sizeof(opt_c));

    // 连接到从节点的：
    int master_worker_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (master_worker_fd < 0) {
        std::cerr << "创建 master_worker 套接字失败" << std::endl;
        delete g_meta_db;
        return 1;
    }
    int opt_w = 1;
    setsockopt(master_worker_fd, SOL_SOCKET, SO_REUSEADDR, &opt_w, sizeof(opt_w));

    // ---------- 2. 准备服务器地址结构 ----------
    // 作用：设置地址的 IP:Port 结构，一遍接下来与套接字关联

    // 连接到客户端的：
    struct sockaddr_in master_client_addr;
    memset(&master_client_addr, 0, sizeof(master_client_addr));
    // 使用 IPv4
    master_client_addr.sin_family = AF_INET;
    // INADDR_LOOPBACK 是一个宏，值对应 127.0.0.1 (回环地址)。
    // htonl() 将"主机字节序（小端）"的 32 位整数转换成"网络字节序（大端）"。
    master_client_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    // htons() 将"主机字节序（小端）"的 16 位整数转换成"网络字节序（大端）"。
    // 8888 是任意选择的端口号，只要大于 1024（避免与系统服务冲突）。
    master_client_addr.sin_port = htons(MASTER_CLIENT_PORT);

    // 连接到从节点的：
    struct sockaddr_in master_worker_addr;
    memset(&master_worker_addr, 0, sizeof(master_worker_addr));
    master_worker_addr.sin_family = AF_INET;
    master_worker_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    master_worker_addr.sin_port = htons(MASTER_WORKER_PORT);

    // ---------- 3. 绑定地址到套接字 ----------
    // 作用: 将套接字与上述创建的 IP 地址和端口关联起来。服务器必须绑定，
    //       否则客户端不知道应该连接到哪里。

    // 连接到客户端的：
    if (bind(master_client_fd, (struct sockaddr*)&master_client_addr, sizeof(master_client_addr)) < 0) {
        std::cerr << "绑定地址失败" << std::endl;
        close(master_client_fd); // 失败后关闭套接字，释放资源
        delete g_meta_db;    // 失败后关闭数据库
        return 1;
    }

    // 连接到从节点的：
    if (bind(master_worker_fd, (struct sockaddr*)&master_worker_addr, sizeof(master_worker_addr)) < 0) {
        std::cerr << "绑定 worker 监听地址失败" << std::endl;
        close(master_worker_fd);
        delete g_meta_db;
        return 1;
    }

    // ---------- 4. 开始监听端口 ----------
    // 作用: 将套接字从主动模式变成被动模式，告诉操作系统开始接收客户端的连接请求。

    // 连接到客户端的：
    if (listen(master_client_fd, 128) < 0) { // 128 是等待连接队列长度
        std::cerr << "监听 client 端口失败" << std::endl;
        close(master_client_fd);
        delete g_meta_db;
        return 1;
    }
    std::cout << "被动监听客户端通道已建立, master_client_fd = " << master_client_fd
              << ", socket: [127.0.0.1:" << MASTER_CLIENT_PORT << "]"
              << " (epoll ET 模式) " << std::endl;

    // 连接到从节点的：
    if (listen(master_worker_fd, 5) < 0) {
        std::cerr << "监听 master_worker 端口失败" << std::endl;
        close(master_worker_fd);
        delete g_meta_db;
        return 1;
    }
    std::cout << "被动监听从节点通道已建立, master_worker_fd = " << master_worker_fd
              <<", socket: [127.0.0.1:" << MASTER_WORKER_PORT << "]"
              << std::endl;

    // ---------- 5. 阻塞连接从节点 ----------
    // 作用：等待所有三个从节点发来的主动连接，并将每个连接的文件描述符保存。
    // 循环直到三个从节点全部加入
    while (g_worker_fds.size() < static_cast<size_t>(WORKER_COUNT)) {
        if (!g_running) {
            close(master_worker_fd);
            delete g_meta_db;
            return 0;
        }
        // 尝试接收新的连接，若接收成功，为该连接分配一个文件描述符
        struct sockaddr_in slave_addr;
        socklen_t slave_len = sizeof(slave_addr);
        int slave_fd = accept(master_worker_fd, (struct sockaddr*)&slave_addr, &slave_len);
        if (slave_fd < 0) {
            if (errno == EINTR) continue;
            std::cerr << "accept slave 连接失败" << std::endl;
            close(master_worker_fd);
            delete g_meta_db;
            return 1;
        }
        // 将连接到的从节点的文件描述符存入数组
        g_worker_fds.push_back(slave_fd);
        // 不为 slave_fd 设置 set_nonblocking 非阻塞模式，因为读写函数用的是阻塞的 write/read 。
        // 打印从节点信息
        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &slave_addr.sin_addr, ip, sizeof(ip));
        int port = ntohs(slave_addr.sin_port);
        std::cout << "工作节点已连接, slave_fd = " << slave_fd << ", worker_index = " << (g_worker_fds.size() - 1)
                << ", socket = " << ip << ":" << port << std::endl;
    }
    std::cout << "已连接到全部 " << WORKER_COUNT << " 个工作节点，关闭 master_worker 监听套接字" << std::endl;
    close(master_worker_fd); // TODO　改为永不关闭，置入epoll监视，以便断开后可重连。

    // ---------- 6. 创建 epoll 实例 ----------
    // 注：epoll_fd 会占用 4 这个值。
    int epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) {
        std::cerr << "创建 epoll 失败" << std::endl;
        close(master_client_fd);
        delete g_meta_db;
        return 1;
    }

    // ---------- 7. 将客户端监听套接字加入 epoll 监视列表 ----------
    // 将服务端的客户端套接字（监听套接字）加入监视列表，这样当有新的客户端连接到该端口时，epoll_wait 就会通知我们。
    struct epoll_event ev;
    ev.events = EPOLLIN;       // 水平LT触发，监听可读。（监听套接字不必使用ET模式，因为新连接事件用LT更简单）
    ev.data.fd = master_client_fd;    // 我们只存 fd，也可以存指针
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, master_client_fd, &ev) < 0) {
        std::cerr << "添加服务端的客户端监听套接字到 epoll 失败" << std::endl;
        close(epoll_fd);
        close(master_client_fd);
        delete g_meta_db;
        return 1;
    }

    // ---------- 8. 循环读取客户端发来的链接或数据 ----------
    struct epoll_event events[MAX_EVENTS];
    while (g_running) {
        // 等待事件发生，timeout超时 -1 表示一直阻塞, 0 表示立即返回，正数表示等待指定毫秒数后返回。
        // timeout采用用较短的1秒超时，以便周期性检查 g_running 标志
        int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, 1000);
        if (nfds < 0) {
            if (!g_running) break;  // 被信号中断，正常退出
            if (errno == EINTR) continue;  // 被信号中断但未要求退出，继续
            std::cerr << "epoll_wait 错误" << std::endl;
            break;
        }

        // 处理每个就绪的epoll实例
        for (int i = 0; i < nfds; ++i) {
            int fd = events[i].data.fd;

            // ---- 8.1 如果就绪的是服务端监听套接字：有新的客户端连接 ----
            if (fd == master_client_fd) {
                // 尝试接收新的连接，若接收成功，为该连接分配一个文件描述符
                struct sockaddr_in client_addr;
                socklen_t client_len = sizeof(client_addr);
                int client_fd = accept(master_client_fd, (struct sockaddr*)&client_addr, &client_len);
                if (client_fd < 0) {
                    // 如果资源不可用，此时errno会被设置成 EAGAIN 或 EWOULDBLOCK（在Linux上两者一致)
                    // 在这里指 accept() 后没有新连接，此时不应视为错误，而是等待epoll再次通知
                    // （如果监听套接字是 ET 模式才可能发生此种情况）
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        continue;
                    }
                    std::cerr << "accept 失败" << std::endl;
                    continue;
                }

                // 设置客户端套接字为非阻塞模式（如果客户端套接字时 ET 模式则必须开启，LT 模式建议）
                set_nonblocking(client_fd);

                // 将新连接的客户端加入映射表
                connections[client_fd] = Connection();

                // 将新连接的客户端套接字也加入 epoll 监视列表，客户端套接字必须使用ET模式。
                // TODO 可添加 EPOLLRDHUP 用于关注客户端链接关闭情况，无需读取数据即可感知关闭，更高效。
                ev.events = EPOLLIN | EPOLLOUT | EPOLLET;  // 设置关注"可读"事件；设置关注"可写"事件；设置为边缘触发(ET)模式，表示当状态发生变化(不可读->可读，不可写->可写)时才会通知。
                ev.data.fd = client_fd;
                if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev) < 0) {
                    std::cerr << "添加客户端套接字到 epoll 失败, client_fd = " << client_fd << std::endl;
                    close(client_fd);
                    continue;
                }

                // 打印新连接的客户端的fd号、IP地址与端口号
                char client_ip[INET_ADDRSTRLEN];
                inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
                int client_port = ntohs(client_addr.sin_port);
                std::cout << "新客户端连接, client_fd = " << client_fd
                          << ", socket = " << client_ip << ":" << client_port << std::endl;
            }
            // ---- 8.2 如果就绪的是普通客户端套接字：有数据可读(有数据来内核接收缓冲区了)或有数据可写(内核发送缓冲区空出来了) ----
            else {
                bool closed = false; // closed 标志用于指示客户端是否关闭连接或发生错误。
                Connection& conn = connections[fd]; // "用户态发送缓冲区"，获取当前连接的"用户态发送缓冲区"引用，以便后续读写操作调用:将send_buffer中的数据发送或将读取的数据写入send_buffer。

                // 检查是否有错误或挂起事件（EPOLLERR | EPOLLHUP），如果发生错误或对端关闭连接，应该立即处理，而不是继续尝试读写。
                if (events[i].events & (EPOLLERR | EPOLLHUP)) {
                    std::cerr << "客户端 client_fd = " << fd << " 发生错误或对端关闭连接" << std::endl;
                    closed = true;
                    // 这里不用 break，而是设置 closed ，会自动走到关闭逻辑
                }

                // ---- 8.2.1 处理可写事件 ----
                if (!closed && (events[i].events & EPOLLOUT)) {
                    // 尝试发送"用户态发送缓冲区"中的数据（循环写直到 EAGAIN(内核发送缓冲区满) 或 发完 ）
                    while (!conn.send_buffer.empty()) {
                        // 将send_buffer中的数据（不含结束符'\0'，因此回写到客户端的数据不会换行）回写给客户端
                        // 回写数据一般不需要ET模式，但是如果因为网络拥塞等问题导致"内核发送缓冲区"满了，send_buffer中的数据就不会完全回写到客户端了。
                        int bytes_write = write(fd, conn.send_buffer.data(), conn.send_buffer.size());
                        if (bytes_write < 0) {
                            // 如果资源不可用，此时errno会被设置成 EAGAIN 或 EWOULDBLOCK（在Linux上两者一致)
                            // 在这里指 write() 后发现"内核发送缓冲区"为满。
                            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                                break; // "内核发送缓冲区"满了，停止发送，下次 EPOLLOUT 再继续
                            } else {
                                std::cerr << "写入错误发生在 client_fd = " << fd << std::endl;
                                closed = true;
                                break;
                            }
                        } else if (bytes_write == 0) {
                            // 应该不会发生
                            std::cerr << "请求写入了 0 个字节! client_fd = " << fd << std::endl;
                            continue;
                        } else {
                            // 成功发送了 n 字节，从发送缓冲区中移除
                            std::cerr << "成功收到来自客户端的信息并发送了 " << bytes_write
                                      << " 字节到 client_fd = " << fd << std::endl;
                            conn.send_buffer.erase(0, bytes_write);
                        }
                    }
                    // 如果"用户态发送缓冲区"已空，则取消 EPOLLOUT 事件。（降低开销，可选）
                    if (conn.send_buffer.empty()) {
                        ev.events = EPOLLIN | EPOLLET; // 只关注读
                        ev.data.fd = fd;
                        if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev) < 0) {
                            std::cerr << "修改客户端套接字状态失败, client_fd = " << fd << std::endl;
                            close(fd);
                            continue;
                        }
                    }
                }

                // ---- 8.2.2 处理可读事件 ----
                if (!closed && (events[i].events & EPOLLIN)) {
                    while (true) { // ET 模式下必须循环读取直到 EAGAIN 出现，表示"内核接收缓冲区"已空，即数据已读完。
                        char temp_buffer[16] = {0}; // "临时数组"，每次只能存n字节，因此需要循环读入直到"内核接收缓冲区"为空。
                        int bytes_read = read(fd, temp_buffer, sizeof(temp_buffer) - 1); // 因为read()函数需要一个已分配了的地址，因此只能先放到temp_buffer中，而不能直接放到conn.recv_buffer中。
                        if (bytes_read > 0) {
                            conn.recv_buffer.append(temp_buffer, bytes_read); // 将"临时数组"中的数据拷贝到"用户态接收缓冲区"。

                            // 循环处理所有完整的行（以 '\n' 结尾）
                            size_t pos;
                            while ((pos = conn.recv_buffer.find('\n')) != std::string::npos) {
                                std::string line = conn.recv_buffer.substr(0, pos);
                                conn.recv_buffer.erase(0, pos + 1); // 移除已处理的行（包括 '\n'）

                                // 放入命令，获得响应，详见 process_command() 内部功能。
                                std::string response = process_command(line);
                                if (!response.empty()) {
                                    conn.send_buffer += response;
                                }
                            }
                        } else if (bytes_read == 0) {
                            std::cout << "客户端 client_fd = " << fd << " 已关闭连接" << std::endl;
                            closed = true;
                            break;
                        } else {
                            // 如果资源不可用，此时errno会被设置成 EAGAIN 或 EWOULDBLOCK（在Linux上两者一致)
                            // 在这里指 read() 后发现"内核接收缓冲区"为空，此时表示数据已读完。
                            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                                // 数据已读完，正常退出读循环
                                break;
                            } else {
                                // 其他错误
                                std::cerr << "客户端 client_fd = " << fd << " 读取数据失败" << std::endl;
                                closed = true;
                                break;
                            }
                        }
                    }
                    // 如果"用户态发送缓冲区"非空，需要注册 EPOLLOUT 事件以便发送
                    if (!conn.send_buffer.empty()) {
                        ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
                        ev.data.fd = fd;
                        if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev) < 0) {
                            std::cerr << "修改客户端套接字状态失败, client_fd = " << fd << std::endl;
                            close(fd);
                            continue;
                        }
                    }
                }

                if (closed) {
                    // 如果客户端关闭连接，或者发生错误，应该从 epoll 中移除并关闭套接字
                    if (epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr) < 0) {
                        std::cerr << "删除客户端套接字对应的文件描述符失败, client_fd = " << fd << std::endl;
                        close(fd);
                        continue;
                    }
                    close(fd);
                    connections.erase(fd); // 并释放缓冲区内存
                }
            }
        }
    }

    // ---------- 9. 关闭套接字 ----------
    // 因为大循环改为了检查g_running而非死循环了,因此程序可能执行到这里。
    // 必须安全关闭数据库，确保数据落盘。
    std::cout << "Master: 正在关闭 Wisckey 元数据库..." << std::endl;
    for (int fd : g_worker_fds) close(fd);
    close(epoll_fd);
    close(master_client_fd);
    delete g_meta_db;  // 关闭 Wisckey 元数据库。delete db 时会将 memtable 中的数据刷盘
    g_meta_db = nullptr;
    std::cout << "Master: 已安全退出" << std::endl;

    return 0;
}
