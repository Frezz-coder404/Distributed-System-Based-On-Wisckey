// =============================================================================
// client.cc - 用于连接 DisKV Master 的简单客户端
// =============================================================================
// 用法:
//   ./client [ip] [port]
//   默认连接 127.0.0.1:8888
//
// 支持命令（与 telnet 下相同）:
//   PUT <key> <value>
//   GET <key>
//   DELETE <key>
//   SCAN <start_key> <end_key>
//   QUIT 退出客户端,断开与从节点的连接,告知主节点已断开
// =============================================================================

#include <iostream>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unordered_map>
#include <vector>
#include <sstream>

const std::string DEFAULT_HOST = "127.0.0.1";
const int DEFAULT_PORT = 8888;

// 路由表条目结构体
struct RouteEntry {
    int index;              // 从节点索引
    std::string ip;         // 从节点 IP 地址
    int port;               // 从节点客户端监听端口
    int range_start;        // 该从节点负责的 key 首字符 ASCII 范围起始
    int range_end;          // 该从节点负责的 key 首字符 ASCII 范围结束
};

// 将 路由表条目结构体 封装成数组，构成全局路由表
std::vector<RouteEntry> route_table;

// 从节点持久连接映射表：worker_index -> fd
// 用于记录套接字与从节点编号的关系，从路由表中进行索引
std::unordered_map<int, int> slave_fds;

// 去除字符串末尾的换行符（用于从 std::getline 读取的用户输入）
static std::string trim_newline(const std::string& s) {
    auto end = s.find_last_not_of("\r\n");
    if (end != std::string::npos)
        return s.substr(0, end + 1);
    return s;
}

// 去除首尾空白
static std::string trim(const std::string& s) {
    auto start = s.find_first_not_of(" \t\r\n");
    auto end = s.find_last_not_of(" \t\r\n");
    if (start == std::string::npos) return "";
    return s.substr(start, end - start + 1);
}

// 根据 key 的首字符查找对应的从节点索引，用于单点查询
int find_worker_for_key(const std::string& key) {
    if (key.empty() || route_table.empty()) return -1;
    unsigned char ch = static_cast<unsigned char>(key[0]);
    // 遍历路由表，找到符合的从节点，返回索引
    for (const auto& entry : route_table) {
        if (ch >= entry.range_start && ch <= entry.range_end) {
            return entry.index;
        }
    }
    return -1; // 未找到对应的从节点
}

// 由 索引值 获取指定从节点的 文件描述符 ，依此来获取连接；若不存在连接则自动创建并加入slave_fds映射表
int get_slave_fd(int worker_index) {
    // 检查是否已有连接
    auto it = slave_fds.find(worker_index);
    if (it != slave_fds.end() && it->second >= 0) {
        // 如果有连接，直接返回对应的文件描述符
        return it->second;
    }

    // 如果 worker_index 小于 0 ，或者大于路由表条目数，说明索引无效，需要创建新连接。
    if (worker_index < 0 || worker_index >= (int)route_table.size()) return -1;
    const RouteEntry& entry = route_table[worker_index];

    // 创建套接字
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;
    // 准备地址结构
    struct sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(entry.port);
    if (inet_pton(AF_INET, entry.ip.c_str(), &addr.sin_addr) <= 0) {
        close(sock);
        return -1;
    }
    // 连接从节点
    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(sock);
        return -1;
    }

    slave_fds[worker_index] = sock;
    return sock;
}

// 解析主节点返回的路由表字符串，并依次构造路由表
// 格式：
// ROUTETABLE
// INDEX | IP:PORT | RANGE
// 0  127.0.0.1:9000  0-42
// 1  127.0.0.1:9001  43-85
// 2  127.0.0.1:9002  86-127
// END
bool parse_route_table(const std::string& response) {
    // 清空之前可能残留的路由表数据
    route_table.clear();

    // 使用字符串输入流
    std::istringstream iss(response);
    std::string line;

    // 状态标志：是否已经遇到 ROUTETABLE 开头标记，
    // 只有在该标记之后的数据行才会被解析，防止处理表头之前的垃圾数据
    bool in_table = false;

    // 按行读取整个响应字符串，iss 会在遇到换行符时停止，将一行的内容放入变量line
    while (std::getline(iss, line)) {
        // 由于网络传输可能带有回车符 \r (CR)，需要去掉末尾的 \r
        if (!line.empty() && line.back() == '\r') line.pop_back();

        // 去除行首和行尾的空格等空白字符
        line = trim(line);

        // 检测到 "ROUTETABLE" 起始标记，设置状态为"在表中"，并继续读取下一行
        if (line == "ROUTETABLE") {
            in_table = true;
            continue;
        }
        // 跳过表头行
        if (line.find("INDEX") == 0) {
            continue;
        }
        // 检测到结束标记 "END"，终止解析，跳出循环
        if (line == "END") {
            break;
        }
        // 如果还没有进入路由表区域（未遇到 ROUTETABLE），忽略当前行的内容，防止读入垃圾数据
        if (!in_table) continue;

        // 正式解析数据行，格式: <index> <ip>:<port> <range_start>-<range_end>
        std::istringstream line_iss(line);
        int index;
        std::string ip_port;
        std::string range_str;
        // 尝试从行流中读取三个字段：整数索引、字符串IP:PORT、范围
        // 如果读取失败，说明格式错误，跳过该行
        if (!(line_iss >> index >> ip_port >> range_str)) continue;

        // 解析 IP:PORT
        size_t colon_pos = ip_port.find(':');
        if (colon_pos == std::string::npos) continue;
        std::string ip = ip_port.substr(0, colon_pos);
        int port = std::stoi(ip_port.substr(colon_pos + 1));

        // 解析 RANGE (格式: start-end)
        size_t dash_pos = range_str.find('-');
        if (dash_pos == std::string::npos) continue;
        int range_start = std::stoi(range_str.substr(0, dash_pos));
        int range_end = std::stoi(range_str.substr(dash_pos + 1));

        // 将解析出的数据加入路由条目
        RouteEntry entry;
        entry.index = index;
        entry.ip = ip;
        entry.port = port;
        entry.range_start = range_start;
        entry.range_end = range_end;
        route_table.push_back(entry);
    }
    // 如果解析后路由表不为空，说明成功解析到有效路由条目，返回1
    return !route_table.empty();
}

// 从指定从节点读取单行响应，返回字符串（用于 PUT/GET/DELETE）
std::string read_slave_response(int fd) {
    std::string response;
    char buf[4096];
    while (true) {
        ssize_t n = read(fd, buf, sizeof(buf) - 1);
        if (n < 0) {
            if (errno == EINTR) continue;
            std::cerr << "读取从节点 PUT/GET/DELETE 响应错误" << std::endl;
            return "";
        } else if (n == 0) {
            std::cout << "从节点关闭了连接" << std::endl;
            return "";
        }
        buf[n] = '\0';
        response += buf;
        // 单行响应以 \r\n 或 \n 结束
        if (response.find("\r\n") != std::string::npos || response.find("\n") != std::string::npos) {
            break;
        }
    }
    return response;
}

// 从指定从节点读取多行响应，返回字符串（用于 SCAN）
// 读取直到遇到 "END\r\n" 或 "END\n"
std::string read_slave_range_response(int fd) {
    std::string response;
    char buf[4096];
    while (true) {
        ssize_t n = read(fd, buf, sizeof(buf) - 1);
        if (n < 0) {
            if (errno == EINTR) continue;
            std::cerr << "读取从节点 SCAN 响应错误" << std::endl;
            return "";
        } else if (n == 0) {
            std::cout << "从节点关闭了连接" << std::endl;
            return "";
        }
        buf[n] = '\0';
        response += buf;
        // SCAN 响应以 END\r\n 或 END\n 结束
        if (response.find("END\r\n") != std::string::npos || response.find("END\n") != std::string::npos) {
            break;
        }
    }
    return response;
}

// 连接到指定IP与端口的节点，用于主动连接到主节点，返回套接字 fd
int connect_to_master(const std::string& host, int port) {
    // 创建套接字
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;
    // 准备地址结构
    struct sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        close(sock);
        return -1;
    }
    // 连接
    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(sock);
        return -1;
    }
    // 返回套接字对应的文件描述符
    return sock;
}

int main(int argc, char* argv[]) {
    std::string host = DEFAULT_HOST;
    int port = DEFAULT_PORT;

    // 读取 IP 与 port 参数2
    if (argc >= 2) host = argv[1];
    if (argc >= 3) port = std::stoi(argv[2]);

    // 显示输入提示
    std::cout << "DisKV 客户端已启动" << std::endl;
    std::cout << "已连接到主节点: " << host << ":" << port << std::endl;
    std::cout << "请输入 HELLO 连接主节点获取路由表" << std::endl;

    // 发送/接收主循环
    std::string user_input;
    while (true) {
        std::cout << ">> ";
        // 接收用户输入
        if (!std::getline(std::cin, user_input)) {
            // 若用户按 Ctrl+D 或输入 EOF
            std::cout << "\n退出客户端" << std::endl;
            break;
        }

        // 去除末尾换行符
        std::string input = trim_newline(user_input);
        if (input.empty()) continue;

        // 解析用户命令
        std::istringstream iss(input);
        std::string cmd;
        iss >> cmd;
        // 统一转为大写,方便比较
        for (auto& c : cmd) c = std::toupper(static_cast<unsigned char>(c));

        // ==================== HELLO：连接主节点获取路由表 ====================
        if (cmd == "HELLO") {
            // 连接到主节点
            int master_sock = connect_to_master(host, port);
            if (master_sock < 0) {
                std::cerr << "错误: 无法连接到主节点 " << host << ":" << port << std::endl;
                continue;
            }
            std::cout << "已连接到 Master (" << host << ":" << port << ")" << std::endl;

            // 发送 HELLO 命令
            std::string hello_req = "HELLO\n";
            ssize_t sent = write(master_sock, hello_req.c_str(), hello_req.size());
            if (sent < 0) {
                std::cerr << "发送 HELLO 失败" << std::endl;
                close(master_sock);
                continue;
            }

            // 读取路由表响应（阻塞循环直到收到 END）
            std::string route_response;
            char recv_buf[4096];
            while (true) {
                ssize_t n = read(master_sock, recv_buf, sizeof(recv_buf) - 1);
                if (n < 0) {
                    std::cerr << "读取路由表响应错误" << std::endl;
                    close(master_sock);
                    break;
                } else if (n == 0) {
                    break; // 主节点主动断开连接（正常行为）
                }
                recv_buf[n] = '\0';
                route_response += recv_buf; // 将路由表存储在字符串中
                // 检查是否已收到完整的路由表（以 END 结束），如读取完成，退出循环
                if (route_response.find("END\r\n") != std::string::npos ||
                    route_response.find("END\n") != std::string::npos) {
                    break;
                }
            }

            // 关闭与主节点的连接
            close(master_sock);
            std::cout << "已从 Master 获取路由表并断开连接" << std::endl;

            // 打印路由表
            std::cout << "--- 路由表 ---" << std::endl;
            std::cout << route_response;
            if (!route_response.empty() && route_response.back() != '\n')
                std::cout << std::endl;
            std::cout << "--------------" << std::endl;

            // 解析路由表
            if (!parse_route_table(route_response)) {
                std::cerr << "错误: 解析路由表失败" << std::endl;
                continue;
            }
            std::cout << "路由表解析成功, 共 " << route_table.size() << " 个从节点" << std::endl;
        }

        // ==================== QUIT：退出客户端 ====================
        else if (cmd == "QUIT") {
            // 连接主节点
            int quit_sock = connect_to_master(host, port);
            if (quit_sock >= 0) {
                std::string quit_req = "QUIT\n";
                // 发送 QUIT 请求
                ssize_t nw = write(quit_sock, quit_req.c_str(), quit_req.size());
                if (nw < 0) {
                    std::cerr << "发送 QUIT 失败" << strerror(errno) << std::endl;
                    continue;
                }
                // 读取 BYE 响应
                char buf[64] = {0};
                ssize_t nr = read(quit_sock, buf, sizeof(buf) - 1);
                if (nr < 0) {
                    std::cerr << "读取从节点 BYE 响应错误" << std::endl;
                    continue;
                } else if (nr == 0) {
                    std::cout << "从节点关闭了连接" << std::endl;
                    close(quit_sock);
                    break;
                }
                close(quit_sock); // TODO 需要检查读入的确是bye才能断开。
            }
            std::cout << "已退出客户端" << std::endl;
            break;
        }

        // ==================== PUT：写入键值对 ====================
        else if (cmd == "PUT") {
            std::string key, value;
            iss >> key; // 取出 key
            // value 允许包含空格，取剩余部分
            std::string rest;
            std::getline(iss, rest); // 取出 value
            value = trim(rest);
            if (key.empty() || value.empty()) {
                std::cout << "ERROR: PUT requires key and value" << std::endl;
                continue;
            }

            // 根据路由表查找 key 对应的从节点
            int worker_idx = find_worker_for_key(key);
            if (worker_idx < 0) {
                std::cout << "ERROR: no slave found for key (please run HELLO first)" << std::endl;
                continue;
            }

            // 获取（或创建）与从节点的持久连接
            int slave_fd = get_slave_fd(worker_idx);
            if (slave_fd < 0) {
                std::cout << "ERROR: cannot connect to slave " << worker_idx << std::endl;
                slave_fds.erase(worker_idx);
                continue;
            }

            // 直接发送 PUT 命令到从节点
            std::string request = "PUT " + key + " " + value + "\n";
            ssize_t w_sent = write(slave_fd, request.c_str(), request.size());
            if (w_sent < 0) {
                std::cerr << "发送 PUT 到从节点失败" << std::endl;
                close(slave_fd);
                slave_fds.erase(worker_idx);
                continue;
            }

            // 读取从节点响应，原样输出
            std::string response = read_slave_response(slave_fd);
            if (response.empty()) {
                close(slave_fd);
                slave_fds.erase(worker_idx);
                continue;
            }
            std::cout << trim(response) << std::endl;
        }

        // ==================== GET：读取键值对 ====================
        // 用户直接输入 GET 命令，客户端原样发送给对应从节点
        else if (cmd == "GET") {
            std::string key;
            iss >> key; // 取出 key
            if (key.empty()) {
                std::cout << "ERROR: GET requires a key" << std::endl;
                continue;
            }

            // 根据路由表查找 key 对应的从节点
            int worker_idx = find_worker_for_key(key);
            if (worker_idx < 0) {
                std::cout << "ERROR: no slave found for key (please run HELLO first)" << std::endl;
                continue;
            }

            // 获取（或创建）与从节点的持久连接
            int slave_fd = get_slave_fd(worker_idx);
            if (slave_fd < 0) {
                std::cout << "ERROR: cannot connect to slave " << worker_idx << std::endl;
                slave_fds.erase(worker_idx);
                continue;
            }

            // 直接发送 GET 命令到从节点
            std::string request = "GET " + key + "\n";
            ssize_t w_sent = write(slave_fd, request.c_str(), request.size());
            if (w_sent < 0) {
                std::cerr << "发送 GET 到从节点失败" << std::endl;
                close(slave_fd);
                slave_fds.erase(worker_idx);
                continue;
            }

            // 读取从节点响应，原样输出
            std::string response = read_slave_response(slave_fd);
            if (response.empty()) {
                close(slave_fd);
                slave_fds.erase(worker_idx);
                continue;
            }
            std::cout << trim(response) << std::endl;
        }

        // ==================== DELETE：删除键值对 ====================
        // 用户直接输入 DELETE 命令，客户端原样发送给对应从节点
        else if (cmd == "DELETE") {
            std::string key;
            iss >> key; // 取出 key
            if (key.empty()) {
                std::cout << "ERROR: DELETE requires a key" << std::endl;
                continue;
            }

            // 根据路由表查找 key 对应的从节点
            int worker_idx = find_worker_for_key(key);
            if (worker_idx < 0) {
                std::cout << "ERROR: no slave found for key (please run HELLO first)" << std::endl;
                continue;
            }

            // 获取（或创建）与从节点的持久连接
            int slave_fd = get_slave_fd(worker_idx);
            if (slave_fd < 0) {
                std::cout << "ERROR: cannot connect to slave " << worker_idx << std::endl;
                slave_fds.erase(worker_idx);
                continue;
            }

            // 直接发送 DELETE 命令到从节点
            std::string request = "DELETE " + key + "\n";
            ssize_t w_sent = write(slave_fd, request.c_str(), request.size());
            if (w_sent < 0) {
                std::cerr << "发送 DELETE 到从节点失败" << std::endl;
                close(slave_fd);
                slave_fds.erase(worker_idx);
                continue;
            }

            // 读取从节点响应，原样输出
            std::string response = read_slave_response(slave_fd);
            if (response.empty()) {
                close(slave_fd);
                slave_fds.erase(worker_idx);
                continue;
            }
            std::cout << trim(response) << std::endl;
        }

        // ==================== SCAN：范围查询 ====================
        // 用户直接输入 SCAN 命令，客户端原样发送给对应从节点
        else if (cmd == "SCAN") {
            std::string first_key, last_key;
            iss >> first_key >> last_key; // 取出 first_key 和 last_key
            if (first_key.empty() || last_key.empty()) {
                std::cout << "ERROR: SCAN requires first_key and last_key" << std::endl;
                continue;
            }
            if (first_key > last_key) {
                std::cout << "ERROR: SCAN first_key must be <= last_key" << std::endl;
                continue;
            }

            // 计算涉及的从节点索引起始范围
            int start_worker = find_worker_for_key(first_key);
            int end_worker = find_worker_for_key(last_key);
            if (start_worker < 0 || end_worker < 0) {
                std::cout << "ERROR: cannot determine slave for range (please run HELLO first)" << std::endl;
                continue;
            }

            // 向每个涉及的从节点发送 SCAN 请求，收集结果
            // 由于 索引 与 存储范围 正相关，因此只需要从 起始索引点 递增扫描到 终止索引点 即可。
            std::string all_results;
            for (int idx = start_worker; idx <= end_worker; ++idx) {
                // 获取（或创建）与从节点的持久连接
                int slave_fd = get_slave_fd(idx);
                if (slave_fd < 0) {
                    std::cerr << "警告: 无法连接到从节点 " << idx << std::endl;
                    slave_fds.erase(idx);
                    continue;
                }

                // 直接发送 SCAN 命令到从节点
                std::string request = "SCAN " + first_key + " " + last_key + "\n";
                ssize_t w_sent = write(slave_fd, request.c_str(), request.size());
                if (w_sent < 0) {
                    std::cerr << "发送 SCAN 到从节点 " << idx << " 失败" << std::endl;
                    close(slave_fd);
                    slave_fds.erase(idx);
                    continue;
                }

                // 读取 SCAN 多行响应
                std::string response = read_slave_range_response(slave_fd);
                if (response.empty()) {
                    close(slave_fd);
                    slave_fds.erase(idx);
                    continue;
                }

                // 解析响应，提取 KVPAIR 行
                std::istringstream resp_iss(response);
                std::string resp_line;
                while (std::getline(resp_iss, resp_line)) {
                    if (!resp_line.empty() && resp_line.back() == '\r') resp_line.pop_back(); // 去除 \r
                    if (resp_line.empty()) continue;
                    if (resp_line == "END") continue; // 跳过单个从节点的 END 标记
                    if (resp_line.find("KVPAIR ") == 0) { // 找到每行的KVPAIR，用于定位
                        // KVPAIR key value -> key value
                        std::string kv = resp_line.substr(7); // 取出每行的 key value 部分
                        all_results += kv + "\r\n"; // 存入结果字符串
                    }
                }
            }
            // 所有从节点的结果合并后，输出统一 END 标记
            // 由于节点间和节点内都是有序的，直接合并即可
            std::cout << all_results;
            std::cout << "END" << std::endl;
        }

        else {
            std::cout << "ERROR: Unknown command. Use HELLO, PUT, GET, DELETE, SCAN, QUIT" << std::endl;
        }
    }

    // 关闭所有从节点连接
    for (auto& pair : slave_fds) {
        if (pair.second >= 0) close(pair.second);
    }
    slave_fds.clear();

    return 0;
}