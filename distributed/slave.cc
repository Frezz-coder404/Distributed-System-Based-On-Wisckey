// =============================================================================
// slave.cc - 基于 Wisckey (LevelDB) 的从节点存储引擎
// =============================================================================

// TODO Wisckey的LSM-Tree只在主节点使用，从节点存储vlog，直接根据传来的id返回值。
#include <iostream>        // std::cout, std::cerr
#include <string>          // std::string
#include <sstream>         // std::istringstream
#include <cstring>         // memset()
#include <unistd.h>        // close(), read(), write()
#include <sys/socket.h>    // socket(), bind(), listen(), accept()
#include <netinet/in.h>    // sockaddr_in, htons(), htonl()
#include <arpa/inet.h>     // inet_ntoa, inet_ntop
#include <algorithm>       // std::find_if_not, std::isspace
#include <cstdlib>         // std::atoi
#include <csignal>         // signal() 处理退出信号

// 引入 Wisckey 的头文件
#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/status.h"
#include "leveldb/slice.h"
#include "leveldb/write_batch.h"

int worker_id = -1;                          // worker 索引，必须在启动时通过参数显式指定
const std::string MASTER_HOST = "127.0.0.1"; // master 的监听地址
const int MASTER_PORT = 8889;                // master 的连接从节点的端口

// 全局数据库指针和运行标志，供信号处理器使用
static leveldb::DB* g_db = nullptr;
static volatile bool g_running = true;

// 去除首尾空白（与主服务器一致）
static inline std::string trim(const std::string &s) {
    auto start = std::find_if_not(s.begin(), s.end(), ::isspace);
    auto end = std::find_if_not(s.rbegin(), s.rend(), ::isspace).base();
    return (start < end) ? std::string(start, end) : "";
}

// 信号处理器
// 若收到 SIGTERM 信号，即意外关闭或kill杀，记为信号1
// 若收到 SIGINT (Ctrl+C) 信号，即正常关闭，记为信号2
void signal_handler(int signum) {
    std::cout << "\n收到退出信号: " << signum << "，正在安全关闭数据库..." << std::endl;
    g_running = false; // 告诉主循环要退出了，该处理数据落盘与数据库关闭了
}

// 原来的代码使用 std::signal() 注册 SIGINT / SIGTERM 的处理函数,
// 在某些 Linux 实现中，signal() 会隐含地给信号动作加上 SA_RESTART 标志，
// 导致被信号中断的 read()/write()/accept() 等慢系统调用自动重启，
// read()/write()/accept() 不会返回，程序便会看起来像是卡死在"正在退出状态"。
void register_signal_handler() {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));              // 将整个结构体清零，避免未初始化的字段带来意外行为
    sa.sa_handler = signal_handler;                  // 设置信号处理函数指针
    sa.sa_flags = 0;                                 // 关键：将sa_flags设为0，即不设置 SA_RESTART
    sigaction(SIGINT, &sa, nullptr);  // 用 sigaction 分别注册 SIGINT（Ctrl+C）和 SIGTERM（kill）
    sigaction(SIGTERM, &sa, nullptr); // 第三个参数为 nullptr，表示不关心旧的信号动作。
}

// main()函数可以接收接收命令行参数，argc 是参数数量，argv[] 是由空格分隔的参数值的数组。
int main(int argc, char* argv[]) {
    // 如果启动时提供了大于等于2个参数（例如 ./slave 0 有两个参数），
    // 则将第二个参数，即 0，由字符串转换为整数，并覆盖 worker_id 的默认值。
    if (argc < 2) {
        std::cerr << "用法: " << argv[0] << " <worker_id (0-2)>" << std::endl;
        return 1;
    }
    worker_id = std::atoi(argv[1]);
    if (worker_id < 0 || worker_id > 2) {
        std::cerr << "错误: worker_id 必须在 0~2 之间" << std::endl;
        return 1;
    }

    // 注册信号处理器，其会在收到信号时自动执行一系列操作
    register_signal_handler();

    // 打开 Wisckey 数据库
    // 每个 worker 使用独立的数据库目录，以端口号区分，避免多 worker 数据冲突
    // "./"是相对路径，因此数据库文件存储在 build 目录下
    std::string db_path = "./wisckey_db_worker_" + std::to_string(worker_id);

    leveldb::Options options;
    options.create_if_missing = true;                       // 如果数据库不存在则自动创建
    // Wisckey 特有参数（控制 vlog 垃圾回收行为）
    options.clean_write_buffer_size = 4 * 1024 * 1024;      // GC 写缓冲区大小（必须大于12）
    options.clean_threshold = 1000;                         // vlog 垃圾记录条数达到此阈值时开始 GC
    options.min_clean_threshold = 500;                      // 手动清理时的最小垃圾记录阈值
    options.log_dropCount_threshold = 100;                  // 合并后新产生此数量垃圾记录时持久化 vloginfo
    options.max_vlog_size = 32 * 1024 * 1024;               // 单个 vlog 文件大小上限（32MB）

    // 打开数据库
    leveldb::DB* db = nullptr;
    leveldb::Status status = leveldb::DB::Open(options, db_path, &db);
    if (!status.ok()) {
        std::cerr << "Worker: 无法打开 Wisckey 数据库: " << status.ToString() << std::endl;
        return 1;
    }
    g_db = db; // 将数据库指针赋值给全局变量，供信号处理器使用
    std::cout << "Worker " << worker_id << ": Wisckey 数据库已打开, 路径: " << db_path << std::endl;

    std::cout << "Worker " << worker_id << ": 尝试连接主服务器 " << MASTER_HOST << ":" << MASTER_PORT << " ..." << std::endl;
    // 从节点主动连接：循环重试直到连接成功
    int master_fd = -1;
    while (g_running) {
        // ---------- 1. 创建空套接字 ----------
        // 这部分主动与被动连接一致。
        master_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (master_fd < 0) {
            std::cerr << "Worker: socket 创建失败" << std::endl;
            sleep(1);
            continue;
        }

        // ---------- 2. 准备服务器地址结构 ----------
        // 主动连接准备的地址说目的地址，即 master 的而非 slave 的。
        struct sockaddr_in master_addr;
        memset(&master_addr, 0, sizeof(master_addr));
        master_addr.sin_family = AF_INET;
        master_addr.sin_port = htons(MASTER_PORT);
        // inet_pton() 与 htonl() 功能基本相同，只是为了更灵活地指定 master 地址（可能更改为其他 IP）。
        if (inet_pton(AF_INET, MASTER_HOST.c_str(), &master_addr.sin_addr) <= 0) {
            std::cerr << "Worker: 无效的 master IP" << std::endl;
            close(master_fd);
            delete db;
            return 1;
        }

        // 主动连接不需要调用 bind()，内核会自动分配一个临时端口并绑定到套接字。

        // ---------- 3. 主动连接到 master ----------
        // 主动连接使用connect(), 被动连接使用 listen() + accept() 。
        if (connect(master_fd, (struct sockaddr*)&master_addr, sizeof(master_addr)) < 0) {
            std::cerr << "Worker: 连接 master 失败, 2秒后重试... (" << strerror(errno) << ")" << std::endl;
            close(master_fd);
            sleep(2);
            continue;
        }
        break; // 连接成功
    }
    // 退出检查，若在连接时退出则立刻关闭，无需进行以下步骤。
    if (!g_running) {
        close(master_fd);
        delete db;
        return 0;
    }

    std::cout << "Worker: 已成功连接到 master, fd = " << master_fd << std::endl;


    // 如果是临时连接(发送数据才建立TCP连接)：只需读取一行，回复，关闭连接
    // 如果是长连接，则处理逻辑如下，必须循环处理：
    while (g_running) {
        char buf[1024] = {0};
        int n = read(master_fd, buf, sizeof(buf) - 1);
        if (n <= 0) {
            if (n == 0) {
                std::cout << "服务端主节点关闭连接, master_fd = " << master_fd << " ，立刻退出" << std::endl;
                break;  // 连接被对方关闭，正常退出
            }
            else {
                if (errno == EINTR) {  // 被信号中断，不是真正的 I/O 错误
                    if (!g_running) {
                        std::cerr << "被退出信号中断，在 master_fd = " << master_fd << " ，立刻退出" << std::endl;
                        break;    // 退出标志已置位 → 退出循环
                    }
                    else {
                        std::cerr << "被非退出信号中断，在 master_fd = " << master_fd << " ，继续运行" << std::endl;
                        continue; // 其他信号 → 重新 read
                    }
                }
                else { // 真正的 I/O 错误，处理或跳出
                    std::cerr << "出现 I/O 错误，在 master_fd = " << master_fd << " ，立刻退出" << std::endl;
                    break;
                }
            }
        }

        std::string request(buf, n);
        // 去除末尾换行符
        while (!request.empty() && (request.back() == '\n' || request.back() == '\r'))
            request.pop_back();

        std::istringstream iss(request);
        std::string cmd, arg;
        iss >> cmd;
        std::getline(iss, arg);
        arg = trim(arg);

        // worker 根据 master 发来的二级命令(STORE/FETCH/DELETE/RANGE)进行处理，
        // 并将响应命令(STORED/VALUE/DELETED/KVPAIR+END)以字符串形式返回给master。
        std::string response;

        // ==================== STORE：写入数据 ====================
        if (cmd == "STORE") {
            // 解析 key 和 value，格式: STORE <key> <value>
            std::string store_arg = arg;   // arg 已经 trim 过，包含 key 和 value 用空格分隔
            size_t spc = store_arg.find(' ');
            if (spc == std::string::npos) {
                response = "ERROR: STORE requires key and value\n";
                ssize_t nw = write(master_fd, response.c_str(), response.size());
                if (nw < 0) {
                    std::cerr << " 向 master 写入失败 " << strerror(errno) << std::endl;
                    break;   // 写失败则终止当前连接的处理循环
                }
                std::cerr << "  -> STORE 参数缺失" << std::endl;
                continue;  // 跳过本次循环
            }
            std::string key = trim(store_arg.substr(0, spc));
            std::string value = trim(store_arg.substr(spc + 1));
            if (key.empty() || value.empty()) {
                response = "ERROR: STORE requires key and value\n";
                ssize_t nw = write(master_fd, response.c_str(), response.size());
                if (nw < 0) {
                    std::cerr << " 向 master 写入失败 " << strerror(errno) << std::endl;
                    break;   // 写失败则终止当前连接的处理循环
                }
                std::cerr << "  -> STORE 的 key 或 value 为空" << std::endl;
                continue;
            }

            std::cout << "收到 STORE 请求: key = \"" << key
                        << "\", value = \"" << value << "\"" << std::endl;

            // 持久化写入
            // sync 字段用于控制当 leveldb 将数据写入到预写日志时，是否同步地调用 fsync() 将内核缓冲区中的数据 flush 到硬盘。
            leveldb::WriteOptions write_opts;
            write_opts.sync = true;
            leveldb::Status put_status = db->Put(write_opts, key, value);

            if (put_status.ok()) {
                // PUT 成功，返回 STORED 响应，无需返回参数 id ，因为id与key一样，主节点直接存key即可。
                response = "STORED\n";
                std::cout << "  -> 已存储为 id = " << key << std::endl;
            } else {
                response = "ERROR store failed\n";
                std::cerr << "  -> Wisckey Put 失败: " << put_status.ToString() << std::endl;
            }
            ssize_t nw = write(master_fd, response.c_str(), response.size());
            if (nw < 0) {
                std::cerr << " 向 master 写入失败 " << strerror(errno) << std::endl;
                break;   // 写失败则终止当前连接的处理循环
            }

        // ==================== FETCH：读取数据 ====================
        } else if (cmd == "FETCH") {
            // 无需解析 key ，格式: FETCH <id>，上方自动解析出的arg就是id，无需再解析
            // 使用 Wisckey 的 Get 接口替代内存 map 查找
            std::cout << "收到 FETCH 请求: id = " << arg << std::endl;

            std::string value;
            leveldb::Status get_status = db->Get(leveldb::ReadOptions(), arg, &value);
            if (get_status.ok()) {
                // 键存在，返回值
                response = "VALUE " + value + "\n";
                ssize_t nw = write(master_fd, response.c_str(), response.size());
                if (nw < 0) {
                    std::cerr << " 向 master 写入失败 " << strerror(errno) << std::endl;
                    break;
                }
                std::cout << "  -> 返回值: \"" << value << "\" " << std::endl;
            } else if (get_status.IsNotFound()) {
                // Wisckey 中未找到该键
                response = "NOT_FOUND\n";
                ssize_t nw = write(master_fd, response.c_str(), response.size());
                if (nw < 0) {
                    std::cerr << " 向 master 写入失败 " << strerror(errno) << std::endl;
                    break;
                }
                std::cout << "  -> 未找到 id " << std::endl;
            } else {
                // 其他读取错误
                response = "ERROR fetch failed\n";
                ssize_t nw = write(master_fd, response.c_str(), response.size());
                if (nw < 0) {
                    std::cerr << " 向 master 写入失败 " << strerror(errno) << std::endl;
                    break;
                }
                std::cerr << "  -> Wisckey Get 错误: " << get_status.ToString() << std::endl;
            }

        // ==================== DELETE：删除数据 ====================
        } else if (cmd == "DELETE") {
            // 使用 Wisckey 的 Get + Delete 接口替代内存 map 删除 =====
            // 现在: 先用 db->Get() 检查键是否存在，再用 db->Delete() 删除
            // 注：Wisckey 的 Delete 不会在键不存在时返回错误，
            //     但为兼容原有协议（区分 DELETED / NOT_FOUND），先做存在性检查
            std::cout << "收到 DELETE 请求: id = " << arg << std::endl;

            std::string value;
            leveldb::Status get_status = db->Get(leveldb::ReadOptions(), arg, &value);
            if (get_status.ok()) { // 键存在
                // 使用 sync=true 确保删除操作持久化
                leveldb::WriteOptions write_opts;
                write_opts.sync = true;

                // 键存在，执行删除
                leveldb::Status del_status = db->Delete(write_opts, arg);
                if (del_status.ok()) { // 删除成功
                    response = "DELETED\n";
                    ssize_t nw = write(master_fd, response.c_str(), response.size());
                    if (nw < 0) {
                        std::cerr << " 向 master 写入失败 " << strerror(errno) << std::endl;
                        break;
                    }
                    std::cout << "  -> 已删除 id = " << arg << std::endl;
                } else { // 删除操作发生失败
                    response = "ERROR delete failed\n";
                    ssize_t nw = write(master_fd, response.c_str(), response.size());
                    if (nw < 0) {
                        std::cerr << " 向 master 写入失败 " << strerror(errno) << std::endl;
                        break;
                    }
                    std::cerr << "  -> Wisckey Delete 失败: " << del_status.ToString() << std::endl;
                }
            } else if (get_status.IsNotFound()) { // 键不存在
                response = "NOT_FOUND\n";
                ssize_t nw = write(master_fd, response.c_str(), response.size());
                if (nw < 0) {
                    std::cerr << " 向 master 写入失败 " << strerror(errno) << std::endl;
                    break;
                }
                std::cout << "  -> 未找到 id, 无法删除 " << std::endl;
            } else { // GET操作发生错误
                response = "ERROR delete check failed\n";
                ssize_t nw = write(master_fd, response.c_str(), response.size());
                if (nw < 0) {
                    std::cerr << " 向 master 写入失败 " << strerror(errno) << std::endl;
                    break;
                }
                std::cerr << "  -> Wisckey Get 检查错误: " << get_status.ToString() << std::endl;
            }

        // ==================== RANGE：范围查询 ====================
        } else if (cmd == "RANGE") {
            // 解析 start_key 和 end_key，格式: RANGE <start_key> <end_key>
            std::string range_arg = arg;   // arg 已经 trim 过
            size_t spc = range_arg.find(' ');
            if (spc == std::string::npos) {
                response = "ERROR: RANGE requires start_key and end_key\n";
                ssize_t nw = write(master_fd, response.c_str(), response.size());
                if (nw < 0) {
                    std::cerr << " 向 master 写入失败 " << strerror(errno) << std::endl;
                    break;   // 写失败则终止当前连接的处理循环
                }
                std::cerr << "  -> RANGE 参数缺失" << std::endl;
                continue;
            }
            std::string start_key = trim(range_arg.substr(0, spc));
            std::string end_key = trim(range_arg.substr(spc + 1));
            if (start_key.empty() || end_key.empty()) {
                response = "ERROR: RANGE requires start_key and end_key\n";
                ssize_t nw = write(master_fd, response.c_str(), response.size());
                if (nw < 0) {
                    std::cerr << " 向 master 写入失败 " << strerror(errno) << std::endl;
                    break;   // 写失败则终止当前连接的处理循环
                }
                std::cerr << "  -> RANGE 的 start_key 或 end_key 为空" << std::endl;
                continue;
            }

            std::cout << "收到 RANGE 请求: start = \"" << start_key
                        << "\", end = \"" << end_key << "\"" << std::endl;

            // 使用 Wisckey 迭代器进行有序范围扫描, 远比一个个 GET 效率来得快.
            leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
            int sent_count = 0;
            // 阻塞扫描并发送所有区间内键值对
            for (it->Seek(start_key); it->Valid() && it->key().ToString() <= end_key; it->Next()) {
                response = "KVPAIR " + it->key().ToString() + " " + it->value().ToString() + "\n";
                ssize_t nw = write(master_fd, response.c_str(), response.size());
                if (nw < 0) {
                    std::cerr << " 向 master 写入失败 " << strerror(errno) << std::endl;
                    break;
                }
                sent_count++;
            }

            if (!it->status().ok()) {
                std::cerr << "  -> RANGE 迭代出错: " << it->status().ToString() << std::endl;
            }
            delete it;

            // 无论是否有结果，都必须以 END 结束，为了告诉主节点"我已发送完毕"
            std::string end_response = "END\n";
            ssize_t nw = write(master_fd, end_response.c_str(), end_response.size());
            if (nw < 0) {
                std::cerr << " 向 master 写入失败 " << strerror(errno) << std::endl;
                break;
            }
            std::cout << "  -> RANGE 完成，发送了 " << sent_count << " 条 KVPAIR" << std::endl;
            // 注：这里不设置 response，因为响应已经在循环中直接发送，最后只需 END

        } else {
            response = "ERROR\n";
            ssize_t nw = write(master_fd, response.c_str(), response.size());
            if (nw < 0) {
                std::cerr << " 向 master 写入失败 " << strerror(errno) << std::endl;
                break;
            }
            std::cout << "收到未知命令: " << cmd << std::endl;
        }
    }
    close(master_fd);

    // 以上不再是死循环，可以执行到此步，安全关闭数据库
    // delete db 会将 memtable 中的数据刷盘为 SSTable 并更新 MANIFEST
    // 这是数据持久化的关键步骤！如果不执行，数据仅存在于 WAL 中
    std::cout << "Slave: 正在关闭 Wisckey 数据库..." << std::endl;
    delete db;  // 关闭 Wisckey 数据库
    g_db = nullptr;
    std::cout << "Slave: 已安全退出" << std::endl;
    return 0;
}
