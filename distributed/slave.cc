// =============================================================================
// slave.cc - 基于 Wisckey (LevelDB) 的从节点存储引擎
// =============================================================================

// TODO Wisckey的LSM-Tree只在主节点使用，从节点存储vlog，直接根据传来的id返回值。
#include <iostream>        // std::cout, std::cerr
#include <string>          // std::string
#include <sstream>         // std::istringstream
#include <cstring>         // memset() 用于清空内存
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

int WORKER_PORT = 8889; // 如果不指明端口，则默认启用的端口号

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

// main()函数可以接收接收命令行参数，argc 是参数数量，argv[] 是由空格分隔的参数值的数组。
int main(int argc, char* argv[]) {
    // 如果启动时提供了大于等于2个参数（例如 ./kv_worker 8890 有两个参数），
    // 则将第二个参数，即8890，由字符串转换为整数，并覆盖 WORKER_PORT 的默认值。
    if (argc >= 2) {
        WORKER_PORT = std::atoi(argv[1]);
    }

    // 注册信号处理器
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    // 打开 Wisckey 数据库
    // 每个 worker 使用独立的数据库目录，以端口号区分，避免多 worker 数据冲突
    // "./"是相对路径，因此数据库文件存储在 build 目录下
    std::string db_path = "./wisckey_db_worker_" + std::to_string(WORKER_PORT);

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
    std::cout << "Worker: Wisckey 数据库已打开, 路径: " << db_path << std::endl;

    // 与 kv_master 中的 main 函数类似，先创建一个套接字。
    // 注意：此时 worker 先启动，被动等待 server 链接，这时的 worker 是 server 的 "server" 。
    int worker_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (worker_fd < 0) {
        std::cerr << "Worker: socket failed\n";
        delete db;  // 关闭数据库
        return 1;
    }

    int opt = 1;
    setsockopt(worker_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // 再准备一个地址+端口，用于和 server 通信。
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(WORKER_PORT);

    // 将套接字与 地址+端口 绑定。
    if (bind(worker_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "Worker: bind failed on port " << WORKER_PORT << "\n";
        close(worker_fd);
        delete db;  // 关闭数据库
        return 1;
    }

    // 监听套接字，准备被动接受连接。
    if (listen(worker_fd, 5) < 0) {
        std::cerr << "Worker: listen failed\n";
        close(worker_fd);
        delete db;
        return 1;
    }

    // 打印从节点信息
    std::cout << "工作节点已启动, worker_fd = " << worker_fd
              << ", 监听端口: [127.0.0.1:" << WORKER_PORT << "]" << std::endl;

    // 修改：不再使用 std::unordered_map<int, std::string> store
    // 原版: std::unordered_map<int, std::string> store;  // 内存中的键值存储
    // 现在所有数据通过 Wisckey 的 db->Put / db->Get / db->Delete 持久化存储

    // 不同于主节点，从节点无需采用epoll轮询方式，因为它只需要处理一个连接（来自主节点的持久连接），不需要同时处理多个客户端连接。
    while (g_running) { // 不再是死循环，而是检查 g_running 标志
        // 循环被动等待链接
        struct sockaddr_in master_addr;
        socklen_t master_len = sizeof(master_addr);
        int master_fd = accept(worker_fd, (struct sockaddr*)&master_addr, &master_len);
        if (master_fd < 0) {
            if (!g_running) break; // 如果是断开连接，则退出循环
            std::cerr << "Worker: accept failed\n";
            continue;              // 如果是其他错误，继续等待新的连接
        }

        // 打印主服务器连接信息
        char master_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &master_addr.sin_addr, master_ip, INET_ADDRSTRLEN);
        int master_port = ntohs(master_addr.sin_port);
        std::cout << "已连接到服务端主节点, master_fd = " << master_fd
                  << ", socket = " << master_ip << ":" << master_port << std::endl;

        // 如果是临时连接(发送数据才建立TCP连接)：只需读取一行，回复，关闭连接
        // 如果是长连接，则处理逻辑如下，必须循环处理：
        while (g_running) {
            char buf[1024] = {0};
            int n = read(master_fd, buf, sizeof(buf) - 1);
            if (n <= 0) {
                if (n == 0)
                    std::cout << "服务端主节点关闭连接, master_fd = " << master_fd << std::endl;
                else
                    std::cerr << "Worker: read error on master_fd = " << master_fd << std::endl;
                break; // 退出内层循环，准备关闭 master_fd
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

            // worker 根据 master 发来的二级命令(STORE/FETCH/DELETE)进行处理，
            // 并将响应命令(STORED/VALUE/DELETED)以字符串形式返回给master。
            std::string response;

            // ==================== STORE：写入数据 ====================
            if (cmd == "STORE") {
                // 解析 key 和 value，格式: STORE <key> <value>
                std::string store_arg = arg;   // arg 已经 trim 过，包含 key 和 value 用空格分隔
                size_t spc = store_arg.find(' ');
                if (spc == std::string::npos) {
                    response = "ERROR: STORE requires key and value\n";
                    write(master_fd, response.c_str(), response.size());
                    std::cerr << "  -> STORE 参数缺失" << std::endl;
                    continue;  // 跳过本次循环
                }
                std::string key = trim(store_arg.substr(0, spc));
                std::string value = trim(store_arg.substr(spc + 1));
                if (key.empty() || value.empty()) {
                    response = "ERROR: STORE requires key and value\n";
                    write(master_fd, response.c_str(), response.size());
                    std::cerr << "  -> key 或 value 为空" << std::endl;
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
                    std::cout << "  -> 已存储为 id = " << key << " (Wisckey持久化)" << std::endl;
                } else {
                    response = "ERROR store failed\n";
                    std::cerr << "  -> Wisckey Put 失败: " << put_status.ToString() << std::endl;
                }
                write(master_fd, response.c_str(), response.size());

            // ==================== FETCH：读取数据 ====================
            } else if (cmd == "FETCH") {
                // 使用 Wisckey 的 Get 接口替代内存 map 查找
                std::cout << "收到 FETCH 请求: id = " << arg << std::endl;

                std::string value;
                leveldb::Status get_status = db->Get(leveldb::ReadOptions(), arg, &value);
                if (get_status.ok()) {
                    // 键存在，返回值
                    response = "VALUE " + value + "\n";
                    write(master_fd, response.c_str(), response.size());
                    std::cout << "  -> 返回值: \"" << value << "\" (Wisckey)" << std::endl;
                } else if (get_status.IsNotFound()) {
                    // Wisckey 中未找到该键
                    response = "NOT_FOUND\n";
                    write(master_fd, response.c_str(), response.size());
                    std::cout << "  -> 未找到 id (Wisckey IsNotFound)" << std::endl;
                } else {
                    // 其他读取错误
                    response = "ERROR fetch failed\n";
                    write(master_fd, response.c_str(), response.size());
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
                        write(master_fd, response.c_str(), response.size());
                        std::cout << "  -> 已删除 id = " << arg << " (Wisckey)" << std::endl;
                    } else { // 删除操作发生失败
                        response = "ERROR delete failed\n";
                        write(master_fd, response.c_str(), response.size());
                        std::cerr << "  -> Wisckey Delete 失败: " << del_status.ToString() << std::endl;
                    }
                } else if (get_status.IsNotFound()) { // 键不存在
                    response = "NOT_FOUND\n";
                    write(master_fd, response.c_str(), response.size());
                    std::cout << "  -> 未找到 id, 无法删除 (Wisckey)" << std::endl;
                } else { // GET操作发生错误
                    response = "ERROR delete check failed\n";
                    write(master_fd, response.c_str(), response.size());
                    std::cerr << "  -> Wisckey Get 检查错误: " << get_status.ToString() << std::endl;
                }

            } else {
                response = "ERROR\n";
                write(master_fd, response.c_str(), response.size());
                std::cout << "收到未知命令: " << cmd << std::endl;
            }
        }
        close(master_fd);
    }

    // 以上不再是死循环，可以执行到此步，安全关闭数据库
    // delete db 会将 memtable 中的数据刷盘为 SSTable 并更新 MANIFEST
    // 这是数据持久化的关键步骤！如果不执行，数据仅存在于 WAL 中
    std::cout << "Slave: 正在关闭 Wisckey 数据库..." << std::endl;
    delete db;  // 关闭 Wisckey 数据库
    g_db = nullptr;
    close(worker_fd);
    std::cout << "Slave: 已安全退出" << std::endl;
    return 0;
}
