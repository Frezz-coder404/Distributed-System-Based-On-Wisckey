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
#include <sys/epoll.h>     // epoll_create1(), epoll_ctl(), epoll_wait()
#include <fcntl.h>         // fcntl, F_SETFL, O_NONBLOCK
#include <errno.h>         // errno, EAGAIN, EWOULDBLOCK
#include <sys/epoll.h>     // epoll_create1(), epoll_ctl(), epoll_wait()
#include <fcntl.h>         // fcntl, F_SETFL, O_NONBLOCK
#include <errno.h>         // errno, EAGAIN, EWOULDBLOCK
#include <unordered_map>   // std::unordered_map

// 引入 Wisckey 的头文件
#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/status.h"
#include "leveldb/slice.h"
#include "leveldb/write_batch.h"

int worker_id = -1;                          // worker 索引，必须在启动时通过参数显式指定
const std::string MASTER_HOST = "127.0.0.1"; // master 的监听地址
const int MASTER_PORT = 8889;                // master 的连接从节点的端口
const int SLAVE_CLIENT_PORTS[3] = {9000, 9001, 9002}; // 从节点监听客户端连接的端口
const int MAX_EVENTS = 64;                   // epoll 最多可容纳的事件数

// Wisckey 元数据库全局指针, g_db 用于指向数据库
// 对相应的数据库进行各种操作，或者读取其中数据，如
// 持久化的 master 的路由表（key -> ValueLocation）
static leveldb::DB* g_db = nullptr;
// 运行标志，供信号处理器使用, 当接收到 退出 信号时，设置为 false 以退出主循环
static volatile bool g_running = true;

// 对于每个客户端连接的数据设置: 缓冲区
struct Connection {
    std::string send_buffer;   // 用户态发送缓冲区，存储待发送的数据
    std::string recv_buffer;   // 用户态接收缓冲区，存储要接收的数据
};

// 客户端连接映射表：fd <-> 用户态缓冲区(send_buffer & recv_buffer)
// 这行的作用是维护一个映射表，记录每个客户端连接的"用户态发送缓冲区"与"用户态接收缓冲区"。
// 每个客户端的"用户态发送缓冲区"和"用户态接收缓冲区"必须独立，以便在发送时正确地回写到对应的客户端，并在接收时正确识别指令。
// 与回显服务器的功能不同，回显服务器只需要一个"用户态接收缓冲区"即可，哪怕一次只收到一个字符也可以立即回显。
// 但是这里必须将"用户态接收缓冲区"独立，因为指令必须读取完整才能进行下一步，没读完的指令存放在独立的缓冲区中，以防止网络异步乱序造成的指令混乱。
std::unordered_map<int, Connection> connections;

// 去除首尾空白（与主服务器一致）
static inline std::string trim(const std::string &s) {
    auto start = std::find_if_not(s.begin(), s.end(), ::isspace);
    auto end = std::find_if_not(s.rbegin(), s.rend(), ::isspace).base();
    return (start < end) ? std::string(start, end) : "";
}

// 将一个文件描述符设置为非阻塞模式（ET 模式需要，LT 模式可选但建议）
void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
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

// 函数：处理一条完整的命令（以 '\n' 结尾，但传入的 line 已去除换行符）
// 从原有阻塞循环中提取的命令处理逻辑，改为返回响应字符串
// 响应格式统一使用 "\r\n" 结尾，以便客户端解析
std::string process_command(const std::string& line, leveldb::DB* db) {
    std::string trimmed = trim(line);
    if (trimmed.empty()) return ""; // 忽略空内容

    // 分离命令cmd和参数arg
    std::istringstream iss(trimmed); // 把字符串变为输入流,便于读取
    std::string cmd, arg;
    iss >> cmd;                          // 流提取运算符,读取读一个非空字符串,到下一个空白前停止,由此提取出 cmd
    std::getline(iss, arg);    // 读取剩余所有内容,到换行符前停止,由此提取出 arg
    arg = trim(arg);                  // 由于提取出来的arg前带有空白,需去除首尾空白

    // 统一转为大写以便比较
    for (auto& c : cmd) c = std::toupper(static_cast<unsigned char>(c));

    // worker 根据 客户端 发来的命令(PUT/GET/DELETE/SCAN)进行处理，
    // 并将响应命令(STORED/VALUE/DELETED/KVPAIR+END)以字符串形式返回。

    // ==================== PUT：写入数据 ====================
    if (cmd == "PUT") {
        // 解析 key 和 value，格式: PUT <key> <value>
        std::string store_arg = arg;   // arg 已经 trim 过，包含 key 和 value 用空格分隔
        size_t spc = store_arg.find(' '); // 找到第一个空格, 分割出 key 和 value
        if (spc == std::string::npos) {
            std::cerr << "  -> PUT 参数缺失" << std::endl;
            return "ERROR: PUT requires key and value\r\n";
        }
        std::string key = trim(store_arg.substr(0, spc));
        std::string value = trim(store_arg.substr(spc + 1));
        if (key.empty() || value.empty()) {
            std::cerr << "  -> PUT 的 key 或 value 为空" << std::endl;
            return "ERROR: PUT requires key and value\r\n";
        }

        std::cout << "收到 PUT 请求: key = \"" << key
                    << "\", value = \"" << value << "\"" << std::endl;

        // 持久化写入
        // sync 字段用于控制当 leveldb 将数据写入到预写日志时，是否同步地调用 fsync() 将内核缓冲区中的数据 flush 到硬盘。
        leveldb::WriteOptions write_opts;
        write_opts.sync = true;
        leveldb::Status put_status = db->Put(write_opts, key, value);

        if (put_status.ok()) {
            // PUT 成功，返回 STORED 响应
            std::cout << "  -> 已存储为 id = " << key << std::endl;
            return "STORED\r\n"; // 无需返回参数 id ，因为id与key一样。
        } else {
            std::cerr << "  -> Wisckey Put 失败: " << put_status.ToString() << std::endl;
            return "ERROR Put failed\r\n";
        }

    // ==================== GET：读取数据 ====================
    } else if (cmd == "GET") {
        // 无需解析 key ，格式: GET <id>，上方自动解析出的arg就是id，无需再解析
        // 使用 Wisckey 的 Get 接口替代内存 map 查找
        std::cout << "收到 GET 请求: id = " << arg << std::endl;

        std::string value;
        leveldb::Status get_status = db->Get(leveldb::ReadOptions(), arg, &value);
        if (get_status.ok()) {
            // 键存在，返回值
            std::cout << "  -> 返回值: \"" << value << "\" " << std::endl;
            return "VALUE " + value + "\r\n";
        } else if (get_status.IsNotFound()) {
            // Wisckey 中未找到该键
            std::cout << "  -> 未找到 id " << std::endl;
            return "NOT_FOUND\r\n";
        } else {
            // 其他读取错误
            std::cerr << "  -> Wisckey Get 错误: " << get_status.ToString() << std::endl;
            return "ERROR Get failed\r\n";
        }

    // ==================== DELETE：删除数据 ====================
    } else if (cmd == "DELETE") {
        // 使用 Wisckey 的 Get + Delete 接口替代内存 map 删除
        // 现在: 先用 db->Get() 检查键是否存在，再用 db->Delete() 删除
        // 注：Wisckey 的 Delete 不会在键不存在时返回错误，
        //     为兼容原有协议（区分 DELETED / NOT_FOUND），先做存在性检查
        std::cout << "收到 DELETE 请求: id = " << arg << std::endl;

        std::string value;
        leveldb::Status get_status = db->Get(leveldb::ReadOptions(), arg, &value);
        if (get_status.ok()) { // 键存在
            // 使用 sync=true 确保删除操作持久化
            leveldb::WriteOptions write_opts;
            write_opts.sync = true;

            // 键存在，执行删除
            // 问：当内存中和磁盘中的某条路由信息被删除后，分别会发生什么？中间空缺的这部分会怎么处理？
            // 答：当数据删除或被覆盖，LSM-Tree中不会将其真的删除，而是贴上删除标记，并在后续的回收过程(合并与恢复操作会调用回收)中真正删除。
            leveldb::Status del_status = db->Delete(write_opts, arg);
            if (del_status.ok()) { // 删除成功
                std::cout << "  -> 已删除 id = " << arg << std::endl;
                return "DELETED\r\n";
            } else { // 删除操作发生失败
                std::cerr << "  -> Wisckey Delete 失败: " << del_status.ToString() << std::endl;
                return "ERROR delete failed\r\n";
            }
        } else if (get_status.IsNotFound()) { // 键不存在
            std::cout << "  -> 未找到 id, 无法删除 " << std::endl;
            return "NOT_FOUND\r\n";
        } else { // GET操作发生错误
            std::cerr << "  -> Wisckey Get 检查错误: " << get_status.ToString() << std::endl;
            return "ERROR delete check failed\r\n";
        }

    // ==================== SCAN：范围查询 ====================
    } else if (cmd == "SCAN") {
        // 解析 start_key 和 end_key，格式: SCAN <start_key> <end_key>
        std::string scan_arg = arg;   // arg 已经 trim 过
        size_t spc = scan_arg.find(' '); // 找到第一个空格, 分割出 key 和 value
        // 如果没有找到空格，说明缺少两个参数，返回错误提示
        if (spc == std::string::npos) {
            std::cerr << "  -> SCAN 参数缺失" << std::endl;
            return "ERROR: SCAN requires start_key and end_key\r\n";
        }
        std::string start_key = trim(scan_arg.substr(0, spc));
        std::string end_key = trim(scan_arg.substr(spc + 1));
        // 如果任一键为空，说明缺少一个参数，返回错误提示
        if (start_key.empty() || end_key.empty()) {
            std::cerr << "  -> SCAN 的 start_key 或 end_key 为空" << std::endl;
            return "ERROR: SCAN requires start_key and end_key\r\n";
        }
        // 如果 <start_key> 大于 <end_key>，说明参数顺序错误，返回错误提示
        if (start_key > end_key) {
            return "ERROR: SCAN first_key must be <= last_key\r\n";
        }

        std::cout << "收到 SCAN 请求: start = \"" << start_key
                    << "\", end = \"" << end_key << "\"" << std::endl;

        // 使用 Wisckey 迭代器进行有序范围扫描, 远比一个个 GET 效率来得快.
        std::string response;
        leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
        int sent_count = 0;
        // 不逐行阻塞 write，而是累积到 response 字符串中，由 epoll 发送缓冲区统一发送
        for (it->Seek(start_key); it->Valid() && it->key().ToString() <= end_key; it->Next()) {
            response += "KVPAIR " + it->key().ToString() + " " + it->value().ToString() + "\r\n";
            sent_count++;
        }

        if (!it->status().ok()) {
            std::cerr << "  -> SCAN 迭代出错: " << it->status().ToString() << std::endl;
        }
        delete it;

        // 无论是否有结果，都必须以 END 结束，告诉客户端"我已发送完毕"
        response += "END\r\n";
        std::cout << "  -> SCAN 完成，发送了 " << sent_count << " 条 KVPAIR" << std::endl;
        return response;

    } else {
        std::cout << "收到未知命令: " << cmd << std::endl;
        return "ERROR unknown command\r\n";
    }
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

    // ========== 第一阶段：主动连接主节点 ==========
    std::cout << "Worker " << worker_id << ": 尝试连接主服务器 " << MASTER_HOST << ":" << MASTER_PORT << " ..." << std::endl;
    // 从节点主动连接：循环重试直到连接成功
    int master_fd = -1;
    while (g_running) {
        // ---------- 1. 创建空套接字 ----------
        // 这部分主动与被动连接一致。
        master_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (master_fd < 0) {
            std::cerr << "Worker: socket 创建失败, 重试..." << std::endl;
            sleep(1);
            continue;
        }

        // ---------- 2. 准备服务器地址结构 ----------
        // 主动连接准备的地址是目的地址，即 master 的而非 slave 的。
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
        // connect 成功返回 0，失败返回 -1，并设置 errno 以指示错误类型。
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

    // ========== 第二阶段：被动连接客户端 ==========
    // 从节点独立监听客户端连接端口，客户端根据路由表直接连接
    // ---------- 1. 创建空套接字 ----------
    // 这部分主动与被动连接一致。
    int slave_listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (slave_listen_fd < 0) {
        std::cerr << "Worker: 创建 slave 监听套接字失败" << std::endl;
        close(master_fd);
        delete db;
        return 1;
    }
    int opt = 1; // 设置端口复用, 注：端口复用不会影响吞吐量
    setsockopt(slave_listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // ---------- 2. 准备服务器地址结构 ----------
    // 被动链接准备从节点自身的地址.
    struct sockaddr_in slave_addr;
    memset(&slave_addr, 0, sizeof(slave_addr));
    slave_addr.sin_family = AF_INET;
    slave_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    slave_addr.sin_port = htons(SLAVE_CLIENT_PORTS[worker_id]);

    // ---------- 3. 绑定地址到套接字 ----------
    if (bind(slave_listen_fd, (struct sockaddr*)&slave_addr, sizeof(slave_addr)) < 0) {
        std::cerr << "Worker: 绑定 slave 监听地址失败 (" << strerror(errno) << ")" << std::endl;
        close(slave_listen_fd);
        close(master_fd);
        delete db;
        return 1;
    }

    // ---------- 4. 开始监听端口 ----------
    if (listen(slave_listen_fd, 128) < 0) {
        std::cerr << "Worker: 监听 slave 端口失败" << std::endl;
        close(slave_listen_fd);
        close(master_fd);
        delete db;
        return 1;
    }
    std::cout << "Worker " << worker_id << ": 客户端监听已建立, socket: [127.0.0.1:"
              << SLAVE_CLIENT_PORTS[worker_id] << "]" << std::endl;


    // ========== 第三阶段：创建 epoll 实例并进入主循环 ==========
    // ---------- 1. 创建 epoll 实例 ----------
    int epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) {
        std::cerr << "Worker: 创建 epoll 失败" << std::endl;
        close(slave_listen_fd);
        close(master_fd);
        delete db;
        return 1;
    }

    // ---------- 2. 将监听套接字加入 epoll 监视列表 ----------
    // 将从节点的客户端套接字（监听套接字）加入监视列表，
    // 这样当有新的客户端连接到该端口时，epoll_wait 就会通知我们。
    struct epoll_event ev;
    ev.events = EPOLLIN;                // 水平LT触发，监听可读。（监听套接字不必使用ET模式，因为新连接事件用LT更简单）
    ev.data.fd = slave_listen_fd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, slave_listen_fd, &ev) < 0) {
        std::cerr << "Worker: 添加 slave 监听套接字到 epoll 失败" << std::endl;
        close(epoll_fd);
        close(slave_listen_fd);
        close(master_fd);
        delete db;
        return 1;
    }

    // ---------- 3. 将 master_fd 也加入 epoll，以便检测主节点断开 ----------
    // master_fd 使用阻塞模式，此处仅用于检测对端关闭
    set_nonblocking(master_fd);
    ev.events = EPOLLIN | EPOLLET;      // ET 模式，检测可读（对端关闭时会触发）
    ev.data.fd = master_fd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, master_fd, &ev) < 0) {
        std::cerr << "Worker: 添加 master_fd 到 epoll 失败" << std::endl;
        // 非致命错误，继续运行
    }

    // ---------- 4. 主循环：循环读取客户端发来的链接或数据 ----------
    struct epoll_event events[MAX_EVENTS];
    while (g_running) {
        // 等待事件发生，timeout超时 -1 表示一直阻塞, 0 表示立即返回，正数表示等待指定毫秒数后返回。
        // timeout采用用较短的1秒超时，以便周期性检查 g_running 标志
        int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, 1000);
        if (nfds < 0) {
            if (!g_running) break;      // 被信号中断，正常退出
            if (errno == EINTR) continue;
            std::cerr << "Worker: epoll_wait 错误" << std::endl;
            break;
        }

        // 处理每个就绪的 epoll 事件
        for (int i = 0; i < nfds; ++i) {
            int fd = events[i].data.fd;

            // ---- 4.1：master_fd 有事件（说明主节点断开或异常数据） ----
            if (fd == master_fd) {
                if (events[i].events & (EPOLLERR | EPOLLHUP)) {
                    std::cout << "主节点断开连接, master_fd = " << master_fd << std::endl;
                    // 从 epoll 移除，但不退出，从节点仍可独立服务客户端
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, master_fd, nullptr);
                    close(master_fd);
                    master_fd = -1;
                } else if (events[i].events & EPOLLIN) {
                    // 断开后主节点理应不再发数据到从节点，若读取需丢弃
                    char buf[64];
                    ssize_t n = read(master_fd, buf, sizeof(buf));
                    if (n <= 0) {
                        std::cout << "主节点关闭连接, master_fd = " << master_fd << std::endl;
                        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, master_fd, nullptr);
                        close(master_fd);
                        master_fd = -1;
                    } else {
                        // 收到意外数据，丢弃
                        std::cerr << "Worker: 收到来自 master 的意外数据，已忽略" << std::endl;
                    }
                }
                continue;
            }

            // ---- 4.2：从节点监听套接字有事件：新的客户端连接 ----
            else if (fd == slave_listen_fd) {
                struct sockaddr_in client_addr;
                socklen_t client_len = sizeof(client_addr);
                // 尝试接收新的连接，若接收成功，为该连接分配一个文件描述符
                int client_fd = accept(slave_listen_fd, (struct sockaddr*)&client_addr, &client_len);
                if (client_fd < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        continue;
                    }
                    std::cerr << "Worker: accept 失败" << std::endl;
                    continue;
                }

                // 设置客户端套接字为非阻塞模式
                set_nonblocking(client_fd);

                // 将新连接的客户端加入映射表
                connections[client_fd] = Connection();

                // 将新连接的客户端套接字加入 epoll（ET 模式）
                ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
                ev.data.fd = client_fd;
                if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev) < 0) {
                    std::cerr << "Worker: 添加客户端套接字到 epoll 失败, client_fd = " << client_fd << std::endl;
                    close(client_fd);
                    continue;
                }

                // 打印新连接的客户端信息
                char client_ip[INET_ADDRSTRLEN];
                inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
                int client_port = ntohs(client_addr.sin_port);
                std::cout << "新客户端连接, client_fd = " << client_fd
                          << ", socket = " << client_ip << ":" << client_port << std::endl;
                continue;
            }

            // ---- 4.3：普通客户端套接字有事件：数据可读或可写 ----
            else {
                bool closed = false; // closed 标志用于指示客户端是否关闭连接或发生错误。
                // 检查该 fd 是否仍在 connections 中（可能已被清理）
                auto conn_it = connections.find(fd);
                if (conn_it == connections.end()) {
                    close(fd);
                    continue;
                }
                Connection& conn = conn_it->second;

                // 检查是否有错误或挂起事件
                if (events[i].events & (EPOLLERR | EPOLLHUP)) {
                    std::cerr << "客户端 client_fd = " << fd << " 发生错误或对端关闭连接" << std::endl;
                    closed = true;
                }

                // ---- 4.3.1 处理可写事件 ----
                if (!closed && (events[i].events & EPOLLOUT)) {
                    // 尝试发送"用户态发送缓冲区"中的数据
                    while (!conn.send_buffer.empty()) {
                        int bytes_write = write(fd, conn.send_buffer.data(), conn.send_buffer.size());
                        if (bytes_write < 0) {
                            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                                break; // 内核发送缓冲区满了，下次 EPOLLOUT 再继续
                            } else {
                                std::cerr << "写入错误发生在 client_fd = " << fd << std::endl;
                                closed = true;
                                break;
                            }
                        } else if (bytes_write == 0) {
                            // 应该不会发生
                            continue;
                        } else {
                            // 成功发送了 n 字节，从发送缓冲区中移除
                            std::cerr << "成功发送了 " << bytes_write << " 字节到 client_fd = " << fd << std::endl;
                            conn.send_buffer.erase(0, bytes_write);
                        }
                    }
                    // 如果"用户态发送缓冲区"已空，则取消 EPOLLOUT 事件（降低开销，可选）
                    if (conn.send_buffer.empty()) {
                        ev.events = EPOLLIN | EPOLLET; // 只关注读
                        ev.data.fd = fd;
                        if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev) < 0) {
                            std::cerr << "Worker: 修改客户端套接字状态失败, client_fd = " << fd << std::endl;
                            close(fd);
                            connections.erase(fd);
                            continue;
                        }
                    }
                }

                // ---- 4.3.2处理可读事件 ----
                if (!closed && (events[i].events & EPOLLIN)) {
                    while (true) { // ET 模式下必须循环读取直到 EAGAIN, 表示"内核接收缓冲区"已空，即数据已读完。
                        char temp_buffer[256] = {0};
                        int bytes_read = read(fd, temp_buffer, sizeof(temp_buffer) - 1);
                        if (bytes_read > 0) {
                            // 将"临时数组"中的数据拷贝到"用户态接收缓冲区"。
                            conn.recv_buffer.append(temp_buffer, bytes_read);

                            // 循环处理所有完整的行（以 '\n' 结尾）
                            size_t pos;
                            while ((pos = conn.recv_buffer.find('\n')) != std::string::npos) {
                                std::string line = conn.recv_buffer.substr(0, pos);
                                conn.recv_buffer.erase(0, pos + 1); // 移除已处理的行（包括 '\n'）

                                // 调用 process_command 处理命令，获得响应
                                std::string response = process_command(line, db);
                                if (!response.empty()) {
                                    conn.send_buffer += response;
                                }
                            }
                        } else if (bytes_read == 0) {
                            std::cout << "客户端 client_fd = " << fd << " 已关闭连接" << std::endl;
                            closed = true;
                            break;
                        } else {
                            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                                // 数据已读完，退出
                                break;
                            } else {
                                // 其他错误
                                std::cerr << "客户端 client_fd = " << fd << " 读取数据失败" << std::endl;
                                closed = true;
                                break;
                            }
                        }
                    }
                    // 如果"用户态发送缓冲区"非空，需要注册 EPOLLOUT 事件以便发送（与上方取消EPOLLOUT形成对应处理）
                    if (!conn.send_buffer.empty()) {
                        ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
                        ev.data.fd = fd;
                        if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev) < 0) {
                            std::cerr << "Worker: 修改客户端套接字状态失败, client_fd = " << fd << std::endl;
                            close(fd);
                            connections.erase(fd);
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
                    connections.erase(fd); // 释放缓冲区内存
                }
            }


        }
    }

    // ========== 第四阶段：关闭套接字并落盘 ==========
    // 以上不再是死循环，可以执行到此步，安全关闭数据库
    // delete db 会将 memtable 中的数据刷盘为 SSTable 并更新 MANIFEST
    // 这是数据持久化的关键步骤！如果不执行，数据仅存在于 WAL 中
    std::cout << "Slave: 正在关闭 Wisckey 数据库..." << std::endl;
    // 关闭文件描述符
    close(epoll_fd);
    close(slave_listen_fd);
    if (master_fd >= 0) close(master_fd);
    // 关闭所有客户端连接
    for (auto& pair : connections) {
        close(pair.first);
    }
    connections.clear();
    // 关闭 Wisckey 数据库
    delete db;
    g_db = nullptr;
    std::cout << "Slave: 已安全退出" << std::endl;
    return 0;
}
