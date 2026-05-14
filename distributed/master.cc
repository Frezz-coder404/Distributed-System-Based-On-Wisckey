// =============================================================================
// kv_master.cc - 轻量化、只存储从节点路由表的主节点
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
#include <list>          // std::list
#include <csignal>       // signal() 处理退出信号

const int MAX_EVENTS = 64;           // 最多可容纳的epoll事件数
const int MASTER_CLIENT_PORT = 8888; // master 的连接 client 的端口常量
const int MASTER_WORKER_PORT = 8889; // master 的连接 worker 的端口常量
const int WORKER_COUNT = 3;          // worker 数量

// 从节点的客户端监听端口，与 slave.cc 中定义的保持一致。主节点需要这个信息来打印路由表。
const int SLAVE_CLIENT_PORTS[WORKER_COUNT] = {9000, 9001, 9002};
// 从节点的 IP 地址（当前均为本机回环地址）
const std::string SLAVE_IP = "127.0.0.1";

// 运行标志，供信号处理器使用, 当接收到 退出 信号时，设置为 false 以退出主循环
static volatile bool g_running = true;

// 对于每个客户端连接的数据设置:缓冲区与相关信息
struct Connection {
    std::string send_buffer;   // 用户态发送缓冲区，存储待发送的数据
    std::string recv_buffer;   // 用户态接收缓冲区，存储要接收的数据
    std::string client_ip;     // 客户端的 IP 地址，在 accept 时获取
    int client_port;           // 客户端的端口号，在 accept 时获取
    bool close_after_send;     // 发送完毕后是否主动关闭连接（即 HELLO/QUIT 后是否需要断开）
    Connection() : client_port(0), close_after_send(false) {}
};

// --- 初始化空映射表：fd <-> 用户态缓冲区(send_buffer & recv_buffer) ---
// 这行的作用是维护一个映射表，记录每个客户端连接的"用户态发送缓冲区"与"用户态接收缓冲区"。
// 每个客户端的"用户态发送缓冲区"和"用户态接收缓冲区"必须独立，以便在发送时正确地回写到对应的客户端，并在接收时正确识别指令。
// 与回显服务器的功能不同，回显服务器只需要一个"用户态接收缓冲区"即可，哪怕一次只收到一个字符也可以立即回显。
// 但是这里必须将"用户态接收缓冲区"独立，因为指令必须读取完整才能进行下一步，没读完的指令存放在独立的缓冲区中，以防止网络异步乱序造成的指令混乱。
std::unordered_map<int, Connection> connections;

// 客户端 IP 记录，当客户端发送 HELLO 时记录，发送 QUIT 时删除
// * 该表用于记录客户端是否与服务端还保持连接，以便出现问题时及时通知到客户端(默认端口10000)，
// * 与connections表不同，上表只用于记录是否还和主节点连接。
std::list<std::string> g_client_ips;

// 为支持多从节点，将单个 g_worker_fd 改为用 vector 存储多个 g_worker_fd
std::vector<int> g_worker_fds;       // 存储所有 worker 连接的文件描述符，用于确认节点已上线

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

// 内联函数：去除字符串首尾空白字符（输入可能带有多余空格）
static inline std::string trim(const std::string &s) {
    auto start = std::find_if_not(s.begin(), s.end(), [](unsigned char ch) {
        return std::isspace(ch);
    });
    auto end = std::find_if_not(s.rbegin(), s.rend(), [](unsigned char ch) {
        return std::isspace(ch);
    }).base();
    return (start < end) ? std::string(start, end) : std::string();
}

// 构建路由表字符串，格式如下：
// ROUTETABLE
// INDEX | IP:PORT | RANGE
// 0 127.0.0.1:9000 0-42
// 1 127.0.0.1:9001 43-85
// 2 127.0.0.1:9002 86-127
// END
std::string build_route_table() {
    std::string table = "ROUTETABLE\r\n";
    table += "INDEX | IP:PORT | RANGE\r\n";
    // ASCII 范围分区，与 get_worker_for_key() 保持一致
    const int ranges[WORKER_COUNT][2] = {{0, 42}, {43, 85}, {86, 127}};
    for (int i = 0; i < WORKER_COUNT; ++i) {
        table += std::to_string(i) + " " + SLAVE_IP + ":" + std::to_string(SLAVE_CLIENT_PORTS[i])
                 + " " + std::to_string(ranges[i][0]) + "-" + std::to_string(ranges[i][1]) + "\r\n";
    }
    table += "END\r\n";
    return table;
}

// 处理函数：处理一条完整的命令（以 '\n' 结尾，但传入的 line 已去除换行符）
// 轻量化后仅处理 HELLO 和 QUIT 两个命令
std::string process_command(const std::string& line, const std::string& client_ip, int client_port) {
    std::string trimmed = trim(line);
    if (trimmed.empty()) return ""; // 忽略空行

    // 统一转为大写以便比较
    std::string cmd = trimmed;
    for (auto& c : cmd) c = std::toupper(static_cast<unsigned char>(c));

    // ==================== HELLO：客户端请求路由表 ====================
    // 客户端连接后发送 HELLO，主节点保存客户端 IP 并返回路由表，然后断开连接
    if (cmd == "HELLO") {
        g_client_ips.push_back(client_ip); // 保存客户端的IP
        std::cout << "客户端IP注册: " << client_ip << std::endl;
        return build_route_table();

    // ==================== QUIT：客户端请求退出 ====================
    // 客户端关闭时发送 QUIT，主节点删除该客户端的 IP 记录，然后断开连接
    } else if (cmd == "QUIT") {
        g_client_ips.remove(client_ip); // 删除客户端的IP
        std::cout << "客户端IP注销: " << client_ip << std::endl;
        return "BYE\r\n";

    } else {
        return "ERROR: Unknown command. Use HELLO or QUIT\r\n";
    }
}

int main() {
    // 注册信号处理器：当收到中断信号(SIGINT/SIGTERM)时，调用函数(signal_handler)。
    std::signal(SIGINT, signal_handler);  // SIGINT的作用是捕获键盘的Ctrl+C信号
    std::signal(SIGTERM, signal_handler); // SIGTERM的作用是捕获进程意外中断信号

    // ========== 第一阶段：被动连接客户端与从节点 ==========
    // ---------- 1. 创建空套接字 ----------
    // socket()返回值: 失败返回 -1，成功返回一个非负整数，称为"套接字文件描述符"，
    // 即"file descriptor"，通常被写作 _fd ，用于标志一个套接字。

    // 连接到客户端的：
    int master_client_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (master_client_fd < 0) {
        std::cerr << "创建 master_client 套接字失败" << std::endl;
        return 1;
    }
    // 设置端口复用（方便调试，避免"Address already in use"，注：端口复用不会影响吞吐量）
    int opt_c = 1;
    setsockopt(master_client_fd, SOL_SOCKET, SO_REUSEADDR, &opt_c, sizeof(opt_c));

    // 连接到从节点的：
    int master_worker_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (master_worker_fd < 0) {
        std::cerr << "创建 master_worker 套接字失败" << std::endl;
        return 1;
    }
    int opt_w = 1;
    setsockopt(master_worker_fd, SOL_SOCKET, SO_REUSEADDR, &opt_w, sizeof(opt_w));

    // ---------- 2. 准备服务器地址结构 ----------
    // 作用：设置地址的 IP:Port 结构，以便接下来与套接字关联

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
        return 1;
    }

    // 连接到从节点的：
    if (bind(master_worker_fd, (struct sockaddr*)&master_worker_addr, sizeof(master_worker_addr)) < 0) {
        std::cerr << "绑定 worker 监听地址失败" << std::endl;
        close(master_worker_fd);
        return 1;
    }

    // ---------- 4. 开始监听端口 ----------
    // 作用: 将套接字从主动模式变成被动模式，告诉操作系统开始接收客户端的连接请求。

    // 连接到客户端的：
    if (listen(master_client_fd, 128) < 0) { // 128 是等待连接队列长度
        std::cerr << "监听 client 端口失败" << std::endl;
        close(master_client_fd);
        return 1;
    }
    std::cout << "被动监听客户端通道已建立, master_client_fd = " << master_client_fd
              << ", socket: [127.0.0.1:" << MASTER_CLIENT_PORT << "]"
              << " (epoll ET 模式) " << std::endl;

    // 连接到从节点的：
    if (listen(master_worker_fd, 5) < 0) {
        std::cerr << "监听 master_worker 端口失败" << std::endl;
        close(master_worker_fd);
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

                // 将新连接的客户端加入映射表，同时保存其 IP 和 端口
                char client_ip[INET_ADDRSTRLEN]; // 获取IP
                inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
                int client_port = ntohs(client_addr.sin_port); // 获取端口

                Connection conn;
                conn.client_ip = client_ip;
                conn.client_port = client_port;
                connections[client_fd] = conn; // 加入映射表

                // 将新连接的客户端套接字也加入 epoll 监视列表，客户端套接字必须使用ET模式。
                // EPOLLIN    : 设置关注"可读"事件；
                // EPOLLOUT   : 设置关注"可写"事件；
                // EPOLLET    : 设置为边缘触发(ET)模式，表示当状态发生变化(不可读->可读，不可写->可写)时才会通知。
                // EPOLLRDHUP : 设置关注"对端关闭连接"事件，无需read数据即可感知关闭，更高效。
                ev.events = EPOLLIN | EPOLLOUT | EPOLLET | EPOLLRDHUP;
                ev.data.fd = client_fd;
                if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev) < 0) {
                    std::cerr << "添加客户端套接字到 epoll 失败, client_fd = " << client_fd << std::endl;
                    close(client_fd);
                    continue;
                }

                // 打印新连接的客户端的fd号、IP地址与端口号
                std::cout << "新客户端连接, client_fd = " << client_fd
                          << ", socket = " << client_ip << ":" << client_port << std::endl;
            }
            // ---- 8.2 如果就绪的是普通客户端套接字：有数据可读(有数据来内核接收缓冲区了)或有数据可写(内核发送缓冲区空出来了) ----
            else {
                bool closed = false; // closed 标志用于指示客户端是否关闭连接或发生错误。
                Connection& conn = connections[fd]; // "用户态缓冲区"，获取当前连接的"用户态缓冲区"引用，以便后续读写操作调用:将send_buffer中的数据发送或将读取的数据写入recv_buffer。

                // 检测对端是否正常关闭连接（EPOLLRDHUP），需要先检测是否关闭，否则可能会被误认为发生错误。
                if (events[i].events & EPOLLRDHUP) {
                    std::cout << "客户端已断开, client_fd = " << fd
                            << ", socket = " << conn.client_ip << ":" << conn.client_port << std::endl;
                    closed = true;
                    // 这里不用 break，而是设置 closed ，会自动走到关闭逻辑
                }

                // 检查是否有错误或挂起事件（EPOLLERR | EPOLLHUP），如果发生错误或对端异常关闭连接，应该立即处理，而不是继续尝试读写。
                if (!closed && (events[i].events & (EPOLLERR | EPOLLHUP))) {
                    std::cerr << "客户端 client_fd = " << fd << " 发生错误或对端异常关闭连接" << std::endl;
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
                            std::cerr << "成功发送了 " << bytes_write << " 字节到 client_fd = " << fd << std::endl;
                            conn.send_buffer.erase(0, bytes_write);
                        }
                    }

                    // 如果"用户态发送缓冲区"已空，说明路由表或BYE已发送完毕
                    if (conn.send_buffer.empty()) {
                        if (conn.close_after_send) {
                            // 如果"用户态发送缓冲区"已空，且设置了HELLO后主动断开(也就是路由表字符串已存入send_buffer)，则说明数据已经全部准备好并且发送完了，需主动关闭连接
                            std::cout << "客户端已断开, client_fd = " << fd
                            << ", socket = " << conn.client_ip << ":" << conn.client_port << std::endl;
                            closed = true;
                        } else {
                            // 如果"用户态发送缓冲区"已空，且未设置HELLO后主动断开(也就是路由表字符串未存入send_buffer)，则取消 EPOLLOUT 事件（降低开销，可选），继续等待
                            ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP; // 只关注读
                            ev.data.fd = fd;
                            if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev) < 0) {
                                std::cerr << "修改客户端套接字状态失败, client_fd = " << fd << std::endl;
                                close(fd);
                                continue;
                            }
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
                                std::string response = process_command(line, conn.client_ip, conn.client_port);
                                if (!response.empty()) {
                                    conn.send_buffer += response;
                                    // HELLO 和 QUIT 命令处理后，设置 close_after_send
                                    // 主节点发送完 路由表 或 BYE 后主动断开与客户端的连接
                                    conn.close_after_send = true;
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
                    // 如果"用户态发送缓冲区"非空，需要注册 EPOLLOUT 事件以便发送（与上方取消EPOLLOUT形成对应处理）
                    if (!conn.send_buffer.empty()) {
                        ev.events = EPOLLIN | EPOLLOUT | EPOLLET | EPOLLRDHUP;
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
                    close(fd); // 关闭套接字
                    connections.erase(fd); // 删除该表项并释放缓冲区内存
                }
            }
        }
    }

    // ---------- 9. 关闭套接字 ----------
    // 因为大循环改为了检查g_running而非死循环了,因此程序可能执行到这里。
    std::cout << "Master: 正在关闭..." << std::endl;
    for (int fd : g_worker_fds) close(fd);
    close(epoll_fd);
    close(master_client_fd);
    std::cout << "Master: 已安全退出" << std::endl;

    return 0;
}
