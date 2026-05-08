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
//   QUIT 退出客户端
// =============================================================================

#include <iostream>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

const std::string DEFAULT_HOST = "127.0.0.1";
const int DEFAULT_PORT = 8888;

// 去除字符串末尾的换行符（用于从 std::getline 读取的用户输入）
static std::string trim_newline(const std::string& s) {
    auto end = s.find_last_not_of("\r\n");
    if (end != std::string::npos)
        return s.substr(0, end + 1);
    return s;
}

int main(int argc, char* argv[]) {
    std::string host = DEFAULT_HOST;
    int port = DEFAULT_PORT;

    // 读取 IP 与 port 参数
    if (argc >= 2) host = argv[1];
    if (argc >= 3) port = std::stoi(argv[2]);

    // 1. 创建套接字
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        std::cerr << "错误: 无法创建套接字" << std::endl;
        return 1;
    }

    // 2. 准备服务器地址
    struct sockaddr_in server_addr;
    std::memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);

    if (inet_pton(AF_INET, host.c_str(), &server_addr.sin_addr) <= 0) {
        std::cerr << "错误: 无效的 IP 地址: " << host << std::endl;
        close(sock);
        return 1;
    }

    // 3. 连接到 master
    if (connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        std::cerr << "错误: 无法连接到 " << host << ":" << port << std::endl;
        close(sock);
        return 1;
    }

    std::cout << "已连接到 Master (" << host << ":" << port << ")" << std::endl;
    std::cout << "支持命令: PUT, GET, DELETE, SCAN, QUIT" << std::endl;

    // 4. 发送/接收循环
    std::string user_input;
    char recv_buf[4096];

    while (true) {
        std::cout << ">> ";
        if (!std::getline(std::cin, user_input)) {
            // 用户按 Ctrl+D 或输入 EOF
            std::cout << "\n退出客户端" << std::endl;
            break;
        }

        std::string cmd = trim_newline(user_input);
        if (cmd.empty()) continue;

        // 本地处理 QUIT 命令（也可以发送给服务器，但这里选择直接退出）
        if (cmd == "QUIT" || cmd == "quit") {
            std::cout << "退出客户端" << std::endl;
            break;
        }

        // 发送命令（需附加 '\n'，因为 master 用 '\n' 作为命令分隔符）
        std::string request = cmd + "\n";
        ssize_t sent = write(sock, request.c_str(), request.size());
        if (sent < 0) {
            std::cerr << "发送失败，连接已断开" << std::endl;
            break;
        }

        // 循环读取，直到遇到 "END\r\n" 作为 SCAN 结束标志，其他命令就一次读。
        std::string response;
        while (true) {
            ssize_t n = read(sock, recv_buf, sizeof(recv_buf) - 1);
            if (n < 0) {
                std::cerr << "读取响应错误" << std::endl;
                close(sock);
                return 1;
            } else if (n == 0) {
                std::cout << "服务器关闭了连接" << std::endl;
                close(sock);
                return 0;
            }
            recv_buf[n] = '\0';
            response += recv_buf;

            // 如果响应以 "END\r\n" 结束（SCAN 命令），停止读取
            if (response.size() >= 5 && response.substr(response.size() - 5) == "END\r\n") {
                break;
            }
            if (n > 0 && recv_buf[n-1] == '\n') {
                // 可能是单行响应（OK, VALUE, NOT FOUND, ERROR 等），直接结束
                break;
            }
            if (cmd.rfind("SCAN", 0) != 0) { // 如果不是 SCAN 命令
                break;
            }
        }

        // 打印响应
        std::cout << response;
        // 如果响应末尾没有换行，也许多加一个换行
        if (!response.empty() && response.back() != '\n')
            std::cout << std::endl;
    }

    close(sock);
    return 0;
}