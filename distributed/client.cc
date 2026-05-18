// =============================================================================
// client.cc - DisKV 客户端（CLI 交互界面）
// =============================================================================
//
// 【架构定位】客户端是用户与分布式 KV 系统的交互入口。
//           1. 连接主节点获取路由表（HELLO）
//           2. 根据路由表直连目标从节点发送数据命令（PUT/GET/DELETE/SCAN）
//           3. 发送 QUIT 通知主节点客户端退出
//
// 【关键数据结构】
//   route_table     — vector<RouteEntry>，路由表（索引/IP/端口/ASCII范围）
//   slave_fds       — unordered_map<worker_idx, fd>，到从节点的 TCP 持久连接
//
// 【协议】详见 master.cc 和 slave.cc 头部注释
// =============================================================================

#include <arpa/inet.h>       // inet_pton, htons
#include <cstring>           // memset
#include <errno.h>           // errno, EINTR
#include <iostream>          // std::cout, std::cin, std::cerr
#include <netinet/in.h>      // sockaddr_in
#include <sstream>           // std::istringstream
#include <string>            // std::string
#include <sys/socket.h>      // socket, connect
#include <unistd.h>          // close, read, write
#include <unordered_map>     // std::unordered_map
#include <vector>            // std::vector

// ==================== 常量定义 ====================

const std::string DEFAULT_HOST = "127.0.0.1";   // 默认主节点 IP
const int DEFAULT_PORT = 8888;                   // 默认主节点端口

// ==================== 数据结构 ====================

// 路由表条目：记录一个从节点的连接信息和负责的 key 范围
struct RouteEntry {
  int index;              // 从节点索引号（0/1/2）
  std::string ip;         // 从节点 IP 地址
  int port;               // 从节点客户端监听端口（9000/9001/9002）
  int range_start;        // 该从节点负责的 key 首字符 ASCII 范围起始
  int range_end;          // 该从节点负责的 key 首字符 ASCII 范围结束
};

// ★ 全局路由表：HELLO 后由 parse_route_table() 填充
std::vector<RouteEntry> route_table;

// ★ 从节点持久连接映射表：worker_index → fd
//   首次访问某从节点时建立 TCP 连接，后续复用
std::unordered_map<int, int> slave_fds;

// ==================== 工具函数 ====================

// 去除字符串末尾的换行符（\r 和 \n）
static std::string trim_newline(const std::string& s) {
  auto end = s.find_last_not_of("\r\n");
  if (end != std::string::npos) return s.substr(0, end + 1);
  return s;
}

// 去除字符串首尾空白（空格、制表、回车、换行）
static std::string trim(const std::string& s) {
  auto start = s.find_first_not_of(" \t\r\n");
  auto end = s.find_last_not_of(" \t\r\n");
  if (start == std::string::npos) return "";
  return s.substr(start, end - start + 1);
}

// 根据 key 的首字符查找对应的从节点索引
// ★ 核心路由查询：遍历路由表，找到 ASCII 范围覆盖 key[0] 的条目
int find_worker_for_key(const std::string& key) {
  if (key.empty() || route_table.empty()) return -1;
  unsigned char ch = static_cast<unsigned char>(key[0]);
  for (const auto& entry : route_table) {
    if (ch >= entry.range_start && ch <= entry.range_end) {
      return entry.index;
    }
  }
  return -1;  // 未找到对应的从节点
}

// 获取（或创建）指定从节点的 TCP 连接
// ★ 延迟连接策略：仅在首次需要与某从节点通信时才建立连接。
//   连接被缓存到 slave_fds，后续操作直接复用。
int get_slave_fd(int worker_index) {
  // 检查是否已有连接
  auto it = slave_fds.find(worker_index);
  if (it != slave_fds.end() && it->second >= 0) {
    return it->second;  // 复用已有连接
  }

  // 边界检查
  if (worker_index < 0 || worker_index >= (int)route_table.size()) return -1;
  const RouteEntry& entry = route_table[worker_index];

  // 创建新连接
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) return -1;

  struct sockaddr_in addr;
  std::memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(entry.port);
  if (inet_pton(AF_INET, entry.ip.c_str(), &addr.sin_addr) <= 0) {
    close(sock); return -1;
  }

  if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
    close(sock); return -1;
  }

  slave_fds[worker_index] = sock;  // 缓存连接
  return sock;
}

// ==================== 路由表解析 ====================

// 解析主节点返回的路由表字符串，填充 route_table
// 路由表格式:
//   ROUTETABLE\r\n
//   INDEX | IP:PORT | RANGE\r\n
//   0  127.0.0.1:9000  0-42\r\n
//   ...
//   END\r\n
bool parse_route_table(const std::string& response) {
  route_table.clear();

  std::istringstream iss(response);
  std::string line;
  bool in_table = false;  // 状态机：是否已进入路由表区域

  // 逐行解析
  while (std::getline(iss, line)) {
    if (!line.empty() && line.back() == '\r') line.pop_back();  // 去除 \r
    line = trim(line);

    if (line == "ROUTETABLE") { in_table = true; continue; }        // 进入
    if (line.find("INDEX") == 0) continue;                           // 跳过表头
    if (line == "END") break;                                        // 结束
    if (!in_table) continue;                                         // 尚未进入，忽略

    // 解析数据行: <index> <ip>:<port> <range_start>-<range_end>
    std::istringstream line_iss(line);
    int index;
    std::string ip_port, range_str;
    if (!(line_iss >> index >> ip_port >> range_str)) continue;

    // 解析 ip:port
    size_t colon_pos = ip_port.find(':');
    if (colon_pos == std::string::npos) continue;
    std::string ip = ip_port.substr(0, colon_pos);
    int port = std::stoi(ip_port.substr(colon_pos + 1));

    // 解析 range (start-end)
    size_t dash_pos = range_str.find('-');
    if (dash_pos == std::string::npos) continue;
    int range_start = std::stoi(range_str.substr(0, dash_pos));
    int range_end = std::stoi(range_str.substr(dash_pos + 1));

    // 构建路由条目
    RouteEntry entry;
    entry.index = index;
    entry.ip = ip;
    entry.port = port;
    entry.range_start = range_start;
    entry.range_end = range_end;
    route_table.push_back(entry);
  }
  return !route_table.empty();
}

// ==================== 网络 IO ====================

// 连接到指定 IP:PORT，返回 fd（用于连接主节点）
int connect_to_master(const std::string& host, int port) {
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) return -1;

  struct sockaddr_in addr;
  std::memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
    close(sock); return -1;
  }
  if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
    close(sock); return -1;
  }
  return sock;
}

// 从从节点读取单行响应（用于 PUT/GET/DELETE，以 \r\n 或 \n 结束）
std::string read_slave_response(int fd) {
  std::string response;
  char buf[4096];
  while (true) {
    ssize_t n = read(fd, buf, sizeof(buf) - 1);
    if (n < 0) {
      if (errno == EINTR) continue;
      std::cerr << "读取从节点响应错误" << std::endl;
      return "";
    } else if (n == 0) {
      std::cout << "从节点关闭了连接" << std::endl;
      return "";
    }
    buf[n] = '\0';
    response += buf;
    // 单行响应以 \r\n 或 \n 结束
    if (response.find("\r\n") != std::string::npos ||
        response.find("\n") != std::string::npos) break;
  }
  return response;
}

// 从从节点读取多行响应（用于 SCAN，直到遇到 END\r\n）
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
    // SCAN 响应以 "END\r\n" 或 "END\n" 结束
    if (response.find("END\r\n") != std::string::npos ||
        response.find("END\n") != std::string::npos) break;
  }
  return response;
}

// ==================== 主函数 ====================

int main(int argc, char* argv[]) {
  std::string host = DEFAULT_HOST;
  int port = DEFAULT_PORT;

  // 解析命令行参数: ./client [ip] [port]
  if (argc >= 2) host = argv[1];
  if (argc >= 3) port = std::stoi(argv[2]);

  std::cout << "DisKV 客户端已启动" << std::endl;
  std::cout << "目标主节点: " << host << ":" << port << std::endl;
  std::cout << "请输入 HELLO 连接主节点获取路由表" << std::endl;

  // ★ 主交互循环
  std::string user_input;
  while (true) {
    std::cout << ">> ";  // 提示符
    if (!std::getline(std::cin, user_input)) {
      std::cout << "\n退出客户端" << std::endl; break;  // Ctrl+D
    }

    std::string input = trim_newline(user_input);
    if (input.empty()) continue;

    // 解析命令
    std::istringstream iss(input);
    std::string cmd;
    iss >> cmd;
    for (auto& c : cmd) c = std::toupper(static_cast<unsigned char>(c));

    // ===== HELLO：连接主节点，获取并解析路由表 =====
    if (cmd == "HELLO") {
      // ★ 连接到主节点
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
        close(master_sock); continue;
      }

      // ★ 读取路由表响应（多行，直到 END）
      std::string route_response;
      char recv_buf[4096];
      while (true) {
        ssize_t n = read(master_sock, recv_buf, sizeof(recv_buf) - 1);
        if (n < 0) {
          if (errno == EINTR) continue;
          std::cerr << "读取路由表响应错误" << std::endl; break;
        } else if (n == 0) break;  // 主节点主动关闭（正常）
        recv_buf[n] = '\0';
        route_response += recv_buf;
        if (route_response.find("END\r\n") != std::string::npos ||
            route_response.find("END\n") != std::string::npos) break;
      }
      close(master_sock);
      std::cout << "已从 Master 获取路由表并断开连接" << std::endl;

      // 打印路由表
      std::cout << "--- 路由表 ---" << std::endl;
      std::cout << route_response;
      if (!route_response.empty() && route_response.back() != '\n')
        std::cout << std::endl;
      std::cout << "--------------" << std::endl;

      // ★ 解析路由表，填充 route_table
      if (!parse_route_table(route_response)) {
        std::cerr << "错误: 解析路由表失败" << std::endl; continue;
      }
      std::cout << "路由表解析成功, 共 " << route_table.size() << " 个从节点" << std::endl;
    }

    // ===== QUIT：退出客户端 =====
    else if (cmd == "QUIT") {
      // 通知主节点注销
      int quit_sock = connect_to_master(host, port);
      if (quit_sock >= 0) {
        std::string quit_req = "QUIT\n";
        write(quit_sock, quit_req.c_str(), quit_req.size());
        char buf[64] = {0};
        read(quit_sock, buf, sizeof(buf) - 1);
        close(quit_sock);
      }
      std::cout << "已退出客户端" << std::endl;
      break;
    }

    // ===== PUT：存储键值对 =====
    // ★ 流程: 路由查询 → 连接从节点 → 发送 PUT 命令 → 读取响应
    else if (cmd == "PUT") {
      std::string key, value;
      iss >> key;
      std::string rest;
      std::getline(iss, rest);
      value = trim(rest);
      if (key.empty() || value.empty()) {
        std::cout << "ERROR: PUT requires key and value" << std::endl; continue;
      }

      int worker_idx = find_worker_for_key(key);
      if (worker_idx < 0) {
        std::cout << "ERROR: no slave found for key (please run HELLO first)" << std::endl;
        continue;
      }

      int slave_fd = get_slave_fd(worker_idx);
      if (slave_fd < 0) {
        std::cout << "ERROR: cannot connect to slave " << worker_idx << std::endl;
        slave_fds.erase(worker_idx); continue;
      }

      std::string request = "PUT " + key + " " + value + "\n";
      ssize_t w_sent = write(slave_fd, request.c_str(), request.size());
      if (w_sent < 0) {
        std::cerr << "发送 PUT 到从节点失败" << std::endl;
        close(slave_fd); slave_fds.erase(worker_idx); continue;
      }

      std::string response = read_slave_response(slave_fd);
      if (response.empty()) { close(slave_fd); slave_fds.erase(worker_idx); continue; }
      std::cout << trim(response) << std::endl;
    }

    // ===== GET：读取键值对 =====
    // ★ 流程: 路由查询 → 连接从节点 → 发送 GET 命令 → 读取响应
    else if (cmd == "GET") {
      std::string key;
      iss >> key;
      if (key.empty()) {
        std::cout << "ERROR: GET requires a key" << std::endl; continue;
      }

      int worker_idx = find_worker_for_key(key);
      if (worker_idx < 0) {
        std::cout << "ERROR: no slave found for key (please run HELLO first)" << std::endl;
        continue;
      }

      int slave_fd = get_slave_fd(worker_idx);
      if (slave_fd < 0) {
        std::cout << "ERROR: cannot connect to slave " << worker_idx << std::endl;
        slave_fds.erase(worker_idx); continue;
      }

      std::string request = "GET " + key + "\n";
      ssize_t w_sent = write(slave_fd, request.c_str(), request.size());
      if (w_sent < 0) {
        std::cerr << "发送 GET 到从节点失败" << std::endl;
        close(slave_fd); slave_fds.erase(worker_idx); continue;
      }

      std::string response = read_slave_response(slave_fd);
      if (response.empty()) { close(slave_fd); slave_fds.erase(worker_idx); continue; }
      std::cout << trim(response) << std::endl;
    }

    // ===== DELETE：删除键值对 =====
    // ★ 流程: 路由查询 → 连接从节点 → 发送 DELETE 命令 → 读取响应
    else if (cmd == "DELETE") {
      std::string key;
      iss >> key;
      if (key.empty()) {
        std::cout << "ERROR: DELETE requires a key" << std::endl; continue;
      }

      int worker_idx = find_worker_for_key(key);
      if (worker_idx < 0) {
        std::cout << "ERROR: no slave found for key (please run HELLO first)" << std::endl;
        continue;
      }

      int slave_fd = get_slave_fd(worker_idx);
      if (slave_fd < 0) {
        std::cout << "ERROR: cannot connect to slave " << worker_idx << std::endl;
        slave_fds.erase(worker_idx); continue;
      }

      std::string request = "DELETE " + key + "\n";
      ssize_t w_sent = write(slave_fd, request.c_str(), request.size());
      if (w_sent < 0) {
        std::cerr << "发送 DELETE 到从节点失败" << std::endl;
        close(slave_fd); slave_fds.erase(worker_idx); continue;
      }

      std::string response = read_slave_response(slave_fd);
      if (response.empty()) { close(slave_fd); slave_fds.erase(worker_idx); continue; }
      std::cout << trim(response) << std::endl;
    }

    // ===== SCAN：范围查询 =====
    // ★ 流程: 计算涉及的从节点范围 → 逐个发送 SCAN → 合并结果
    else if (cmd == "SCAN") {
      std::string first_key, last_key;
      iss >> first_key >> last_key;
      if (first_key.empty() || last_key.empty()) {
        std::cout << "ERROR: SCAN requires first_key and last_key" << std::endl; continue;
      }
      if (first_key > last_key) {
        std::cout << "ERROR: SCAN first_key must be <= last_key" << std::endl; continue;
      }

      int start_worker = find_worker_for_key(first_key);
      int end_worker = find_worker_for_key(last_key);
      if (start_worker < 0 || end_worker < 0) {
        std::cout << "ERROR: cannot determine slave for range (please run HELLO first)" << std::endl;
        continue;
      }

      // ★ 收集所有涉及从节点的结果（按顺序合并）
      std::string all_results;
      for (int idx = start_worker; idx <= end_worker; ++idx) {
        int slave_fd = get_slave_fd(idx);
        if (slave_fd < 0) {
          std::cerr << "警告: 无法连接到从节点 " << idx << std::endl;
          slave_fds.erase(idx); continue;
        }

        std::string request = "SCAN " + first_key + " " + last_key + "\n";
        ssize_t w_sent = write(slave_fd, request.c_str(), request.size());
        if (w_sent < 0) {
          std::cerr << "发送 SCAN 到从节点 " << idx << " 失败" << std::endl;
          close(slave_fd); slave_fds.erase(idx); continue;
        }

        std::string response = read_slave_range_response(slave_fd);
        if (response.empty()) { close(slave_fd); slave_fds.erase(idx); continue; }

        // 提取 KVPAIR 行，忽略每个从节点的 END 标记
        std::istringstream resp_iss(response);
        std::string resp_line;
        while (std::getline(resp_iss, resp_line)) {
          if (!resp_line.empty() && resp_line.back() == '\r') resp_line.pop_back();
          if (resp_line.empty()) continue;
          if (resp_line == "END") continue;  // 跳过单个从节点的 END
          if (resp_line.find("KVPAIR ") == 0) {
            std::string kv = resp_line.substr(7);  // 去掉 "KVPAIR "
            all_results += kv + "\r\n";
          }
        }
      }
      // ★ 输出合并后结果，末尾统一添加 END
      std::cout << all_results;
      std::cout << "END" << std::endl;
    }

    // ===== COMPACT：强制主节点执行全量 Compaction =====
    // ★ 客户端连接主节点 → 发送 COMPACT → 读取响应 → 输出 COMPACT OK
    else if (cmd == "COMPACT") {
      int master_sock = connect_to_master(host, port);
      if (master_sock < 0) {
        std::cerr << "错误: 无法连接到主节点 " << host << ":" << port << std::endl;
        continue;
      }
      std::string req = "COMPACT\n";
      write(master_sock, req.c_str(), req.size());
      std::string response = read_slave_response(master_sock);
      close(master_sock);
      std::cout << trim(response) << std::endl;
    }

    // ===== GC <slave_number>：强制指定从节点执行垃圾回收 =====
    // ★ 客户端根据 <slave_number> 直连对应从节点 → 发送 GC → 读取响应 → 输出 GC OK
    else if (cmd == "GC") {
      int slave_num = -1;
      if (!(iss >> slave_num)) {
        std::cout << "ERROR: GC requires slave number, e.g. GC 0" << std::endl;
        continue;
      }
      if (route_table.empty()) {
        std::cout << "ERROR: no route table (please run HELLO first)" << std::endl;
        continue;
      }
      if (slave_num < 0 || slave_num >= (int)route_table.size()) {
        std::cout << "ERROR: GC requires a valid slave number (0-"
                  << (int)route_table.size() - 1 << ")" << std::endl;
        continue;
      }

      int slave_fd = get_slave_fd(slave_num);
      if (slave_fd < 0) {
        std::cout << "ERROR: cannot connect to slave " << slave_num << std::endl;
        slave_fds.erase(slave_num); continue;
      }

      std::string req = "GC\n";
      ssize_t w_sent = write(slave_fd, req.c_str(), req.size());
      if (w_sent < 0) {
        std::cerr << "发送 GC 到从节点 " << slave_num << " 失败" << std::endl;
        close(slave_fd); slave_fds.erase(slave_num); continue;
      }
      std::string response = read_slave_response(slave_fd);
      if (response.empty()) {
        close(slave_fd); slave_fds.erase(slave_num); continue;
      }
      std::cout << trim(response) << std::endl;
    }

    else {
      std::cout << "ERROR: Unknown command. Use HELLO, PUT, GET, DELETE, SCAN, COMPACT, GC, QUIT" << std::endl;
    }
  }

  // 关闭所有从节点连接
  for (auto& pair : slave_fds) { if (pair.second >= 0) close(pair.second); }
  slave_fds.clear();
  return 0;
}
