// =============================================================================
// slave.cc - DisKV 从节点：仅存储 vlog 文件（*.log），不存储 LSM-Tree
// =============================================================================
//
// 架构说明（节点级键值分离）：
//   主节点 (Master) : 存储 LSM-Tree（key → vlog地址），负责元数据管理。
//   从节点 (Slave)  : 只存储 vlog 文件（*.log），保存实际的值数据。
//                     从节点通过网络与主节点通信：
//                       PUT:  写值到 vlog → 通知主节点存储地址
//                       GET:  向主节点查询地址 → 从 vlog 读取值
//                       DELETE: 通知主节点标记删除
//                       SCAN: 向主节点查询地址列表 → 从 vlog 批量读取值
//   客户端 (Client): 与从节点直接交互（协议不变）。
//
// 从节点数据库目录下仅包含：
//   *.log  — vlog 文件，存储实际值数据（永不删除，GC 异步回收）
//   LOG / LOG.old — 数据库运行日志
//   LOCK — 进程锁文件
//   （不包含 *.ldb / MANIFEST / CURRENT 等 LSM-Tree 文件）
//
// 内部协议（从节点 → 主节点，基于文本行，换行符 \n）：
//   PUT_ADDR <key> <vlog_numb> <offset> <size>
//   GET_ADDR <key>
//   DELETE_ADDR <key>
//   SCAN_ADDR <start_key> <end_key> <worker_id>
// =============================================================================

#include <arpa/inet.h>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <netinet/in.h>
#include <sstream>
#include <string>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

// WiscKey vlog 相关头文件
#include "db/vlog_manager.h"
#include "db/vlog_reader.h"
#include "db/vlog_writer.h"
#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/options.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "util/coding.h"

// ==================== 常量定义 ====================

int worker_id = -1;
const std::string MASTER_HOST = "127.0.0.1";
const int MASTER_PORT = 8889;
const int SLAVE_CLIENT_PORTS[3] = {9000, 9001, 9002};
const int MAX_EVENTS = 64;

// vlog 相关常量
static const int kVHeaderSize = 12;  // crc32(4) + length(8)

// ==================== 全局状态 ====================

static volatile bool g_running = true;
static leveldb::vlog::VlogManager* g_vlog_manager = nullptr;
static std::string g_dbname;
static leveldb::Options g_options;
static uint64_t g_vlog_number = 1;       // 当前 vlog 文件编号
static int g_master_fd = -1;             // 与主节点的连接

// 客户端连接状态
struct Connection {
  std::string send_buffer;
  std::string recv_buffer;
};
std::unordered_map<int, Connection> connections;

// ==================== 工具函数 ====================

static inline std::string trim(const std::string& s) {
  auto start = std::find_if_not(s.begin(), s.end(), ::isspace);
  auto end = std::find_if_not(s.rbegin(), s.rend(), ::isspace).base();
  return (start < end) ? std::string(start, end) : "";
}

void set_nonblocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

void signal_handler(int signum) {
  std::cout << "\n收到退出信号: " << signum << "，正在安全关闭..." << std::endl;
  g_running = false;
}

void register_signal_handler() {
  struct sigaction sa;
  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = signal_handler;
  sa.sa_flags = 0;
  sigaction(SIGINT, &sa, nullptr);
  sigaction(SIGTERM, &sa, nullptr);
}

// ==================== 与主节点通信 ====================

// 连接到主节点（阻塞，带重试）
int connect_to_master() {
  while (g_running) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { sleep(1); continue; }

    struct sockaddr_in master_addr;
    memset(&master_addr, 0, sizeof(master_addr));
    master_addr.sin_family = AF_INET;
    master_addr.sin_port = htons(MASTER_PORT);
    if (inet_pton(AF_INET, MASTER_HOST.c_str(), &master_addr.sin_addr) <= 0) {
      close(fd); return -1;
    }

    if (connect(fd, (struct sockaddr*)&master_addr, sizeof(master_addr)) < 0) {
      std::cerr << "Worker " << worker_id << ": 连接 master 失败，2秒后重试... ("
                << strerror(errno) << ")" << std::endl;
      close(fd); sleep(2); continue;
    }
    std::cout << "Worker " << worker_id << ": 已连接到主节点, fd=" << fd << std::endl;
    return fd;
  }
  return -1;
}

// 向主节点发送命令并等待单行响应（用于 PUT_ADDR / GET_ADDR / DELETE_ADDR）
// 使用阻塞式 I/O（g_master_fd 保持阻塞模式，不加入 epoll 读写）
// 注意：此函数在主节点连接正常时从客户端命令处理中调用，此时 epoll 循环
//       正在处理客户端事件，对主节点的读写采用简单的阻塞模式。
std::string send_to_master(const std::string& request) {
  if (g_master_fd < 0) {
    std::cerr << "Worker " << worker_id << ": 主节点未连接" << std::endl;
    return "ERROR: Master not connected\r\n";
  }

  // 发送请求（带 EAGAIN 重试）
  std::string req = request + "\n";
  size_t sent_total = 0;
  while (sent_total < req.size()) {
    ssize_t n = write(g_master_fd, req.data() + sent_total,
                      req.size() - sent_total);
    if (n < 0) {
      if (errno == EINTR) continue;
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        usleep(1000);  // 等待 1ms 后重试
        continue;
      }
      std::cerr << "Worker " << worker_id << ": 发送到主节点失败: "
                << strerror(errno) << std::endl;
      return "ERROR: Send to master failed\r\n";
    }
    sent_total += n;
  }

  // 读取单行响应（带 EAGAIN 重试）
  std::string response;
  char buf[4096];
  while (true) {
    ssize_t n = read(g_master_fd, buf, sizeof(buf) - 1);
    if (n < 0) {
      if (errno == EINTR) continue;
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        usleep(1000);  // 等待 1ms 后重试
        continue;
      }
      std::cerr << "Worker " << worker_id << ": 读取主节点响应错误: "
                << strerror(errno) << std::endl;
      return "ERROR: Read from master failed\r\n";
    } else if (n == 0) {
      std::cerr << "Worker " << worker_id << ": 主节点关闭了连接" << std::endl;
      return "ERROR: Master closed connection\r\n";
    }
    buf[n] = '\0';
    response += buf;
    if (response.find('\n') != std::string::npos) break;
  }
  return response;
}

// 向主节点发送 SCAN_ADDR 请求并读取多行响应（直到 END）
std::string send_scan_to_master(const std::string& request) {
  if (g_master_fd < 0) {
    return "ERROR: Master not connected\r\n";
  }

  // 发送请求（带 EAGAIN 重试）
  std::string req = request + "\n";
  size_t sent_total = 0;
  while (sent_total < req.size()) {
    ssize_t n = write(g_master_fd, req.data() + sent_total,
                      req.size() - sent_total);
    if (n < 0) {
      if (errno == EINTR) continue;
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        usleep(1000);
        continue;
      }
      return "ERROR: Send to master failed\r\n";
    }
    sent_total += n;
  }

  // 读取多行响应（直到 END）
  std::string response;
  char buf[4096];
  while (true) {
    ssize_t n = read(g_master_fd, buf, sizeof(buf) - 1);
    if (n < 0) {
      if (errno == EINTR) continue;
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        usleep(1000);
        continue;
      }
      return "ERROR: Read from master failed\r\n";
    } else if (n == 0) {
      return "ERROR: Master closed connection\r\n";
    }
    buf[n] = '\0';
    response += buf;
    if (response.find("END\r\n") != std::string::npos ||
        response.find("END\n") != std::string::npos) break;
  }
  return response;
}

// ==================== Vlog 操作 ====================

// 将值编码为 vlog 格式：<kTypeValue:1><key_len:varint><key><value_len:varint><value>
// 此格式与 vlog_fetcher.cc 的 Parse() 一致，FetchValueFromVlog 可自动解析。
std::string encode_vlog_entry(const std::string& key, const std::string& value) {
  std::string data;
  data.push_back(static_cast<char>(leveldb::kTypeValue));
  leveldb::PutLengthPrefixedSlice(&data, key);
  leveldb::PutLengthPrefixedSlice(&data, value);
  return data;
}

// 写入值到 vlog，返回编码后的地址字符串

// 写入值到 vlog，返回编码后的地址字符串
// 地址格式：<varint64:vlog_number><varint64:offset><varint64:size>
std::string write_value_to_vlog(const std::string& key, const std::string& value) {
  // 编码 vlog 条目
  std::string vlog_data = encode_vlog_entry(key, value);

  // 记录写入前的偏移位置（数据部分的起始偏移 = 当前写入位置 + 12字节头部）
  uint64_t data_offset = g_vlog_manager->GetWritePos() + kVHeaderSize;

  // 写入 vlog
  leveldb::Status s = g_vlog_manager->AddRecord(vlog_data);
  if (!s.ok()) {
    std::cerr << "Worker " << worker_id << ": vlog 写入失败: " << s.ToString() << std::endl;
    return "";
  }

  // 编码地址
  std::string addr;
  leveldb::PutVarint64(&addr, g_vlog_number);
  leveldb::PutVarint64(&addr, data_offset);
  leveldb::PutVarint64(&addr, vlog_data.size());

  std::cout << "Worker " << worker_id << ": vlog 写入 key=\"" << key
            << "\" vlog=" << g_vlog_number << " offset=" << data_offset
            << " size=" << vlog_data.size() << std::endl;

  return addr;
}

// ==================== 命令处理 ====================

std::string process_command(const std::string& line) {
  std::string trimmed = trim(line);
  if (trimmed.empty()) return "";

  std::istringstream iss(trimmed);
  std::string cmd, arg;
  iss >> cmd;
  std::getline(iss, arg);
  arg = trim(arg);
  for (auto& c : cmd) c = std::toupper(static_cast<unsigned char>(c));

  // ===== PUT =====
  if (cmd == "PUT") {
    // 格式: PUT <key> <value>
    size_t spc = arg.find(' ');
    if (spc == std::string::npos) return "ERROR: PUT requires key and value\r\n";
    std::string key = trim(arg.substr(0, spc));
    std::string value = trim(arg.substr(spc + 1));
    if (key.empty() || value.empty()) return "ERROR: PUT requires key and value\r\n";

    std::cout << "Worker " << worker_id << ": PUT key=\"" << key
              << "\" value=\"" << value << "\"" << std::endl;

    // 1. 写入值到本地 vlog
    std::string addr = write_value_to_vlog(key, value);
    if (addr.empty()) return "ERROR: Vlog write failed\r\n";

    // 2. 解码地址，发送给主节点
    leveldb::Slice addr_slice(addr);
    uint64_t vlog_numb, offset, size;
    leveldb::GetVarint64(&addr_slice, &vlog_numb);
    leveldb::GetVarint64(&addr_slice, &offset);
    leveldb::GetVarint64(&addr_slice, &size);

    // 3. 通过主节点连接发送 PUT_ADDR 命令
    std::string request = "PUT_ADDR " + key + " " +
                          std::to_string(vlog_numb) + " " +
                          std::to_string(offset) + " " +
                          std::to_string(size);
    std::string response = send_to_master(request);

    // 4. 解析主节点响应
    if (response.find("STORED") != std::string::npos) {
      return "STORED\r\n";
    } else {
      std::cerr << "Worker " << worker_id << ": 主节点 PUT_ADDR 失败: " << response << std::endl;
      return "ERROR: Master PUT_ADDR failed\r\n";
    }
  }

  // ===== GET =====
  else if (cmd == "GET") {
    // 格式: GET <key>
    std::string key = arg;
    if (key.empty()) return "ERROR: GET requires a key\r\n";
    std::cout << "Worker " << worker_id << ": GET key=\"" << key << "\"" << std::endl;

    // 1. 向主节点查询地址
    std::string request = "GET_ADDR " + key;
    std::string response = send_to_master(request);

    // 2. 解析响应
    if (response.find("NOT_FOUND") != std::string::npos) {
      return "NOT_FOUND\r\n";
    }
    if (response.find("ERROR") != std::string::npos) {
      return response;  // 透传错误
    }
    // ADDR <vlog_numb> <offset> <size>
    std::string addr_str = trim(response);
    if (addr_str.find("ADDR ") != 0) {
      return "ERROR: Unexpected master response\r\n";
    }
    addr_str = addr_str.substr(5);  // 去掉 "ADDR "

    std::istringstream addr_iss(addr_str);
    uint64_t vlog_numb, offset, size;
    if (!(addr_iss >> vlog_numb >> offset >> size)) {
      return "ERROR: Invalid address from master\r\n";
    }

    std::cout << "Worker " << worker_id << ": GET addr vlog=" << vlog_numb
              << " offset=" << offset << " size=" << size << std::endl;

    // 3. 从 vlog 读取值
    // 注意：FetchValueFromVlog 内部已调用 vlog_fetcher.cc 的 Parse()，
    // 该函数已经剥离了 kTypeValue 标签和 key，直接返回纯 value。
    // 因此 raw_value 就是最终需要的值，无需再次解析。
    std::string encoded_addr;
    leveldb::PutVarint64(&encoded_addr, vlog_numb);
    leveldb::PutVarint64(&encoded_addr, offset);
    leveldb::PutVarint64(&encoded_addr, size);

    std::string value;
    leveldb::Status s = g_vlog_manager->FetchValueFromVlog(encoded_addr, &value);
    if (!s.ok()) {
      std::cerr << "Worker " << worker_id << ": vlog 读取失败: " << s.ToString() << std::endl;
      return "ERROR: Vlog read failed\r\n";
    }

    // value 已经是纯值，直接返回
    std::cout << "Worker " << worker_id << ": GET 返回 value=\"" << value << "\"" << std::endl;
    return "VALUE " + value + "\r\n";
  }

  // ===== DELETE =====
  else if (cmd == "DELETE") {
    // 格式: DELETE <key>
    std::string key = arg;
    if (key.empty()) return "ERROR: DELETE requires a key\r\n";
    std::cout << "Worker " << worker_id << ": DELETE key=\"" << key << "\"" << std::endl;

    // 向主节点发送删除请求
    std::string request = "DELETE_ADDR " + key;
    std::string response = send_to_master(request);

    if (response.find("DELETED") != std::string::npos) {
      return "DELETED\r\n";
    } else if (response.find("NOT_FOUND") != std::string::npos) {
      return "NOT_FOUND\r\n";
    } else {
      return "ERROR: Master DELETE_ADDR failed\r\n";
    }
  }

  // ===== SCAN =====
  else if (cmd == "SCAN") {
    // 格式: SCAN <start_key> <end_key>
    size_t spc = arg.find(' ');
    if (spc == std::string::npos) return "ERROR: SCAN requires start_key and end_key\r\n";
    std::string start_key = trim(arg.substr(0, spc));
    std::string end_key = trim(arg.substr(spc + 1));
    if (start_key.empty() || end_key.empty()) return "ERROR: SCAN requires start_key and end_key\r\n";
    if (start_key > end_key) return "ERROR: SCAN first_key must be <= last_key\r\n";

    std::cout << "Worker " << worker_id << ": SCAN start=\"" << start_key
              << "\" end=\"" << end_key << "\"" << std::endl;

    // 1. 向主节点请求范围地址查询
    std::string request = "SCAN_ADDR " + start_key + " " + end_key + " " +
                          std::to_string(worker_id);
    std::string response = send_scan_to_master(request);

    if (response.find("ERROR") != std::string::npos) {
      return response;  // 透传错误
    }

    // 2. 解析响应中的 KVPAIR_ADDR 行
    std::string result;
    std::istringstream resp_iss(response);
    std::string resp_line;
    int count = 0;

    while (std::getline(resp_iss, resp_line)) {
      if (!resp_line.empty() && resp_line.back() == '\r') resp_line.pop_back();
      resp_line = trim(resp_line);
      if (resp_line.empty()) continue;
      if (resp_line == "END") continue;

      // KVPAIR_ADDR <key> <vlog_numb> <offset> <size>
      if (resp_line.find("KVPAIR_ADDR ") == 0) {
        std::string kv = resp_line.substr(12);  // 去掉 "KVPAIR_ADDR "
        std::istringstream kv_iss(kv);
        std::string scan_key;
        uint64_t vlog_numb, offset, size;
        if (!(kv_iss >> scan_key >> vlog_numb >> offset >> size)) continue;

        // 从 vlog 读取值
        // 注意：FetchValueFromVlog 已通过 vlog_fetcher.cc 的 Parse() 剥离
        // kTypeValue 和 key，直接返回纯 value，无需再次调用 parse_vlog_value。
        std::string encoded_addr;
        leveldb::PutVarint64(&encoded_addr, vlog_numb);
        leveldb::PutVarint64(&encoded_addr, offset);
        leveldb::PutVarint64(&encoded_addr, size);

        std::string scan_value;
        leveldb::Status s = g_vlog_manager->FetchValueFromVlog(encoded_addr, &scan_value);
        if (!s.ok()) {
          std::cerr << "Worker " << worker_id << ": SCAN vlog 读取失败: "
                    << s.ToString() << std::endl;
          continue;
        }

        result += "KVPAIR " + scan_key + " " + scan_value + "\r\n";
        count++;
      }
    }

    result += "END\r\n";
    std::cout << "Worker " << worker_id << ": SCAN 返回 " << count << " 条结果" << std::endl;
    return result;
  }

  else {
    std::cout << "Worker " << worker_id << ": 未知命令: " << cmd << std::endl;
    return "ERROR unknown command\r\n";
  }
}

// ==================== 初始化 Vlog ====================

bool init_vlog(const std::string& dbname) {
  g_dbname = dbname;

  // 创建数据库目录
  leveldb::Env::Default()->CreateDir(dbname);

  // 创建 VlogManager（clean_threshold 设为很大的值，基本不做 GC）
  g_options.env = leveldb::Env::Default();
  g_options.clean_threshold = 1000000;
  g_options.min_clean_threshold = 500000;
  g_options.log_dropCount_threshold = 100000;
  g_options.max_vlog_size = 64 * 1024 * 1024;  // 64MB

  g_vlog_manager = new leveldb::vlog::VlogManager(g_options.clean_threshold);

  // 创建初始 vlog 文件
  g_vlog_manager->AddVlog(dbname, g_options, g_vlog_number);

  std::cout << "Worker " << worker_id << ": Vlog 已初始化, vlog_number="
            << g_vlog_number << ", 路径: " << dbname << std::endl;
  std::cout << "  - 仅存储 .log (vlog) 文件，无 LSM-Tree" << std::endl;
  return true;
}

void shutdown_vlog() {
  if (g_vlog_manager) {
    delete g_vlog_manager;
    g_vlog_manager = nullptr;
  }
}

// ==================== 主函数 ====================

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cerr << "用法: " << argv[0] << " <worker_id (0-2)>" << std::endl;
    return 1;
  }
  worker_id = std::atoi(argv[1]);
  if (worker_id < 0 || worker_id > 2) {
    std::cerr << "错误: worker_id 必须在 0~2 之间" << std::endl;
    return 1;
  }

  register_signal_handler();

  // ===== 0. 初始化 Vlog =====
  std::string db_path = "./diskv_slave_vlog_" + std::to_string(worker_id);
  if (!init_vlog(db_path)) {
    std::cerr << "Worker " << worker_id << ": Vlog 初始化失败" << std::endl;
    return 1;
  }

  // ===== 1. 连接到主节点 =====
  std::cout << "Worker " << worker_id << ": 正在连接主节点 " << MASTER_HOST
            << ":" << MASTER_PORT << " ..." << std::endl;
  g_master_fd = connect_to_master();
  if (g_master_fd < 0 || !g_running) {
    shutdown_vlog();
    return 1;
  }

  // ===== 2. 创建客户端监听套接字 =====
  int slave_listen_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (slave_listen_fd < 0) {
    std::cerr << "Worker " << worker_id << ": 创建监听套接字失败" << std::endl;
    close(g_master_fd);
    shutdown_vlog();
    return 1;
  }
  int opt = 1;
  setsockopt(slave_listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

  struct sockaddr_in slave_addr;
  memset(&slave_addr, 0, sizeof(slave_addr));
  slave_addr.sin_family = AF_INET;
  slave_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  slave_addr.sin_port = htons(SLAVE_CLIENT_PORTS[worker_id]);

  if (bind(slave_listen_fd, (struct sockaddr*)&slave_addr, sizeof(slave_addr)) < 0) {
    std::cerr << "Worker " << worker_id << ": 绑定失败 (" << strerror(errno) << ")" << std::endl;
    close(slave_listen_fd); close(g_master_fd); shutdown_vlog(); return 1;
  }
  if (listen(slave_listen_fd, 128) < 0) {
    std::cerr << "Worker " << worker_id << ": 监听失败" << std::endl;
    close(slave_listen_fd); close(g_master_fd); shutdown_vlog(); return 1;
  }
  std::cout << "Worker " << worker_id << ": 客户端监听已建立 [127.0.0.1:"
            << SLAVE_CLIENT_PORTS[worker_id] << "]" << std::endl;

  // ===== 3. 创建 epoll 实例 =====
  int epoll_fd = epoll_create1(0);
  if (epoll_fd < 0) {
    std::cerr << "Worker " << worker_id << ": 创建 epoll 失败" << std::endl;
    close(slave_listen_fd); close(g_master_fd); shutdown_vlog(); return 1;
  }

  // 将客户端监听套接字加入 epoll
  struct epoll_event ev;
  ev.events = EPOLLIN;
  ev.data.fd = slave_listen_fd;
  if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, slave_listen_fd, &ev) < 0) {
    std::cerr << "Worker " << worker_id << ": 添加监听 fd 到 epoll 失败" << std::endl;
    close(epoll_fd); close(slave_listen_fd); close(g_master_fd); shutdown_vlog(); return 1;
  }

  // 将主节点连接加入 epoll（检测断开）
  set_nonblocking(g_master_fd);
  ev.events = EPOLLIN | EPOLLET;
  ev.data.fd = g_master_fd;
  if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, g_master_fd, &ev) < 0) {
    std::cerr << "Worker " << worker_id << ": 添加 master_fd 到 epoll 失败" << std::endl;
  }

  // ===== 4. 主事件循环 =====
  struct epoll_event events[MAX_EVENTS];
  while (g_running) {
    int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, 1000);
    if (nfds < 0) {
      if (!g_running) break;
      if (errno == EINTR) continue;
      std::cerr << "Worker " << worker_id << ": epoll_wait 错误" << std::endl;
      break;
    }

    for (int i = 0; i < nfds; ++i) {
      int fd = events[i].data.fd;

      // ---- 主节点事件（断开或异常） ----
      if (fd == g_master_fd) {
        if (events[i].events & (EPOLLERR | EPOLLHUP)) {
          std::cout << "Worker " << worker_id << ": 主节点断开连接" << std::endl;
          epoll_ctl(epoll_fd, EPOLL_CTL_DEL, g_master_fd, nullptr);
          close(g_master_fd);
          g_master_fd = -1;
          // 尝试重连
          std::cout << "Worker " << worker_id << ": 尝试重新连接主节点..." << std::endl;
          g_master_fd = connect_to_master();
          if (g_master_fd >= 0) {
            set_nonblocking(g_master_fd);
            ev.events = EPOLLIN | EPOLLET;
            ev.data.fd = g_master_fd;
            epoll_ctl(epoll_fd, EPOLL_CTL_ADD, g_master_fd, &ev);
          }
        } else if (events[i].events & EPOLLIN) {
          char buf[64];
          ssize_t n = read(g_master_fd, buf, sizeof(buf));
          if (n <= 0) {
            std::cout << "Worker " << worker_id << ": 主节点关闭连接" << std::endl;
            epoll_ctl(epoll_fd, EPOLL_CTL_DEL, g_master_fd, nullptr);
            close(g_master_fd);
            g_master_fd = connect_to_master();
            if (g_master_fd >= 0) {
              set_nonblocking(g_master_fd);
              ev.events = EPOLLIN | EPOLLET;
              ev.data.fd = g_master_fd;
              epoll_ctl(epoll_fd, EPOLL_CTL_ADD, g_master_fd, &ev);
            }
          }
        }
        continue;
      }

      // ---- 新客户端连接 ----
      if (fd == slave_listen_fd) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(slave_listen_fd, (struct sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) {
          if (errno == EAGAIN || errno == EWOULDBLOCK) continue;
          std::cerr << "Worker " << worker_id << ": accept 失败" << std::endl;
          continue;
        }

        set_nonblocking(client_fd);
        connections[client_fd] = Connection();

        ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
        ev.data.fd = client_fd;
        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev) < 0) {
          std::cerr << "Worker " << worker_id << ": 添加客户端 fd 到 epoll 失败" << std::endl;
          close(client_fd);
          continue;
        }

        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
        int client_port = ntohs(client_addr.sin_port);
        std::cout << "Worker " << worker_id << ": 新客户端 fd=" << client_fd
                  << " socket=" << client_ip << ":" << client_port << std::endl;
        continue;
      }

      // ---- 客户端数据 ----
      bool closed = false;
      auto conn_it = connections.find(fd);
      if (conn_it == connections.end()) { close(fd); continue; }
      Connection& conn = conn_it->second;

      if (events[i].events & (EPOLLERR | EPOLLHUP)) {
        std::cerr << "Worker " << worker_id << ": 客户端 fd=" << fd << " 异常" << std::endl;
        closed = true;
      }

      // 可写
      if (!closed && (events[i].events & EPOLLOUT)) {
        while (!conn.send_buffer.empty()) {
          int bw = write(fd, conn.send_buffer.data(), conn.send_buffer.size());
          if (bw < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            std::cerr << "Worker " << worker_id << ": 写入错误 fd=" << fd << std::endl;
            closed = true; break;
          } else if (bw == 0) continue;
          else conn.send_buffer.erase(0, bw);
        }
        if (conn.send_buffer.empty()) {
          ev.events = EPOLLIN | EPOLLET;
          ev.data.fd = fd;
          epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev);
        }
      }

      // 可读
      if (!closed && (events[i].events & EPOLLIN)) {
        while (true) {
          char temp_buffer[256] = {0};
          int br = read(fd, temp_buffer, sizeof(temp_buffer) - 1);
          if (br > 0) {
            conn.recv_buffer.append(temp_buffer, br);
            size_t pos;
            while ((pos = conn.recv_buffer.find('\n')) != std::string::npos) {
              std::string line = conn.recv_buffer.substr(0, pos);
              conn.recv_buffer.erase(0, pos + 1);
              std::string response = process_command(line);
              if (!response.empty()) conn.send_buffer += response;
            }
          } else if (br == 0) {
            std::cout << "Worker " << worker_id << ": 客户端 fd=" << fd << " 关闭连接" << std::endl;
            closed = true; break;
          } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            std::cerr << "Worker " << worker_id << ": 读取错误 fd=" << fd << std::endl;
            closed = true; break;
          }
        }
        if (!closed && !conn.send_buffer.empty()) {
          ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
          ev.data.fd = fd;
          epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev);
        }
      }

      if (closed) {
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
        close(fd);
        connections.erase(fd);
      }
    }
  }

  // ===== 5. 安全关闭 =====
  std::cout << "Worker " << worker_id << ": 正在关闭..." << std::endl;
  close(epoll_fd);
  close(slave_listen_fd);
  if (g_master_fd >= 0) close(g_master_fd);
  for (auto& pair : connections) close(pair.first);
  connections.clear();
  shutdown_vlog();
  // 注意：g_env 指向 Env::Default() 单例，不需要 delete
  std::cout << "Worker " << worker_id << ": 已安全退出" << std::endl;
  return 0;
}
