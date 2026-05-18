// =============================================================================
// slave.cc - DisKV 从节点（值存储服务器）
// =============================================================================
//
// 【架构定位】从节点是分布式系统中的"值存储层"，只管理 vlog 文件（*.log）。
//           不运行 LSM-Tree，不产生 .ldb / MANIFEST / CURRENT 文件。
//           所有 key→addr 映射存储在主节点的 LSM-Tree 中。
//
// 【职责范围】
//   1. 接收客户端 PUT/GET/DELETE/SCAN 请求（客户端直连从节点）
//   2. 将值写入本地 vlog，计算地址，通知主节点存储地址
//   3. 查询时向主节点请求地址，从本地 vlog 读取值
//   4. 后台 GC 线程：回收旧 vlog 文件中的垃圾空间
//   5. 持久化 vlog 元数据到 VLOG_META 文件，支持宕机恢复
//
// 【数据库目录结构】 ./diskv_slave_vlog_<id>/
//   VLOG_META   — 文本文件，记录 vlog_number 和 write_pos
//   xxxxxx.log       — vlog 文件，存储实际值数据（vlog 格式编码）
//   LOCK        — 进程锁文件
//   LOG/LOG.old — 数据库运行日志
//
// 【关键数据结构】
//   g_vlog_manager       — VlogManager 指针，管理所有 .log 文件
//   g_master_fd          — 与主节点的 TCP 连接（客户端请求+GC 共用）
//   g_vlog_mutex         — vlog 读写锁（客户端请求 ↔ GC 线程）
//   g_master_mutex       — 主节点连接锁（客户端请求 ↔ GC 线程）
//   g_gc_thread          — GC 后台线程句柄
//
// 【网络端口】
//   连接到主节点: 8889 + worker_id（每个 worker 连接对应端口）
//   监听客户端:   9000 + worker_id
//
// 【vlog 编码格式】每条记录:
//   Header(12B): CRC32(4B) | Length(8B)
//   Body:        kTypeValue(1B) | Varint32(key_len) | key | Varint32(value_len) | value
//
// 【地址格式】<Varint64:vlog_number><Varint64:offset><Varint64:size>
//   存入主节点 LSM-Tree 的值是此格式，供 FetchValueFromVlog 解析
// =============================================================================

#include <arpa/inet.h>     // inet_pton, htons
#include <csignal>         // sigaction, SIGINT, SIGTERM
#include <cstdlib>         // std::atoi, std::stoull
#include <cstring>         // memset, strerror
#include <dirent.h>        // opendir, readdir, closedir (扫描 .log 文件)
#include <errno.h>         // errno, EAGAIN, EINTR
#include <fcntl.h>         // fcntl, O_NONBLOCK
#include <fstream>         // std::ifstream, std::ofstream (VLOG_META 读写)
#include <iostream>        // std::cout, std::cerr
#include <netinet/in.h>    // sockaddr_in
#include <pthread.h>       // pthread_create, pthread_join, pthread_mutex_*
#include <sstream>         // std::istringstream
#include <string>          // std::string
#include <sys/epoll.h>     // epoll_create1, epoll_ctl, epoll_wait
#include <sys/socket.h>    // socket, bind, listen, accept, connect
#include <sys/stat.h>      // stat (获取文件大小)
#include <unistd.h>        // close, read, write, sleep, usleep
#include <unordered_map>   // std::unordered_map (connections)
#include <vector>          // std::vector

// WiscKey vlog 核心模块
#include "db/vlog_manager.h"   // VlogManager (vlog 文件管理)
#include "db/vlog_reader.h"    // VReader (顺序读取 vlog 记录)
#include "db/vlog_writer.h"    // VWriter (追加写入 vlog)
#include "db/filename.h"       // LogFileName, ParseFileName
#include "db/dbformat.h"       // kTypeValue, kTypeDeletion
#include "leveldb/env.h"       // Env (文件系统抽象)
#include "leveldb/options.h"   // Options
#include "leveldb/slice.h"     // Slice
#include "leveldb/status.h"    // Status
#include "util/coding.h"       // PutVarint64, GetVarint64, PutLengthPrefixedSlice, GetLengthPrefixedSlice

// ==================== 系统常量 ====================

int worker_id = -1;                                // 本从节点编号（0/1/2），由命令行参数指定
const std::string MASTER_HOST = "127.0.0.1";       // 主节点 IP
// 主节点为每个从节点分配独立端口：Worker i 连接 port 8889+i
int g_master_port = 0;                             // 在 main() 中根据 worker_id 计算
const int SLAVE_CLIENT_PORTS[3] = {9000, 9001, 9002};  // 从节点监听客户端连接的端口
const int MAX_EVENTS = 64;                         // epoll 一次轮询最多处理的事件数

// vlog 常量
static const int kVHeaderSize = 12;                // vlog 记录头部大小: CRC32(4B) + Length(8B)

// GC 参数（阈值较大，避免频繁 GC）
static const uint64_t kGcIntervalSec = 30;         // GC 检查间隔（秒）
static const int kGcVlogFileCountThreshold = 3;    // vlog 文件数超过此值触发 GC
static const int VlogSize = 32 * 1024 * 1024;      // 单个 vlog 文件大小(Bytes)
//   每次 GC 处理最旧的 (threshold/3) 个 vlog 文件（向下取整），
//   而非逐文件处理。多文件批量 GC 可减少 GC 唤醒频率，提高吞吐。

// ==================== 全局状态 ====================

static volatile bool g_running = true;             // 运行标志（信号处理器设为 false）
static leveldb::vlog::VlogManager* g_vlog_manager = nullptr;  // vlog 管理器
static std::string g_dbname;                       // 数据库目录路径
static leveldb::Options g_options;                 // WiscKey 配置选项
static uint64_t g_vlog_number = 1;                 // 当前 vlog 文件编号
static int g_master_fd = -1;                       // 与主节点的 TCP 连接（客户端请求+GC 共用）
static pthread_mutex_t g_master_mutex = PTHREAD_MUTEX_INITIALIZER;  // 保护 g_master_fd 并发访问。临界资源：文件描述符；临界区：send_to_master_via_fd()，send_scan_via_fd()
static pthread_mutex_t g_vlog_mutex = PTHREAD_MUTEX_INITIALIZER;    // 保护 vlog 读写并发访问。临界资源：vlog；临界区：write_value_to_vlog()，FetchValueFromVlog()
static pthread_t g_gc_thread = 0;                  // GC 后台线程句柄
static volatile bool g_gc_running = false;         // GC 线程运行标志

// 客户端连接状态（每个客户端独立缓冲区）
struct Connection {
  std::string send_buffer;   // 用户态发送缓冲区
  std::string recv_buffer;   // 用户态接收缓冲区
};
std::unordered_map<int, Connection> connections;   // client_fd → Connection 映射

// ==================== 通用工具函数 ====================

// 去除字符串首尾空白字符
static inline std::string trim(const std::string& s) {
  auto start = std::find_if_not(s.begin(), s.end(), ::isspace);
  auto end = std::find_if_not(s.rbegin(), s.rend(), ::isspace).base();
  return (start < end) ? std::string(start, end) : "";
}

// 将 fd 设为非阻塞模式（O_NONBLOCK）
// ★ ET 模式 epoll 要求 fd 非阻塞，避免 read/write 阻塞整个事件循环
void set_nonblocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

// 信号处理器：设置为 g_running=false 触发安全退出
void signal_handler(int signum) {
  std::cout << "\n收到退出信号: " << signum << "，正在安全关闭..." << std::endl;
  g_running = false;
}

// 使用 sigaction 注册信号处理器（不使用 SA_RESTART 标志）
// ★ 关键：不设置 SA_RESTART，确保被信号中断的 read/write/accept 返回 EINTR
//   而不是自动重启，避免程序看起来"卡死"在退出状态
void register_signal_handler() {
  struct sigaction sa;
  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = signal_handler;
  sa.sa_flags = 0;  // 不设置 SA_RESTART
  sigaction(SIGINT, &sa, nullptr);
  sigaction(SIGTERM, &sa, nullptr);
}

// ==================== 与主节点通信 ====================

// 连接到主节点（阻塞模式，失败后 2 秒重试）
// ★ 返回值: 成功返回 fd，失败或用户退出返回 -1
int connect_to_master() {
  while (g_running) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { sleep(1); continue; }

    struct sockaddr_in master_addr;
    memset(&master_addr, 0, sizeof(master_addr));
    master_addr.sin_family = AF_INET;
    master_addr.sin_port = htons(g_master_port);  // ★ 连接到 worker_id 对应的端口
    if (inet_pton(AF_INET, MASTER_HOST.c_str(), &master_addr.sin_addr) <= 0) {
      close(fd); return -1;
    }

    if (connect(fd, (struct sockaddr*)&master_addr, sizeof(master_addr)) < 0) {
      std::cerr << "Worker " << worker_id << ": 连接 master 失败，2秒后重试... ("
                << strerror(errno) << ")" << std::endl;
      close(fd); sleep(2); continue;
    }
    return fd;  // 连接成功
  }
  return -1;
}

// 通过指定 fd 向主节点发送请求并读取单行响应（直到 \n）
// ★ 支持 EAGAIN 重试，适用于阻塞和非阻塞 fd
// ★ 调用方负责持有 g_master_mutex 或使用独立 fd
static std::string send_to_master_via_fd(int fd, const std::string& request) {
  if (fd < 0) return "ERROR: Master not connected\r\n";

  // 发送请求（带 EAGAIN 重试）
  std::string req = request + "\n";
  size_t sent_total = 0;
  while (sent_total < req.size()) {
    ssize_t n = write(fd, req.data() + sent_total, req.size() - sent_total);
    if (n < 0) {
      if (errno == EINTR) continue;
      if (errno == EAGAIN || errno == EWOULDBLOCK) { usleep(1000); continue; }
      return "ERROR: Send to master failed\r\n";
    }
    sent_total += n;
  }

  // 读取单行响应（直到 \n）
  std::string response;
  char buf[4096];
  while (true) {
    ssize_t n = read(fd, buf, sizeof(buf) - 1);
    if (n < 0) {
      if (errno == EINTR) continue;
      if (errno == EAGAIN || errno == EWOULDBLOCK) { usleep(1000); continue; }
      return "ERROR: Read from master failed\r\n";
    } else if (n == 0) {
      return "ERROR: Master closed connection\r\n";
    }
    buf[n] = '\0';
    response += buf;
    if (response.find('\n') != std::string::npos) break;  // 读到换行即完成
  }
  return response;
}

// ★ 线程安全：通过 g_master_fd 发送（客户端请求路径）
//   内部加锁，确保与 GC 线程的并发访问串行化
std::string send_to_master(const std::string& request) {
  pthread_mutex_lock(&g_master_mutex);
  std::string resp = send_to_master_via_fd(g_master_fd, request);
  pthread_mutex_unlock(&g_master_mutex);
  return resp;
}

// 通过独立 fd 发送多行请求（SCAN 用，读到 END 为止）
static std::string send_scan_via_fd(int fd, const std::string& request) {
  if (fd < 0) return "ERROR: Master not connected\r\n";

  std::string req = request + "\n";
  size_t sent_total = 0;
  while (sent_total < req.size()) {
    ssize_t n = write(fd, req.data() + sent_total, req.size() - sent_total);
    if (n < 0) {
      if (errno == EINTR) continue;
      if (errno == EAGAIN || errno == EWOULDBLOCK) { usleep(1000); continue; }
      return "ERROR: Send to master failed\r\n";
    }
    sent_total += n;
  }

  // 读取多行响应，直到遇到 END
  std::string response;
  char buf[4096];
  while (true) {
    ssize_t n = read(fd, buf, sizeof(buf) - 1);
    if (n < 0) {
      if (errno == EINTR) continue;
      if (errno == EAGAIN || errno == EWOULDBLOCK) { usleep(1000); continue; }
      return "ERROR: Read from master failed\r\n";
    } else if (n == 0) return "ERROR: Master closed connection\r\n";
    buf[n] = '\0';
    response += buf;
    if (response.find("END\r\n") != std::string::npos ||
        response.find("END\n") != std::string::npos) break;
  }
  return response;
}

// ★ 线程安全：通过 g_master_fd 发送 SCAN（客户端请求路径）
std::string send_scan_to_master(const std::string& request) {
  pthread_mutex_lock(&g_master_mutex);
  std::string resp = send_scan_via_fd(g_master_fd, request);
  pthread_mutex_unlock(&g_master_mutex);
  return resp;
}

// ==================== Vlog 持久化 ====================
// 由于总元数据文件在主节点MANIFEST文件中存储，从节点需要额外的元数据文件。

// VLOG_META 文件路径
// ★ VLOG_META 格式（纯文本）:
//   vlog_number: <当前vlog文件编号>
//   write_pos:   <当前写入位置(字节)>
//   gc_threshold: <当前 GC 阈值>        — 动态阈值，GC 频繁时自动升高以抑制 GC 风暴
//   gc_consecutive: <连续无删除GC次数>   — 重置后从 0 开始计数
static std::string vlog_meta_path() { return g_dbname + "/VLOG_META"; }

// 当前生效的 GC 阈值（初始 = kGcVlogFileCountThreshold，可动态调整）
// ★ 设计目的: 当 GC 搬迁了大量有效数据但未能减少 vlog 文件数时（所有数据都有效），
//   说明当前阈值过低。动态升高阈值可避免 GC 空转（GC 风暴）。
static int g_gc_threshold = kGcVlogFileCountThreshold;
// 连续 GC 未删除任何文件的次数计数器（成功删除 ≥1 个文件后重置）
static int g_gc_consecutive_no_delete = 0;

// 持久化 vlog 元数据到 VLOG_META 文件
// ★ 调用时机:
//   1. 每次 PUT 成功后（确保宕机后可恢复最新写入位置）
//   2. GC 完成后
//   3. 正常退出时（shutdown_vlog）
void save_vlog_meta() {
  uint64_t pos = 0;
  if (g_vlog_manager) pos = g_vlog_manager->GetWritePos();

  std::ofstream ofs(vlog_meta_path(), std::ios::trunc);
  if (!ofs.is_open()) {
    std::cerr << "Worker " << worker_id << ": 无法写入 VLOG_META" << std::endl;
    return;
  }
  ofs << "vlog_number: " << g_vlog_number << "\n";
  ofs << "write_pos: " << pos << "\n";
  ofs << "gc_threshold: " << g_gc_threshold << "\n";
  ofs << "gc_consecutive: " << g_gc_consecutive_no_delete << "\n";
  ofs.close();
  std::cout << "Worker " << worker_id << ": VLOG_META 已保存 (vlog="
            << g_vlog_number << ", pos=" << pos
            << ", gc_threshold=" << g_gc_threshold << ")" << std::endl;
}

// ==================== Vlog 操作 ====================

// 前向声明（用于 GC 命令在定义之前引用）
static std::vector<uint64_t> scan_vlog_files(const std::string& dbname);
static std::vector<uint64_t> collect_gc_targets(const std::vector<uint64_t>& vlog_files, int max_count);
static uint64_t get_file_size(const std::string& path);
static int execute_gc_on_files(const std::vector<uint64_t>& targets, bool check_stop);

// 将 (key, value) 编码为 vlog 格式:
//   <kTypeValue:1><Varint32(key_len)><key><Varint32(value_len)><value>
// ★ 此格式与 vlog_fetcher.cc 的 Parse() 兼容，FetchValueFromVlog 可自动解析
std::string encode_vlog_entry(const std::string& key, const std::string& value) {
  std::string data;
  data.push_back(static_cast<char>(leveldb::kTypeValue));  // tag = kTypeValue
  leveldb::PutLengthPrefixedSlice(&data, key);              // len_prefix + key
  leveldb::PutLengthPrefixedSlice(&data, value);            // len_prefix + value
  return data;
}

// 从 vlog 原始数据中解析出 key 和 value（用于 GC 验证）
// ★ vlog 格式: <kTypeValue:1><Varint32(key_len)><key><Varint32(value_len)><value>
static bool parse_vlog_key_value(const std::string& raw, std::string* key,
                                  std::string* value) {
  leveldb::Slice s(raw);
  if (s.empty() || s[0] != leveldb::kTypeValue) return false;
  s.remove_prefix(1);  // 跳过 tag
  leveldb::Slice k, v;
  if (!leveldb::GetLengthPrefixedSlice(&s, &k)) return false;
  if (!leveldb::GetLengthPrefixedSlice(&s, &v)) return false;
  if (key) key->assign(k.data(), k.size());
  if (value) value->assign(v.data(), v.size());
  return true;
}

// 写入值到 vlog，返回编码后的地址字符串
//
// ★ 关键函数：PUT 请求的核心。
//   1. 加 vlog 互斥锁（与 GC 线程互斥）
//   2. 检查是否需要创建新 vlog 文件（达到大小上限）
//   3. 编码 vlog 条目（encode_vlog_entry）
//   4. 记录写入前的偏移位置（data_offset）
//   5. 调用 VlogManager::AddRecord 追加写入
//   6. 释放锁
//   7. 编码地址并返回
//
// 返回值: 成功返回编码地址 <Varint64:vlog_number><Varint64:offset><Varint64:size>
//         失败返回空字符串
std::string write_value_to_vlog(const std::string& key, const std::string& value) {
  pthread_mutex_lock(&g_vlog_mutex);  // ★ 锁：与 GC 线程互斥

  // 检查是否需要创建新 vlog 文件
  //   — 若当前写入位置已超过 max_vlog_size，先将旧 vlog 的缓冲区刷盘，
  //     再创建新 vlog，确保 .log 文件能实时反映出已写入的数据
  if (g_vlog_manager->GetWritePos() >= g_options.max_vlog_size) {
    // 将当前 vlog 内存缓冲区中的数据强制写入磁盘
    // *注：vlog中的数据会先写在4KB内存缓冲区中，若不强制写入磁盘，vlog文件可能不会及时更新。
    g_vlog_manager->FlushCurrentBuffer();
    // 创建新 vlog 文件（编号自增）
    g_vlog_number++;
    g_vlog_manager->AddVlog(g_dbname, g_options, g_vlog_number);
    std::cout << "Worker " << worker_id << ": 创建新 vlog 文件, number="
              << g_vlog_number << std::endl;
  }

  // 编码 vlog 条目
  std::string vlog_data = encode_vlog_entry(key, value);
  // 计算数据部分在 vlog 文件中的偏移（当前写入位置 + 12字节 Header）
  uint64_t data_offset = g_vlog_manager->GetWritePos() + kVHeaderSize;

  // ★ 核心写入：追加到当前 vlog 文件
  leveldb::Status s = g_vlog_manager->AddRecord(vlog_data);

  pthread_mutex_unlock(&g_vlog_mutex);  // 释放锁

  if (!s.ok()) {
    std::cerr << "Worker " << worker_id << ": vlog 写入失败: " << s.ToString() << std::endl;
    return "";
  }

  // ★ 编码地址：三段 Varint64 紧凑格式
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

// 处理客户端发来的命令，返回响应字符串
//
// ★ 此函数是客户端交互的核心。
//   命令格式: <CMD> [args...]
//   支持: PUT / GET / DELETE / SCAN
//
// 注意: PUT/GET 需要与主节点通信（通过 send_to_master），
//       主节点连接被 g_master_mutex 保护。
std::string process_command(const std::string& line) {
  std::string trimmed = trim(line);
  if (trimmed.empty()) return "";

  // 解析命令和参数
  std::istringstream iss(trimmed);
  std::string cmd, arg;
  iss >> cmd;
  std::getline(iss, arg);  // 剩余部分作为参数（可能包含空格）
  arg = trim(arg);
  for (auto& c : cmd) c = std::toupper(static_cast<unsigned char>(c));

  // ===== PUT：存储键值对 =====
  // 流程: 客户端发送 PUT <key> <value>
  //   → 从节点写入 vlog 并获取地址
  //   → 从节点向主节点发送 PUT_ADDR 注册地址
  //   → 主节点将 key→addr 存入 LSM-Tree
  //   → 返回 STORED 给客户端
  if (cmd == "PUT") {
    // 解析 key 和 value（value 可能包含空格）
    size_t spc = arg.find(' ');
    if (spc == std::string::npos) return "ERROR: PUT requires key and value\r\n";
    std::string key = trim(arg.substr(0, spc));
    std::string value = trim(arg.substr(spc + 1));
    if (key.empty() || value.empty()) return "ERROR: PUT requires key and value\r\n";

    std::cout << "Worker " << worker_id << ": PUT key=\"" << key
              << "\" value=\"" << value << "\"" << std::endl;

    // 步骤1: 写入值到本地 vlog，获取编码地址
    std::string addr = write_value_to_vlog(key, value);
    if (addr.empty()) return "ERROR: Vlog write failed\r\n";

    // 步骤2: 解码地址，提取 vlog_number, offset, size
    leveldb::Slice addr_slice(addr);
    uint64_t vlog_numb, offset, size;
    leveldb::GetVarint64(&addr_slice, &vlog_numb);
    leveldb::GetVarint64(&addr_slice, &offset);
    leveldb::GetVarint64(&addr_slice, &size);

    // 步骤3: 向主节点发送 PUT_ADDR 命令（注册地址映射）
    std::string request = "PUT_ADDR " + key + " " +
                          std::to_string(vlog_numb) + " " +
                          std::to_string(offset) + " " +
                          std::to_string(size);
    std::string response = send_to_master(request);

    // 步骤4: 处理主节点响应
    if (response.find("STORED") != std::string::npos) {
      // ★ 成功后立即持久化 VLOG_META，确保宕机可恢复
      save_vlog_meta();
      return "STORED\r\n";
    } else {
      std::cerr << "Worker " << worker_id << ": 主节点 PUT_ADDR 失败: " << response << std::endl;
      return "ERROR: Master PUT_ADDR failed\r\n";
    }
  }

  // ===== GET：读取键值对 =====
  // 流程: 客户端发送 GET <key>
  //   → 从节点向主节点发送 GET_ADDR 查询地址
  //   → 主节点从 LSM-Tree 返回地址
  //   → 从节点调用 FetchValueFromVlog 从 vlog 读取值
  //   → 返回 VALUE <value> 给客户端
  else if (cmd == "GET") {
    std::string key = arg;
    if (key.empty()) return "ERROR: GET requires a key\r\n";
    std::cout << "Worker " << worker_id << ": GET key=\"" << key << "\"" << std::endl;

    // 步骤1: 向主节点查询 vlog 地址
    std::string response = send_to_master("GET_ADDR " + key);
    // 调试日志：打印原始响应便于排查 NOT_FOUND 问题
    std::cout << "Worker " << worker_id << ": GET_ADDR response=[" << trim(response) << "]" << std::endl;

    // 步骤2: 解析主节点响应
    if (response.find("NOT_FOUND") != std::string::npos) return "NOT_FOUND\r\n";
    if (response.find("ERROR") != std::string::npos) return response;

    // ADDR <vlog_numb> <offset> <size>
    std::string addr_str = trim(response);
    if (addr_str.find("ADDR ") != 0) return "ERROR: Unexpected master response\r\n";
    addr_str = addr_str.substr(5);  // 去掉 "ADDR " 前缀

    std::istringstream addr_iss(addr_str);
    uint64_t vlog_numb, offset, size;
    if (!(addr_iss >> vlog_numb >> offset >> size))
      return "ERROR: Invalid address from master\r\n";

    std::cout << "Worker " << worker_id << ": GET addr vlog=" << vlog_numb
              << " offset=" << offset << " size=" << size << std::endl;

    // 步骤3: 重新编码地址，调用 FetchValueFromVlog 读取值
    std::string encoded_addr;
    leveldb::PutVarint64(&encoded_addr, vlog_numb);
    leveldb::PutVarint64(&encoded_addr, offset);
    leveldb::PutVarint64(&encoded_addr, size);

    // ★ FetchValueFromVlog 内部流程:
    //   1. 解析地址得到 (vlog_number, offset, size)
    //   2. 通过 VlogFetcher 随机读取 vlog 文件
    //   3. 调用 Parse() 剥离 kTypeValue 和 key，返回纯 value
    std::string value;
    pthread_mutex_lock(&g_vlog_mutex);  // 锁：与 GC 线程互斥
    leveldb::Status s = g_vlog_manager->FetchValueFromVlog(encoded_addr, &value);
    pthread_mutex_unlock(&g_vlog_mutex);

    if (!s.ok()) {
      std::cerr << "Worker " << worker_id << ": vlog 读取失败: " << s.ToString() << std::endl;
      return "ERROR: Vlog read failed\r\n";
    }

    std::cout << "Worker " << worker_id << ": GET 返回 value=\"" << value << "\"" << std::endl;
    return "VALUE " + value + "\r\n";
  }

  // ===== DELETE：删除键值对 =====
  // 流程: 客户端发送 DELETE <key>
  //   → 从节点向主节点发送 DELETE_ADDR
  //   → 主节点在 LSM-Tree 中插入删除标记（kTypeDeletion）
  //   → 返回 DELETED 或 NOT_FOUND
  // ★ 注意: vlog 中的旧值不会立即删除，由 GC 线程异步回收
  else if (cmd == "DELETE") {
    std::string key = arg;
    if (key.empty()) return "ERROR: DELETE requires a key\r\n";
    std::cout << "Worker " << worker_id << ": DELETE key=\"" << key << "\"" << std::endl;

    std::string response = send_to_master("DELETE_ADDR " + key);

    if (response.find("DELETED") != std::string::npos) return "DELETED\r\n";
    if (response.find("NOT_FOUND") != std::string::npos) return "NOT_FOUND\r\n";
    return "ERROR: Master DELETE_ADDR failed\r\n";
  }

  // ===== SCAN：范围查询 =====
  // 流程: 客户端发送 SCAN <start_key> <end_key>
  //   → 从节点向主节点发送 SCAN_ADDR 请求
  //   → 主节点使用 NewAddrIterator 遍历 LSM-Tree，返回 (key, addr) 对
  //   → 从节点逐个调用 FetchValueFromVlog 读取值
  //   → 合并结果返回 KVPAIR ... END
  else if (cmd == "SCAN") {
    size_t spc = arg.find(' ');
    if (spc == std::string::npos) return "ERROR: SCAN requires start_key and end_key\r\n";
    std::string start_key = trim(arg.substr(0, spc));
    std::string end_key = trim(arg.substr(spc + 1));
    if (start_key.empty() || end_key.empty()) return "ERROR: SCAN requires start_key and end_key\r\n";
    if (start_key > end_key) return "ERROR: SCAN first_key must be <= last_key\r\n";

    std::cout << "Worker " << worker_id << ": SCAN start=\"" << start_key
              << "\" end=\"" << end_key << "\"" << std::endl;

    // 步骤1: 向主节点查询范围内所有 (key, addr) 对
    std::string response = send_scan_to_master(
        "SCAN_ADDR " + start_key + " " + end_key + " " + std::to_string(worker_id));

    if (response.find("ERROR") != std::string::npos) return response;

    // 步骤2: 逐行解析 KVPAIR_ADDR，读取对应 vlog 值
    std::string result;
    std::istringstream resp_iss(response);
    std::string resp_line;
    int count = 0;

    while (std::getline(resp_iss, resp_line)) {
      if (!resp_line.empty() && resp_line.back() == '\r') resp_line.pop_back();
      resp_line = trim(resp_line);
      if (resp_line.empty()) continue;
      if (resp_line == "END") continue;  // 跳过结束标记

      // 解析 KVPAIR_ADDR <key> <vlog_numb> <offset> <size>
      if (resp_line.find("KVPAIR_ADDR ") == 0) {
        std::string kv = resp_line.substr(12);
        std::istringstream kv_iss(kv);
        std::string scan_key;
        uint64_t vlog_numb, offset, size;
        if (!(kv_iss >> scan_key >> vlog_numb >> offset >> size)) continue;

        // 从 vlog 读取值
        std::string encoded_addr;
        leveldb::PutVarint64(&encoded_addr, vlog_numb);
        leveldb::PutVarint64(&encoded_addr, offset);
        leveldb::PutVarint64(&encoded_addr, size);

        std::string scan_value;
        pthread_mutex_lock(&g_vlog_mutex);
        leveldb::Status s = g_vlog_manager->FetchValueFromVlog(encoded_addr, &scan_value);
        pthread_mutex_unlock(&g_vlog_mutex);

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

  // ===== GC：手动触发垃圾回收 =====
  // 与自动 GC 不同：客户端 GC 仅处理最旧的一个非活跃 vlog 文件。
  // 若从节点只有一个 vlog 文件（必定为当前活跃的热文件，不可 GC），返回 GC DENY。
  else if (cmd == "GC") {
    std::cout << "Worker " << worker_id << ": 收到手动 GC 请求" << std::endl;

    auto vlog_files = scan_vlog_files(g_dbname);
    if (vlog_files.empty()) return "GC OK (no vlog files)\r\n";

    // ★ 收集非活跃 vlog 文件（跳过当前活跃 vlog g_vlog_number）
    std::vector<uint64_t> all_targets;
    for (uint64_t num : vlog_files) {
      if (num >= g_vlog_number) continue;
      all_targets.push_back(num);
    }

    // ★ 若只有一个 vlog 文件（必为活跃热文件），拒绝 GC
    if (all_targets.empty()) {
      std::cout << "Worker " << worker_id << ": 手动 GC 拒绝 — 只有活跃 vlog 文件" << std::endl;
      return "GC DENY\r\n";
    }

    // ★ 只取最旧的一个非活跃 vlog 文件
    std::vector<uint64_t> single_target = { all_targets.front() };
    uint64_t file_size = get_file_size(leveldb::LogFileName(g_dbname, single_target[0]));

    std::cout << "Worker " << worker_id << ": 手动 GC 开始! 目标文件=" << single_target[0]
              << ", 大小=" << (file_size >> 20) << "MB" << std::endl;

    // ★ check_stop=false：不检查退出标志，确保客户端 GC 操作完成
    int removed = execute_gc_on_files(single_target, false);

    std::cout << "Worker " << worker_id << ": 手动 GC 完成! 删除文件数=" << removed << std::endl;
    save_vlog_meta();
    return "GC OK\r\n";
  }

  else {
    std::cout << "Worker " << worker_id << ": 未知命令: " << cmd << std::endl;
    return "ERROR unknown command\r\n";
  }
}

// ==================== Vlog 初始化与恢复 ====================

// 扫描数据库目录，获取所有 .log 文件的编号（升序排列）
static std::vector<uint64_t> scan_vlog_files(const std::string& dbname) {
  std::vector<uint64_t> numbers;
  DIR* dir = opendir(dbname.c_str());
  if (!dir) return numbers;

  struct dirent* entry;
  while ((entry = readdir(dir)) != nullptr) {
    std::string name(entry->d_name);
    uint64_t number;
    leveldb::FileType type;
    // ★ 使用 LevelDB 的文件名解析函数，识别 .log 后缀
    if (leveldb::ParseFileName(name, &number, &type) && type == leveldb::kLogFile) {
      numbers.push_back(number);
    }
  }
  closedir(dir);
  std::sort(numbers.begin(), numbers.end());
  return numbers;
}

// 获取文件物理大小（通过 stat 系统调用）
static uint64_t get_file_size(const std::string& path) {
  struct stat st;
  if (stat(path.c_str(), &st) == 0) return static_cast<uint64_t>(st.st_size);
  return 0;
}

// 初始化或恢复 vlog 状态
//
// ★ 恢复策略:
//   1. 扫描已有 .log 文件
//   2. 尝试读取 VLOG_META 获得精确的 vlog_number 和 write_pos
//   3. 若有 VLOG_META: 按元数据恢复
//   4. 若无 VLOG_META 但有 .log 文件: 从最大编号文件末尾恢复
//   5. 若全新启动: 创建 vlog_number=1
bool init_vlog(const std::string& dbname) {
  g_dbname = dbname;
  leveldb::Env::Default()->CreateDir(dbname);  // 确保目录存在

  // 设置 vlog 参数
  g_options.env = leveldb::Env::Default();
  g_options.max_vlog_size = VlogSize;

  g_vlog_manager = new leveldb::vlog::VlogManager(g_options.clean_threshold);

  // 扫描已有 vlog 文件
  auto existing = scan_vlog_files(dbname);

  // 尝试从 VLOG_META 恢复
  std::string meta_path = vlog_meta_path();
  bool meta_loaded = false;
  uint64_t saved_vlog = 0, saved_pos = 0;

  std::ifstream ifs(meta_path);
  if (ifs.is_open()) {
    std::string line;
    while (std::getline(ifs, line)) {
      if (line.find("vlog_number:") == 0)
        saved_vlog = std::stoull(line.substr(13));
      else if (line.find("write_pos:") == 0)
        saved_pos = std::stoull(line.substr(11));
      else if (line.find("gc_threshold:") == 0)
        g_gc_threshold = std::stoi(line.substr(14));
      else if (line.find("gc_consecutive:") == 0)
        g_gc_consecutive_no_delete = std::stoi(line.substr(16));
    }
    ifs.close();
    if (saved_vlog > 0) meta_loaded = true;
  }
  // 确保阈值不低于初始值（防御旧版 VLOG_META 或损坏数据）
  if (g_gc_threshold < kGcVlogFileCountThreshold)
    g_gc_threshold = kGcVlogFileCountThreshold;

  if (!existing.empty() && meta_loaded) {
    // ★ 情况1: 有文件且有 VLOG_META → 精确恢复
    std::cout << "Worker " << worker_id << ": 检测到 " << existing.size()
              << " 个已有 vlog 文件，从 VLOG_META 恢复..." << std::endl;
    for (uint64_t num : existing) {
      g_vlog_manager->AddVlog(dbname, g_options, num);  // 注册所有 vlog 文件
    }
    g_vlog_number = saved_vlog;
    if (saved_pos > 0) {
      g_vlog_manager->SetCurrentVlog(saved_vlog);
      g_vlog_manager->SetHead(saved_pos);  // 恢复写入位置
    }
    std::cout << "Worker " << worker_id << ": Vlog 恢复完成, 当前 vlog="
              << g_vlog_number << ", write_pos=" << saved_pos << std::endl;
  } else if (!existing.empty()) {
    // ★ 情况2: 有文件但无 VLOG_META → 从最大编号文件恢复
    uint64_t max_num = existing.back();
    std::cout << "Worker " << worker_id << ": VLOG_META 不存在，从已有文件恢复..." << std::endl;
    for (uint64_t num : existing) {
      g_vlog_manager->AddVlog(dbname, g_options, num);
    }
    g_vlog_number = max_num;
    g_vlog_manager->SetCurrentVlog(max_num);
    uint64_t fsize = get_file_size(leveldb::LogFileName(dbname, max_num));
    g_vlog_manager->SetHead(fsize);  // 假设文件末尾为写入位置
    std::cout << "Worker " << worker_id << ": Vlog 恢复完成, 当前 vlog="
              << g_vlog_number << ", head~=" << fsize << std::endl;
  } else {
    // ★ 情况3: 全新初始化
    std::cout << "Worker " << worker_id << ": 全新 Vlog 初始化" << std::endl;
    g_vlog_number = 1;
    g_vlog_manager->AddVlog(dbname, g_options, g_vlog_number);
  }

  std::cout << "Worker " << worker_id << ": Vlog 已就绪, vlog_number="
            << g_vlog_number << ", 路径: " << dbname << std::endl;
  return true;
}

// 安全关闭 vlog：保存 VLOG_META 并释放 VlogManager
void shutdown_vlog() {
  if (g_vlog_manager) {
    save_vlog_meta();
    delete g_vlog_manager;
    g_vlog_manager = nullptr;
  }
}

// ==================== GC 核心逻辑 ====================

// 对指定的 vlog 文件列表逐文件执行 GC：读取 → 验证 → 搬迁有效值 → 删除全垃圾文件
//
// ★ GC 验证规则（逐记录）:
//   1. 向主节点发送 GCGET_ADDR <key> 查询当前地址
//   2. 若 key 不存在（NOT_FOUND）→ 已被主节点 Compaction 回收 → 无效
//   3. 若 key 存在但 (vlog_number, offset, size) 与当前记录不匹配
//      → 被覆盖写或 GC 搬迁 → 无效
//   4. 若完全匹配 → 有效，搬迁到当前 vlog 头部
//
//   ★ VReader 不暴露记录偏移，此处通过累加 (kVHeaderSize + record.size())
//     手动跟踪每条记录在旧 vlog 中的位置，用于与主节点地址做三字段精确比对。
//
// 参数:
//   targets     — 目标 vlog 文件编号列表（旧→新，不应包含当前活跃 vlog）
//   check_stop  — 如果为 true，循环中检查 g_running && g_gc_running（后台 GC 用）
//                 如果为 false，不检查退出标志（客户端手动 GC 用，保证操作完成）
//
// 返回值: 成功删除的 vlog 文件数
static int execute_gc_on_files(const std::vector<uint64_t>& targets, bool check_stop) {
  int total_removed = 0;
  for (uint64_t oldest_vlog : targets) {
    if (check_stop && (!g_running || !g_gc_running)) break;
    if (g_master_fd < 0) break;

    std::cout << "Worker " << worker_id << ":   GC 文件 " << oldest_vlog
              << " (" << leveldb::LogFileName(g_dbname, oldest_vlog) << ")"
              << std::endl;

    std::string old_vlog_path = leveldb::LogFileName(g_dbname, oldest_vlog);
    leveldb::SequentialFile* seq_file = nullptr;
    leveldb::Status s = g_options.env->NewSequentialFile(old_vlog_path, &seq_file);
    if (!s.ok()) {
      std::cerr << "Worker " << worker_id << ": GC 无法打开 vlog 文件 "
                << old_vlog_path << ": " << s.ToString() << std::endl;
      continue;
    }

    // ★ VReader(initial_offset=0) 从文件头部开始顺序读取
    leveldb::vlog::VReader reader(seq_file, nullptr, true, 0);
    std::string scratch;
    leveldb::Slice record;
    int total_records = 0, valid_records = 0;
    uint64_t rec_offset = 0;  // 当前记录在文件中的偏移（字节），手动跟踪

    while ((check_stop ? (g_running && g_gc_running) : true) &&
           reader.ReadRecord(&record, &scratch)) {
      total_records++;
      std::string raw_data(record.data(), record.size());

      // 记录本条 vlog 记录的精确偏移和大小
      uint64_t this_offset = rec_offset + kVHeaderSize;  // 数据部分偏移（跳过 header）
      uint64_t this_size = raw_data.size();

      // 下一条记录的文件偏移 = 当前偏移 + 头部 + 数据体
      rec_offset += kVHeaderSize + raw_data.size();

      // 从 vlog 记录中提取 key
      std::string entry_key;
      if (!parse_vlog_key_value(raw_data, &entry_key, nullptr)) continue;

      // 向主节点查询该 key 的当前地址
      std::string addr_resp = send_to_master("GCGET_ADDR " + entry_key);
      // *情况一：若不存在key，已被主节点 Compaction 回收 → 无效
      if (addr_resp.find("ADDR ") != 0) continue;

      // 解析当前地址
      std::string addr_str = trim(addr_resp).substr(5);
      std::istringstream addr_iss(addr_str);
      uint64_t cur_vlog, cur_offset, cur_size;
      if (!(addr_iss >> cur_vlog >> cur_offset >> cur_size)) continue;

      // ★ 三字段精确比对：vlog_number, offset, size 必须完全一致才有效
      //   仅比对 vlog_number 会导致同文件内的旧版本被误判为有效
      if (cur_vlog != oldest_vlog ||
          cur_offset != this_offset ||
          cur_size != this_size) {
        // *情况二：被覆盖写、已删除、或 GC 已搬迁 → 无效
        continue;
      }

      // *情况三：地址完全匹配，该记录仍然有效，需要搬迁
      std::string entry_value;
      if (!parse_vlog_key_value(raw_data, nullptr, &entry_value)) continue;

      // 重写到当前 vlog（write_value_to_vlog 内部加锁）
      std::string new_addr = write_value_to_vlog(entry_key, entry_value);
      if (new_addr.empty()) {
        std::cerr << "Worker " << worker_id << ": GC 重写失败 key=" << entry_key << std::endl;
        continue;
      }

      // 解码新地址
      leveldb::Slice na(new_addr);
      uint64_t new_vlog, new_offset, new_size;
      leveldb::GetVarint64(&na, &new_vlog);
      leveldb::GetVarint64(&na, &new_offset);
      leveldb::GetVarint64(&na, &new_size);

      // ★ 通知主节点更新地址（CAS 语义：仅当旧地址完全匹配时才更新）
      std::string update_req = "UPDATE_ADDR " + entry_key + " " +
                               std::to_string(oldest_vlog) + " " +
                               std::to_string(this_offset) + " " +
                               std::to_string(this_size) + " " +
                               std::to_string(new_vlog) + " " +
                               std::to_string(new_offset) + " " +
                               std::to_string(new_size);
      std::string update_resp = send_to_master(update_req);
      if (update_resp.find("UPDATED") == std::string::npos) {
        std::cerr << "Worker " << worker_id << ": GC UPDATE_ADDR 失败 key="
                  << entry_key << ": " << update_resp << std::endl;
      } else {
        valid_records++;
      }
    }

    std::cout << "Worker " << worker_id << ":   GC 文件 " << oldest_vlog
              << " 完成! 总记录=" << total_records
              << ", 有效搬迁=" << valid_records << std::endl;

    // ★ 如果所有记录已被扫描完成，安全删除旧 vlog 文件
    if (valid_records == 0 && total_records > 0) {
      std::cout << "Worker " << worker_id << ":   GC 删除 vlog: " << old_vlog_path << std::endl;
      g_options.env->RemoveFile(old_vlog_path);
      total_removed++;
    }
  }
  return total_removed;
}

// 收集非活跃 vlog 文件编号（旧→新排序，跳过当前活跃 vlog）
static std::vector<uint64_t> collect_gc_targets(const std::vector<uint64_t>& vlog_files,
                                                 int max_count) {
  std::vector<uint64_t> targets;
  for (uint64_t num : vlog_files) {
    if (num >= g_vlog_number) continue;  // 跳过当前活跃 vlog（热文件不能 GC）
    targets.push_back(num);
  }
  if (static_cast<int>(targets.size()) > max_count && max_count > 0) {
    targets.resize(max_count);
  }
  return targets;
}

// GC 线程主函数
//
// ★ GC 策略（批量文件 GC + 动态阈值）:
//   1. 每 kGcIntervalSec 秒检查一次
//   2. 触发条件: 非活跃 vlog 文件数 >= g_gc_threshold（动态，初始 = kGcVlogFileCountThreshold）
//   3. 每次取最旧的 (threshold/3) 个非活跃 vlog 文件，依次执行搬迁
//   4. 动态阈值防风暴: 连续无效 GC 次数 > threshold/3+1 → 阈值+1
//   ★ 若 g_gc_threshold 升高，下次启动后从 VLOG_META 恢复，不会丢失
//
// ★ 并发安全:
//   - g_vlog_mutex: 保护 vlog 读写（与客户端请求互斥）
//   - g_master_mutex: 保护主节点连接（send_to_master 内部加锁）
//   - GC 与客户端请求共用 g_master_fd，不需要独立连接
static void* gc_thread_main(void*) {
  g_gc_running = true;
  std::cout << "Worker " << worker_id << ": GC 后台线程已启动 (共用 g_master_fd="
            << g_master_fd << ")" << std::endl;

  while (g_running && g_gc_running) {
    for (int s = 0; s < kGcIntervalSec && g_running && g_gc_running; ++s) {
      sleep(1);
    }
    if (!g_running || !g_gc_running) break;
    if (g_master_fd < 0) { usleep(1000000); continue; }

    auto vlog_files = scan_vlog_files(g_dbname);
    if (vlog_files.empty()) continue;

    std::vector<uint64_t> gc_targets = collect_gc_targets(vlog_files, -1);

    // ★ 动态阈值判断：若文件数低于当前阈值，无需 GC，重置计数器
    if (static_cast<int>(gc_targets.size()) < g_gc_threshold) {
      g_gc_consecutive_no_delete = 0;
      continue;
    }

    // ★ GC 批次大小基于当前动态阈值计算
    int gc_batch = g_gc_threshold / 3;
    if (gc_batch < 1) gc_batch = 1;
    if (static_cast<int>(gc_targets.size()) > gc_batch) gc_targets.resize(gc_batch);

    uint64_t batch_size = 0;
    for (uint64_t num : gc_targets)
      batch_size += get_file_size(leveldb::LogFileName(g_dbname, num));

    std::cout << "Worker " << worker_id << ": GC 触发! vlog文件总数=" << vlog_files.size()
              << ", 非活跃数=" << gc_targets.size()
              << ", 当前GC阈值=" << g_gc_threshold
              << ", 本次GC目标数=" << gc_targets.size()
              << ", 目标总大小=" << (batch_size >> 20) << "MB" << std::endl;

    int removed = execute_gc_on_files(gc_targets, true);

    std::cout << "Worker " << worker_id << ": GC 本轮完成! 处理文件数=" << gc_targets.size()
              << ", 删除文件数=" << removed << std::endl;

    // ★ 动态阈值调整算法（防止 GC 风暴）:
    //   若本轮 GC 删除了 ≥1 个旧 vlog 文件 → GC 有效，重置计数器
    //   若本轮 GC 未删除任何文件（搬迁了有效数据但文件数未减）→ 计数器+1
    //   当连续无效 GC 次数 > g_gc_threshold/3 + 1 → 阈值+1，计数器重置
    if (removed > 0) {
      g_gc_consecutive_no_delete = 0;
    } else {
      g_gc_consecutive_no_delete++;
      int escalation_limit = g_gc_threshold / 3 + 1;
      if (g_gc_consecutive_no_delete > escalation_limit) {
        int old_threshold = g_gc_threshold;
        g_gc_threshold++;
        g_gc_consecutive_no_delete = 0;
        std::cout << "Worker " << worker_id << ": GC 阈值自动提升! "
                  << old_threshold << " → " << g_gc_threshold
                  << " (连续 " << escalation_limit << " 次无效 GC)"
                  << std::endl;
      }
    }

    save_vlog_meta();
  }

  g_gc_running = false;
  std::cout << "Worker " << worker_id << ": GC 后台线程已退出" << std::endl;
  return nullptr;
}

// ==================== 主函数 ====================

int main(int argc, char* argv[]) {
  // 解析命令行参数: ./slave <worker_id>
  if (argc < 2) {
    std::cerr << "用法: " << argv[0] << " <worker_id (0-2)>" << std::endl;
    return 1;
  }
  worker_id = std::atoi(argv[1]);
  if (worker_id < 0 || worker_id > 2) {
    std::cerr << "错误: worker_id 必须在 0~2 之间" << std::endl;
    return 1;
  }
  // ★ 计算主节点端口: Worker i 连接 8889+i
  g_master_port = 8889 + worker_id;

  register_signal_handler();

  // ===== 阶段0: 初始化 Vlog =====
  std::string db_path = "./diskv_slave_vlog_" + std::to_string(worker_id);
  if (!init_vlog(db_path)) {
    std::cerr << "Worker " << worker_id << ": Vlog 初始化失败" << std::endl;
    return 1;
  }

  // ===== 阶段1: 连接到主节点 =====
  std::cout << "Worker " << worker_id << ": 正在连接主节点 " << MASTER_HOST
            << ":" << g_master_port << " ..." << std::endl;
  g_master_fd = connect_to_master();
  if (g_master_fd < 0 || !g_running) {
    shutdown_vlog(); return 1;
  }
  std::cout << "Worker " << worker_id << ": 已连接到主节点 (port "
            << g_master_port << "), fd=" << g_master_fd << std::endl;

  // ===== 阶段1b: 启动 GC 后台线程 =====
  if (pthread_create(&g_gc_thread, nullptr, gc_thread_main, nullptr) != 0) {
    std::cerr << "Worker " << worker_id << ": 创建 GC 线程失败" << std::endl;
  }

  // ===== 阶段2: 创建客户端监听套接字 =====
  int slave_listen_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (slave_listen_fd < 0) {
    std::cerr << "Worker " << worker_id << ": 创建监听套接字失败" << std::endl;
    g_running = false;
    if (g_gc_thread) pthread_join(g_gc_thread, nullptr); // pthread_join()用于创建新线程，每个客户端线程之前并发。
    close(g_master_fd); shutdown_vlog(); return 1;
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
    g_running = false; if (g_gc_thread) pthread_join(g_gc_thread, nullptr);
    close(slave_listen_fd); close(g_master_fd); shutdown_vlog(); return 1;
  }
  if (listen(slave_listen_fd, 128) < 0) {
    std::cerr << "Worker " << worker_id << ": 监听失败" << std::endl;
    g_running = false; if (g_gc_thread) pthread_join(g_gc_thread, nullptr);
    close(slave_listen_fd); close(g_master_fd); shutdown_vlog(); return 1;
  }
  std::cout << "Worker " << worker_id << ": 客户端监听已建立 [127.0.0.1:"
            << SLAVE_CLIENT_PORTS[worker_id] << "]" << std::endl;

  // ===== 阶段3: 创建 epoll 实例并注册 fd =====
  int epoll_fd = epoll_create1(0);
  if (epoll_fd < 0) {
    std::cerr << "Worker " << worker_id << ": 创建 epoll 失败" << std::endl;
    g_running = false; if (g_gc_thread) pthread_join(g_gc_thread, nullptr);
    close(slave_listen_fd); close(g_master_fd); shutdown_vlog(); return 1;
  }

  struct epoll_event ev;

  // 客户端监听 → LT 模式
  ev.events = EPOLLIN;
  ev.data.fd = slave_listen_fd;
  if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, slave_listen_fd, &ev) < 0) {
    std::cerr << "Worker " << worker_id << ": 添加监听 fd 到 epoll 失败" << std::endl;
    g_running = false; if (g_gc_thread) pthread_join(g_gc_thread, nullptr);
    close(epoll_fd); close(slave_listen_fd); close(g_master_fd); shutdown_vlog(); return 1;
  }

  // 主节点连接 → ET 模式（仅检测断开/错误，不监听 EPOLLIN）
  //  *注：不注册 EPOLLIN！因为 GC 线程和客户端请求通过 send_to_master()
  //   在 g_master_fd 上做阻塞 read() 等待主节点响应。如果 epoll 也监听 EPOLLIN，
  //   事件循环的 read() 会与 GC/客户端线程的阻塞 read() 产生竞态——事件循环
  //   抢走响应数据后丢弃，导致另一端永久阻塞，进而死锁整个系统。
  //   仅使用 EPOLLRDHUP + EPOLLERR + EPOLLHUP 检测连接断开。
  set_nonblocking(g_master_fd);
  ev.events = EPOLLRDHUP | EPOLLERR | EPOLLHUP | EPOLLET;
  ev.data.fd = g_master_fd;
  if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, g_master_fd, &ev) < 0) {
    std::cerr << "Worker " << worker_id << ": 添加 master_fd 到 epoll 失败" << std::endl;
  }

  // ===== 阶段4: 主事件循环 =====
  struct epoll_event events[MAX_EVENTS];
  while (g_running) {
    int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, 1000);
    if (nfds < 0) {
      if (!g_running) break;
      if (errno == EINTR) continue;
      std::cerr << "Worker " << worker_id << ": epoll_wait 错误" << std::endl; break;
    }

    for (int i = 0; i < nfds; ++i) {
      int fd = events[i].data.fd;

      // —— 主节点事件（仅检测断开） ——
      // ★ g_master_fd 不监听 EPOLLIN，因此无需 read() 丢弃数据。
      //   所有对主节点的读写均由 send_to_master() / send_to_master_via_fd()
      //   通过 g_master_mutex 序列化访问，不存在竞态。
      if (fd == g_master_fd) {
        if (events[i].events & (EPOLLRDHUP | EPOLLERR | EPOLLHUP)) {
          std::cout << "Worker " << worker_id << ": 主节点断开连接" << std::endl;
          epoll_ctl(epoll_fd, EPOLL_CTL_DEL, g_master_fd, nullptr);
          close(g_master_fd); g_master_fd = -1;
          // 尝试重连
          std::cout << "Worker " << worker_id << ": 尝试重新连接..." << std::endl;
          g_master_fd = connect_to_master();
          if (g_master_fd >= 0) {
            set_nonblocking(g_master_fd);
            ev.events = EPOLLRDHUP | EPOLLERR | EPOLLHUP | EPOLLET;
            ev.data.fd = g_master_fd;
            epoll_ctl(epoll_fd, EPOLL_CTL_ADD, g_master_fd, &ev);
          }
        }
        continue;
      }

      // —— 新客户端连接 ——
      if (fd == slave_listen_fd) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(slave_listen_fd, (struct sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) {
          if (errno == EAGAIN || errno == EWOULDBLOCK) continue;
          std::cerr << "Worker " << worker_id << ": accept 失败" << std::endl; continue;
        }
        set_nonblocking(client_fd);
        connections[client_fd] = Connection();  // 初始化缓冲区

        ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
        ev.data.fd = client_fd;
        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev) < 0) {
          std::cerr << "Worker " << worker_id << ": 添加客户端 fd 到 epoll 失败" << std::endl;
          close(client_fd); continue;
        }
        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
        int client_port = ntohs(client_addr.sin_port);
        std::cout << "Worker " << worker_id << ": 新客户端 fd=" << client_fd
                  << " socket=" << client_ip << ":" << client_port << std::endl;
        continue;
      }

      // —— 客户端数据 IO ——
      bool closed = false;
      auto conn_it = connections.find(fd);
      if (conn_it == connections.end()) { close(fd); continue; }
      Connection& conn = conn_it->second;

      if (events[i].events & (EPOLLERR | EPOLLHUP)) {
        std::cerr << "Worker " << worker_id << ": 客户端 fd=" << fd << " 异常" << std::endl;
        closed = true;
      }

      // 可写：发送 send_buffer
      if (!closed && (events[i].events & EPOLLOUT)) {
        while (!conn.send_buffer.empty()) {
          int bw = write(fd, conn.send_buffer.data(), conn.send_buffer.size());
          if (bw < 0) { if (errno == EAGAIN || errno == EWOULDBLOCK) break; closed = true; break; }
          else if (bw == 0) continue;
          else conn.send_buffer.erase(0, bw);
        }
        if (conn.send_buffer.empty()) {
          ev.events = EPOLLIN | EPOLLET; ev.data.fd = fd;
          epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev);
        }
      }

      // 可读：读取数据 → 解析命令 → 生成响应
      if (!closed && (events[i].events & EPOLLIN)) {
        while (true) {  // ET 模式循环 read
          char temp_buffer[256] = {0};
          int br = read(fd, temp_buffer, sizeof(temp_buffer) - 1);
          if (br > 0) {
            conn.recv_buffer.append(temp_buffer, br);
            // 按 \n 分割命令
            size_t pos;
            while ((pos = conn.recv_buffer.find('\n')) != std::string::npos) {
              std::string line = conn.recv_buffer.substr(0, pos);
              conn.recv_buffer.erase(0, pos + 1);
              std::string response = process_command(line);
              if (!response.empty()) conn.send_buffer += response;
            }
          } else if (br == 0) { closed = true; break; }
          else { if (errno == EAGAIN || errno == EWOULDBLOCK) break; closed = true; break; }
        }
        if (!closed && !conn.send_buffer.empty()) {
          ev.events = EPOLLIN | EPOLLOUT | EPOLLET; ev.data.fd = fd;
          epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev);
        }
      }

      if (closed) {
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
        close(fd); connections.erase(fd);
      }
    }
  }

  // ===== 阶段5: 安全关闭 =====
  std::cout << "Worker " << worker_id << ": 正在关闭..." << std::endl;

  // 停止 GC 线程（等待最多 1 秒）
  g_gc_running = false;
  if (g_gc_thread) { pthread_join(g_gc_thread, nullptr); g_gc_thread = 0; }

  close(epoll_fd);
  close(slave_listen_fd);
  if (g_master_fd >= 0) close(g_master_fd);
  for (auto& pair : connections) close(pair.first);
  connections.clear();

  shutdown_vlog();
  std::cout << "Worker " << worker_id << ": 已安全退出" << std::endl;
  return 0;
}
