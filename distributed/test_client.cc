// =============================================================================
// test_client.cc - DisKV 自动测试客户端（批量基准测试）
// =============================================================================
//
// 【架构定位】基于 client.cc 的同架构自动测试客户端。
//   1. 启动后等待用户输入"命令"行（参数格式类似 db_bench）
//   2. 自动发送 HELLO 获取路由表
//   3. 根据 --benchmarks 指定的操作项目批量执行 PUT/GET/DELETE/SCAN
//   4. 自动发送 QUIT 注销，回到等待命令状态
//   5. Ctrl+C 或 kill 退出进程
//
// 【支持的命令参数】
//   --benchmarks=<list>    操作项目（逗号分隔）:
//     fillseq       — 顺序写入 N 条记录
//     fillrandom    — 随机顺序写入 N 条记录
//     overwrite     — 随机覆盖写入 N 条记录
//     deleteseq     — 顺序删除 N 条记录
//     deleterandom  — 随机删除 N 条记录
//     readseq       — 顺序读取 N 条记录
//     readreverse   — 逆序读取 N 条记录
//     readrandom    — 随机读取 N 条记录
//     readmissing   — 随机读取 N 条不存在的键
//     readhot       — 随机读取 DB 前 1% 热点数据
//     scanall       — 全库范围扫描（不受 --num 限制）
//   --num=<N>             单次操作的记录数  （scanall 除外）
//   --key_size=<N>        键的字节大小     （默认 16）
//   --value_size=<N>      值的字节大小     （默认 100）
//   --histogram=<0|1>     是否打印耗时直方图（默认 0）
//   --use_existing_db=<0|1> 是否使用已有数据库。若为 0，先发送 DELETE 清空所有键（默认 1）
//
// 【直方图指标】
//   每条操作的耗时 = 从客户端发出命令开始到收到完整反馈结束
//   输出包括: 操作数, 平均耗时(微秒), 标准差, 最小/中位数/最大值, 分桶直方图
// =============================================================================

#include <arpa/inet.h>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <csignal>
#include <errno.h>
#include <iostream>
#include <netinet/in.h>
#include <sstream>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

#include "util/histogram.h"

// ==================== 常量与默认值 ====================

const std::string DEFAULT_HOST = "127.0.0.1";
const int DEFAULT_PORT = 8888;
const int WORKER_COUNT = 3;
const int SLAVE_CLIENT_PORTS[3] = {9000, 9001, 9002};

// 默认键大小和值大小（与 db_bench 一致）
static int FLAGS_key_size = 16;
static int FLAGS_value_size = 100;
static int64_t FLAGS_num = 1000000;
static bool FLAGS_histogram = false;
static bool FLAGS_use_existing_db = true;

// ==================== 数据结构 ====================

struct RouteEntry {
  int index;
  std::string ip;
  int port;
  int range_start;
  int range_end;
};

std::vector<RouteEntry> route_table;
std::unordered_map<int, int> slave_fds;

// ==================== 工具函数 ====================

static std::string trim(const std::string& s) {
  auto start = s.find_first_not_of(" \t\r\n");
  auto end = s.find_last_not_of(" \t\r\n");
  if (start == std::string::npos) return "";
  return s.substr(start, end - start + 1);
}

int find_worker_for_key(const std::string& key) {
  if (key.empty() || route_table.empty()) return -1;
  unsigned char ch = static_cast<unsigned char>(key[0]);
  for (const auto& entry : route_table) {
    if (ch >= entry.range_start && ch <= entry.range_end) return entry.index;
  }
  return -1;
}

int get_slave_fd(int worker_index) {
  auto it = slave_fds.find(worker_index);
  if (it != slave_fds.end() && it->second >= 0) return it->second;
  if (worker_index < 0 || worker_index >= (int)route_table.size()) return -1;
  const RouteEntry& entry = route_table[worker_index];

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
  slave_fds[worker_index] = sock;
  return sock;
}

// ==================== 网络 IO ====================

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

std::string read_slave_response(int fd) {
  std::string response;
  char buf[4096];
  while (true) {
    ssize_t n = read(fd, buf, sizeof(buf) - 1);
    if (n < 0) { if (errno == EINTR) continue; return ""; }
    else if (n == 0) return "";
    buf[n] = '\0'; response += buf;
    if (response.find("\r\n") != std::string::npos ||
        response.find("\n") != std::string::npos) break;
  }
  return response;
}

// ==================== HELLO / QUIT ====================

bool do_hello(const std::string& host, int port) {
  int fd = connect_to_master(host, port);
  if (fd < 0) { std::cerr << "HELLO 连接失败" << std::endl; return false; }
  std::string req = "HELLO\n";
  write(fd, req.c_str(), req.size());

  std::string response;
  char buf[4096];
  while (true) {
    ssize_t n = read(fd, buf, sizeof(buf) - 1);
    if (n < 0) { if (errno == EINTR) continue; break; }
    else if (n == 0) break;
    buf[n] = '\0'; response += buf;
    if (response.find("END\r\n") != std::string::npos ||
        response.find("END\n") != std::string::npos) break;
  }
  close(fd);

  // 解析路由表
  route_table.clear();
  std::istringstream iss(response);
  std::string line;
  bool in_table = false;
  while (std::getline(iss, line)) {
    if (!line.empty() && line.back() == '\r') line.pop_back();
    line = trim(line);
    if (line == "ROUTETABLE") { in_table = true; continue; }
    if (line.find("INDEX") == 0) continue;
    if (line == "END") break;
    if (!in_table) continue;
    std::istringstream lis(line);
    int idx; std::string ip_port, range_str;
    if (!(lis >> idx >> ip_port >> range_str)) continue;
    size_t cp = ip_port.find(':');
    if (cp == std::string::npos) continue;
    size_t dp = range_str.find('-');
    if (dp == std::string::npos) continue;
    RouteEntry e;
    e.index = idx;
    e.ip = ip_port.substr(0, cp);
    e.port = std::stoi(ip_port.substr(cp + 1));
    e.range_start = std::stoi(range_str.substr(0, dp));
    e.range_end = std::stoi(range_str.substr(dp + 1));
    route_table.push_back(e);
  }
  return !route_table.empty();
}

void do_quit(const std::string& host, int port) {
  int fd = connect_to_master(host, port);
  if (fd < 0) return;
  std::string req = "QUIT\n";
  write(fd, req.c_str(), req.size());
  char buf[64] = {0};
  read(fd, buf, sizeof(buf) - 1);
  close(fd);
}

// ==================== 简易随机数生成器（兼容 LevelDB 的 Random） ====================

class Random {
 public:
  explicit Random(uint32_t s) : seed_(s & 0x7fffffffU) {
    if (seed_ == 0 || seed_ == 2147483647L) seed_ = 1;
  }
  uint32_t Next() {
    static const uint32_t M = 2147483647L;
    static const uint64_t A = 16807;
    uint64_t product = seed_ * A;
    seed_ = static_cast<uint32_t>((product >> 31) + (product & M));
    if (seed_ > M) seed_ -= M;
    return seed_;
  }
  uint32_t Uniform(int n) { return Next() % n; }
 private:
  uint32_t seed_;
};

// ==================== 键与值生成 ====================

// 生成 ASCII 范围 [32, 126] 内的可打印单字符键（用于 fillseq: a, b, c, ...）
// 索引 N 映射到 ASCII 字符
static std::string seq_key(int64_t n) {
  // 将索引映射到可打印 ASCII 范围: 32(' ') ~ 126('~')
  // 但为了简单，使用 'a' + n 方式（0→'a', 1→'b', ...）
  // 超过 26 个后用多字符键
  if (n < 26) {
    char c = static_cast<char>('a' + (n % 26));
    return std::string(1, c);
  }
  // 多字符键: 前缀 'a'..'z' + 后缀数字
  char prefix = static_cast<char>('a' + (n % 26));
  return prefix + std::to_string(n / 26);
}

// 生成随机键（可打印 ASCII 字符组成，长度由 FLAGS_key_size 决定）
static std::string random_key(class Random* rnd) {
  // 键由首字符（控制分区）+ 随机补齐组成
  // 首字符在可打印 ASCII [32,126] 范围内随机
  std::string k;
  k.resize(FLAGS_key_size);
  for (int i = 0; i < FLAGS_key_size; ++i) {
    k[i] = static_cast<char>(32 + (rnd->Next() % 95));  // 32~126
  }
  return k;
}

// ==================== 耗时记录宏 ====================

// 记录操作耗时，返回响应字符串
#define TIME_OP(op_expr, hist) do {                         \
    auto _start = std::chrono::high_resolution_clock::now(); \
    op_expr;                                                 \
    auto _end = std::chrono::high_resolution_clock::now();   \
    double _us = std::chrono::duration<double, std::micro>(  \
        _end - _start).count();                              \
    if (hist) (hist)->Add(_us);                              \
  } while (0)

// ==================== 测试操作实现 ====================

// 生成指定长度的随机值字符串
static std::string random_value(class Random* rnd, int len) {
  std::string v;
  v.resize(len);
  for (int i = 0; i < len; ++i)
    v[i] = static_cast<char>(32 + (rnd->Next() % 95));
  return v;
}

// 发送 PUT 并等待响应
static bool do_put(int slave_fd, const std::string& key, const std::string& value) {
  std::string req = "PUT " + key + " " + value + "\n";
  if (write(slave_fd, req.c_str(), req.size()) < 0) return false;
  std::string resp = read_slave_response(slave_fd);
  return resp.find("STORED") != std::string::npos;
}

// 发送 GET 并等待响应
static bool do_get(int slave_fd, const std::string& key, std::string* value) {
  std::string req = "GET " + key + "\n";
  if (write(slave_fd, req.c_str(), req.size()) < 0) return false;
  std::string resp = read_slave_response(slave_fd);
  if (resp.find("VALUE ") == 0) {
    if (value) *value = trim(resp.substr(6));
    return true;
  }
  return false;
}

// 发送 DELETE 并等待响应
static bool do_delete(int slave_fd, const std::string& key) {
  std::string req = "DELETE " + key + "\n";
  if (write(slave_fd, req.c_str(), req.size()) < 0) return false;
  std::string resp = read_slave_response(slave_fd);
  return resp.find("DELETED") != std::string::npos;
}

// ==================== 操作执行 ====================

// 顺序写入: key = seq_key(i), value = random
void bench_fillseq(leveldb::Histogram* hist, int64_t num, class Random* rnd) {
  std::cout << "fillseq: 顺序写入 " << num << " 条记录..." << std::endl;
  int64_t ok = 0;
  for (int64_t i = 0; i < num; ++i) {
    std::string key = seq_key(i);
    std::string val = random_value(rnd, FLAGS_value_size);
    int wid = find_worker_for_key(key);
    if (wid < 0) continue;
    int fd = get_slave_fd(wid);
    if (fd < 0) continue;

    auto t0 = std::chrono::high_resolution_clock::now();
    bool success = do_put(fd, key, val);
    auto t1 = std::chrono::high_resolution_clock::now();
    if (hist) hist->Add(std::chrono::duration<double, std::micro>(t1 - t0).count());
    if (success) ok++;
  }
  std::cout << "  完成: " << ok << "/" << num << std::endl;
}

// 随机写入
void bench_fillrandom(leveldb::Histogram* hist, int64_t num, class Random* rnd) {
  std::cout << "fillrandom: 随机写入 " << num << " 条记录..." << std::endl;
  int64_t ok = 0;
  for (int64_t i = 0; i < num; ++i) {
    std::string key = random_key(rnd);
    std::string val = random_value(rnd, FLAGS_value_size);
    int wid = find_worker_for_key(key);
    if (wid < 0) continue;
    int fd = get_slave_fd(wid);
    if (fd < 0) continue;

    auto t0 = std::chrono::high_resolution_clock::now();
    bool success = do_put(fd, key, val);
    auto t1 = std::chrono::high_resolution_clock::now();
    if (hist) hist->Add(std::chrono::duration<double, std::micro>(t1 - t0).count());
    if (success) ok++;
  }
  std::cout << "  完成: " << ok << "/" << num << std::endl;
}

// 覆盖写: 用 seq_key 范围的键随机覆盖
void bench_overwrite(leveldb::Histogram* hist, int64_t num, class Random* rnd) {
  std::cout << "overwrite: 随机覆盖写入 " << num << " 条记录..." << std::endl;
  int64_t range = num < 1000 ? num : num / 2;  // 覆盖前 range 个键
  int64_t ok = 0;
  for (int64_t i = 0; i < num; ++i) {
    std::string key = seq_key(rnd->Next() % range);
    std::string val = random_value(rnd, FLAGS_value_size);
    int wid = find_worker_for_key(key);
    if (wid < 0) continue;
    int fd = get_slave_fd(wid);
    if (fd < 0) continue;

    auto t0 = std::chrono::high_resolution_clock::now();
    bool success = do_put(fd, key, val);
    auto t1 = std::chrono::high_resolution_clock::now();
    if (hist) hist->Add(std::chrono::duration<double, std::micro>(t1 - t0).count());
    if (success) ok++;
  }
  std::cout << "  完成: " << ok << "/" << num << std::endl;
}

// 顺序删除
void bench_deleteseq(leveldb::Histogram* hist, int64_t num) {
  std::cout << "deleteseq: 顺序删除 " << num << " 条记录..." << std::endl;
  int64_t ok = 0;
  for (int64_t i = 0; i < num; ++i) {
    std::string key = seq_key(i);
    int wid = find_worker_for_key(key);
    if (wid < 0) continue;
    int fd = get_slave_fd(wid);
    if (fd < 0) continue;

    auto t0 = std::chrono::high_resolution_clock::now();
    bool success = do_delete(fd, key);
    auto t1 = std::chrono::high_resolution_clock::now();
    if (hist) hist->Add(std::chrono::duration<double, std::micro>(t1 - t0).count());
    if (success) ok++;
  }
  std::cout << "  完成: " << ok << "/" << num << std::endl;
}

// 随机删除
void bench_deleterandom(leveldb::Histogram* hist, int64_t num, class Random* rnd) {
  std::cout << "deleterandom: 随机删除 " << num << " 条记录..." << std::endl;
  int64_t range = num < 1000 ? num : num / 2;
  int64_t ok = 0;
  for (int64_t i = 0; i < num; ++i) {
    std::string key = seq_key(rnd->Next() % range);
    int wid = find_worker_for_key(key);
    if (wid < 0) continue;
    int fd = get_slave_fd(wid);
    if (fd < 0) continue;

    auto t0 = std::chrono::high_resolution_clock::now();
    bool success = do_delete(fd, key);
    auto t1 = std::chrono::high_resolution_clock::now();
    if (hist) hist->Add(std::chrono::duration<double, std::micro>(t1 - t0).count());
    if (success) ok++;
  }
  std::cout << "  完成: " << ok << "/" << num << std::endl;
}

// 顺序读取: key = seq_key(i)
void bench_readseq(leveldb::Histogram* hist, int64_t num) {
  std::cout << "readseq: 顺序读取 " << num << " 条记录..." << std::endl;
  int64_t found = 0;
  for (int64_t i = 0; i < num; ++i) {
    std::string key = seq_key(i);
    int wid = find_worker_for_key(key);
    if (wid < 0) continue;
    int fd = get_slave_fd(wid);
    if (fd < 0) continue;

    std::string value;
    auto t0 = std::chrono::high_resolution_clock::now();
    bool ok = do_get(fd, key, &value);
    auto t1 = std::chrono::high_resolution_clock::now();
    if (hist) hist->Add(std::chrono::duration<double, std::micro>(t1 - t0).count());
    if (ok) found++;
  }
  std::cout << "  找到: " << found << "/" << num << std::endl;
}

// 逆序读取
void bench_readreverse(leveldb::Histogram* hist, int64_t num) {
  std::cout << "readreverse: 逆序读取 " << num << " 条记录..." << std::endl;
  int64_t found = 0;
  for (int64_t i = num - 1; i >= 0; --i) {
    std::string key = seq_key(i);
    int wid = find_worker_for_key(key);
    if (wid < 0) continue;
    int fd = get_slave_fd(wid);
    if (fd < 0) continue;

    std::string value;
    auto t0 = std::chrono::high_resolution_clock::now();
    bool ok = do_get(fd, key, &value);
    auto t1 = std::chrono::high_resolution_clock::now();
    if (hist) hist->Add(std::chrono::duration<double, std::micro>(t1 - t0).count());
    if (ok) found++;
  }
  std::cout << "  找到: " << found << "/" << num << std::endl;
}

// 随机读取
void bench_readrandom(leveldb::Histogram* hist, int64_t num, class Random* rnd,
                      int64_t key_range) {
  std::cout << "readrandom: 随机读取 " << num << " 条记录..." << std::endl;
  int64_t found = 0;
  for (int64_t i = 0; i < num; ++i) {
    std::string key = seq_key(rnd->Next() % key_range);
    int wid = find_worker_for_key(key);
    if (wid < 0) continue;
    int fd = get_slave_fd(wid);
    if (fd < 0) continue;

    std::string value;
    auto t0 = std::chrono::high_resolution_clock::now();
    bool ok = do_get(fd, key, &value);
    auto t1 = std::chrono::high_resolution_clock::now();
    if (hist) hist->Add(std::chrono::duration<double, std::micro>(t1 - t0).count());
    if (ok) found++;
  }
  std::cout << "  找到: " << found << "/" << num << std::endl;
}

// 随机读取不存在的键
void bench_readmissing(leveldb::Histogram* hist, int64_t num, class Random* rnd) {
  std::cout << "readmissing: 随机读取不存在键 " << num << " 条..." << std::endl;
  int64_t found = 0;
  for (int64_t i = 0; i < num; ++i) {
    // 生成几乎不可能存在的随机键
    std::string key = random_key(rnd);
    int wid = find_worker_for_key(key);
    if (wid < 0) continue;
    int fd = get_slave_fd(wid);
    if (fd < 0) continue;

    std::string value;
    auto t0 = std::chrono::high_resolution_clock::now();
    bool ok = do_get(fd, key, &value);
    auto t1 = std::chrono::high_resolution_clock::now();
    if (hist) hist->Add(std::chrono::duration<double, std::micro>(t1 - t0).count());
    if (ok) found++;
  }
  std::cout << "  找到: " << found << "/" << num << " (应为 0)" << std::endl;
}

// 热点读取: 从 seq_key 范围的前 1% 随机读取
void bench_readhot(leveldb::Histogram* hist, int64_t num, class Random* rnd,
                   int64_t key_range) {
  std::cout << "readhot: 热点读取 (前 1% 键) " << num << " 条..." << std::endl;
  int64_t hot_range = key_range / 100;
  if (hot_range < 1) hot_range = 1;
  int64_t found = 0;
  for (int64_t i = 0; i < num; ++i) {
    std::string key = seq_key(rnd->Next() % hot_range);
    int wid = find_worker_for_key(key);
    if (wid < 0) continue;
    int fd = get_slave_fd(wid);
    if (fd < 0) continue;

    std::string value;
    auto t0 = std::chrono::high_resolution_clock::now();
    bool ok = do_get(fd, key, &value);
    auto t1 = std::chrono::high_resolution_clock::now();
    if (hist) hist->Add(std::chrono::duration<double, std::micro>(t1 - t0).count());
    if (ok) found++;
  }
  std::cout << "  找到: " << found << "/" << num << std::endl;
}

// 全库扫描: SCAN <min_key> <max_key>（覆盖所有从节点）
// 向每个从节点分别发送 SCAN，合并结果
void bench_scanall(leveldb::Histogram* hist) {
  std::cout << "scanall: 全库范围扫描..." << std::endl;
  int total_kv = 0;

  for (int wid = 0; wid < WORKER_COUNT; ++wid) {
    int fd = get_slave_fd(wid);
    if (fd < 0) continue;

    // 该从节点负责的 ASCII 范围
    int range_start = (wid == 0) ? 32 : (wid == 1) ? 43 : 86;  // 跳过控制字符
    int range_end   = (wid == 0) ? 42 : (wid == 1) ? 85 : 126;
    std::string start_key(1, static_cast<char>(range_start));
    std::string end_key(1, static_cast<char>(range_end));

    std::string req = "SCAN " + start_key + " " + end_key + "\n";
    auto t0 = std::chrono::high_resolution_clock::now();
    if (write(fd, req.c_str(), req.size()) < 0) continue;

    // 读取多行响应
    std::string response;
    char buf[4096];
    while (true) {
      ssize_t n = read(fd, buf, sizeof(buf) - 1);
      if (n < 0) { if (errno == EINTR) continue; break; }
      else if (n == 0) break;
      buf[n] = '\0'; response += buf;
      if (response.find("END\r\n") != std::string::npos ||
          response.find("END\n") != std::string::npos) break;
    }
    auto t1 = std::chrono::high_resolution_clock::now();

    // 统计 KVPAIR 行数
    std::istringstream iss(response);
    std::string line;
    int kv_count = 0;
    while (std::getline(iss, line)) {
      if (line.find("KVPAIR ") == 0) kv_count++;
    }
    total_kv += kv_count;
    std::cout << "  Worker " << wid << ": " << kv_count << " 条 KV" << std::endl;

    if (hist) hist->Add(std::chrono::duration<double, std::micro>(t1 - t0).count());
  }
  std::cout << "  总计: " << total_kv << " 条 KV" << std::endl;
}

// 打印直方图
void print_histogram(const std::string& name, leveldb::Histogram* hist) {
  std::cout << "\n" << name << " 直方图:" << std::endl;
  std::cout << hist->ToString() << std::endl;
}

// ==================== 命令解析 ====================

// 解析形如 "--key=value" 或 "--flag" 的参数
static bool parse_flag(const std::string& arg, const std::string& flag,
                       std::string* value) {
  if (arg.find(flag + "=") == 0) {
    *value = arg.substr(flag.size() + 1);
    return true;
  }
  return false;
}

// 解析命令行
struct BenchCommand {
  std::string benchmarks;
  bool valid = true;
};

BenchCommand parse_command(const std::string& line) {
  BenchCommand cmd;
  cmd.benchmarks = "fillseq";  // 默认

  std::istringstream iss(line);
  std::string token;
  while (iss >> token) {
    std::string val;
    if (parse_flag(token, "--benchmarks", &val)) {
      cmd.benchmarks = val;
    } else if (parse_flag(token, "--num", &val)) {
      FLAGS_num = std::stoll(val);
    } else if (parse_flag(token, "--key_size", &val)) {
      FLAGS_key_size = std::stoi(val);
    } else if (parse_flag(token, "--value_size", &val)) {
      FLAGS_value_size = std::stoi(val);
    } else if (parse_flag(token, "--histogram", &val)) {
      FLAGS_histogram = (val == "1");
    } else if (parse_flag(token, "--use_existing_db", &val)) {
      FLAGS_use_existing_db = (val == "1");
    }
  }
  return cmd;
}

// 执行一次测试流程: HELLO → benchmarks → QUIT
void run_benchmark(const BenchCommand& cmd, const std::string& host, int port) {
  // 1. HELLO
  std::cout << "===== HELLO =====" << std::endl;
  if (!do_hello(host, port)) {
    std::cerr << "HELLO 失败，跳过本次测试" << std::endl;
    return;
  }
  std::cout << "路由表获取成功 (" << route_table.size() << " 个从节点)" << std::endl;

  // 2. 若 use_existing_db=0，删除所有 seq_key 范围内的键（简化清理）
  Random rnd_clean(301);
  if (!FLAGS_use_existing_db) {
    int64_t clean_num = FLAGS_num * 2;  // 清理足够多的可能键
    std::cout << "清理旧数据库 (use_existing_db=0)..." << std::endl;
    for (int64_t i = 0; i < clean_num; ++i) {
      // 同时清理 seq_key 和随机键
      std::string key;
      if (i % 2 == 0) key = seq_key(i / 2);
      else key = random_key(&rnd_clean);
      int wid = find_worker_for_key(key);
      if (wid < 0) continue;
      int fd = get_slave_fd(wid);
      if (fd < 0) continue;
      do_delete(fd, key);
    }
    std::cout << "清理完成" << std::endl;
  }

  // 3. 解析并执行 benchmarks
  Random rnd(1000);
  std::istringstream bss(cmd.benchmarks);
  std::string bench_name;
  while (std::getline(bss, bench_name, ',')) {
    bench_name = trim(bench_name);
    if (bench_name.empty()) continue;

    leveldb::Histogram hist;
    hist.Clear();  // Histogram 构造函数不做初始化，必须显式 Clear
    leveldb::Histogram* phist = FLAGS_histogram ? &hist : nullptr;

    int64_t key_range = FLAGS_num;  // 用于读取操作的键范围

    if (bench_name == "fillseq") {
      bench_fillseq(phist, FLAGS_num, &rnd);
    } else if (bench_name == "fillrandom") {
      bench_fillrandom(phist, FLAGS_num, &rnd);
    } else if (bench_name == "overwrite") {
      bench_overwrite(phist, FLAGS_num, &rnd);
    } else if (bench_name == "deleteseq") {
      bench_deleteseq(phist, FLAGS_num);
    } else if (bench_name == "deleterandom") {
      bench_deleterandom(phist, FLAGS_num, &rnd);
    } else if (bench_name == "readseq") {
      bench_readseq(phist, FLAGS_num);
    } else if (bench_name == "readreverse") {
      bench_readreverse(phist, FLAGS_num);
    } else if (bench_name == "readrandom") {
      bench_readrandom(phist, FLAGS_num, &rnd, key_range);
    } else if (bench_name == "readmissing") {
      bench_readmissing(phist, FLAGS_num, &rnd);
    } else if (bench_name == "readhot") {
      bench_readhot(phist, FLAGS_num, &rnd, key_range);
    } else if (bench_name == "scanall") {
      bench_scanall(phist);
    } else {
      std::cerr << "未知操作: " << bench_name << std::endl;
    }

    if (FLAGS_histogram) {
      print_histogram(bench_name, &hist);
    }
  }

  // 4. QUIT
  std::cout << "===== QUIT =====" << std::endl;
  do_quit(host, port);

  // 5. 清理连接
  for (auto& p : slave_fds) close(p.second);
  slave_fds.clear();
  route_table.clear();
}

// ==================== 主函数 ====================

static volatile bool g_running = true;

void signal_handler(int) {
  std::cout << "\n收到退出信号，正在退出..." << std::endl;
  g_running = false;
}

int main(int argc, char* argv[]) {
  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);

  std::string host = DEFAULT_HOST;
  int port = DEFAULT_PORT;
  if (argc >= 2) host = argv[1];
  if (argc >= 3) port = std::stoi(argv[2]);

  std::cout << "DisKV 测试客户端已启动" << std::endl;
  std::cout << "主节点: " << host << ":" << port << std::endl;
  std::cout << "请输入测试命令（格式: --benchmarks=... --num=... ...）" << std::endl;
  std::cout << "支持的操作: fillseq fillrandom overwrite deleteseq deleterandom"
            << " readseq readreverse readrandom readmissing readhot scanall" << std::endl;
  std::cout << "可用参数: --benchmarks --num --key_size --value_size --histogram --use_existing_db" << std::endl;
  std::cout << "输入 quit 退出客户端" << std::endl;

  std::string user_input;
  while (g_running) {
    std::cout << ">> ";
    if (!std::getline(std::cin, user_input)) break;
    user_input = trim(user_input);
    if (user_input.empty()) continue;
    if (user_input == "quit" || user_input == "QUIT") break;

    BenchCommand cmd = parse_command(user_input);
    run_benchmark(cmd, host, port);
    std::cout << "测试完成，等待下一条命令..." << std::endl;
  }

  std::cout << "测试客户端已退出" << std::endl;
  return 0;
}
