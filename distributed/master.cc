// =============================================================================
// master.cc - DisKV 主节点（元数据服务器）
// =============================================================================
//
// 【架构定位】主节点是分布式系统中的"元数据层"，存储 key→vlog地址 的映射。
//           它本身是一个 WiscKey 实例，但通过 no_vlog=true 关闭了 vlog 层，
//           只保留 LSM-Tree（memtable + SSTable），不产生 .log 文件。
//
// 【职责范围】
//   1. 管理从节点路由表（ASCII 静态分区），响应客户端 HELLO 请求
//   2. 接收从节点的 PUT_ADDR/GET_ADDR/DELETE_ADDR/SCAN_ADDR 内部命令
//   3. 为 GC 提供 VALIDATE_ADDR/UPDATE_ADDR 支持
//   4. 监听从节点重连并正确映射身份
//
// 【关键数据结构】
//   g_db              — WiscKey LSM-Tree 指针（no_vlog=true）
//   g_worker_fds[i]   — Worker i 的 TCP 连接 fd（-1 表示未连接）
//   connections[fd]   — fd → Connection 映射（含缓冲区、类型、worker_id）
//   g_client_ips      — 已注册的客户端 IP 列表
//
// 【网络端口分配】
//   8888           — 客户端连接端口（HELLO/QUIT）
//   8889/8890/8891 — 从节点连接端口（Worker 0/1/2 各占一个独立端口）
//   ★ 独立端口设计确保从节点重连时主节点根据 accept 的端口立即确定 worker_id，
//     无需依赖连接顺序或握手协议。
//
// 【内部协议】主节点 ↔ 从节点（文本行，\n 分隔）
//   PUT_ADDR <key> <vlog_numb> <offset> <size>     → STORED / ERROR
//   GET_ADDR <key>                                 → ADDR <vlog> <off> <sz> / NOT_FOUND / ERROR
//   DELETE_ADDR <key>                              → DELETED / ERROR
//   SCAN_ADDR <start> <end> <worker_id>            → KVPAIR_ADDR ... \r\n END
//   VALIDATE_ADDR <key> <vlog> <off> <sz>          → VALID / INVALID / NOT_FOUND  (GC用)
//   UPDATE_ADDR <key> <old_vlog> <old_off> <old_sz> <new_vlog> <new_off> <new_sz> → UPDATED / ERROR  (GC用)
//
// 【外部协议】主节点 ↔ 客户端（文本行）
//   HELLO  → ROUTETABLE\r\n ... END\r\n
//   QUIT   → BYE\r\n
// =============================================================================

#include <arpa/inet.h>     // inet_ntop, htons, htonl
#include <csignal>         // signal, SIGINT, SIGTERM
#include <cstdlib>         // std::stoi
#include <cstring>         // memset
#include <errno.h>         // errno, EAGAIN, EINTR
#include <fcntl.h>         // fcntl, F_SETFL, O_NONBLOCK
#include <iostream>        // std::cout, std::cerr
#include <list>            // std::list (g_client_ips 用)
#include <netinet/in.h>    // sockaddr_in
#include <sstream>         // std::istringstream
#include <string>          // std::string
#include <sys/epoll.h>     // epoll_create1, epoll_ctl, epoll_wait
#include <sys/socket.h>    // socket, bind, listen, accept
#include <unistd.h>        // close, read, write, usleep
#include <unordered_map>   // std::unordered_map (connections 用)
#include <vector>          // std::vector (g_worker_fds 用)

// WiscKey 核心接口
#include "leveldb/db.h"           // DB, DB::Open, PutAddress, GetAddress, DeleteKey
#include "leveldb/options.h"      // Options, ReadOptions, WriteOptions
#include "leveldb/slice.h"        // Slice (零拷贝字符串视图)
#include "leveldb/status.h"       // Status (操作结果)
#include "leveldb/iterator.h"     // Iterator (范围扫描)
#include "util/coding.h"          // PutVarint64, GetVarint64 (vlog地址编解码)

// ==================== 系统常量 ====================

const int MAX_EVENTS = 128;                     // epoll 一次轮询最多处理的事件数
const int MASTER_CLIENT_PORT = 8888;            // 客户端连接端口
const int MASTER_WORKER_PORT_BASE = 8889;       // 从节点连接端口基址（Worker i → port 8889+i）
const int WORKER_COUNT = 3;                     // 从节点总数

// 从节点对外服务端口（客户端直连用），用于构建路由表
const int SLAVE_CLIENT_PORTS[WORKER_COUNT] = {9000, 9001, 9002};
// 从节点 IP
#define SLAVE0_IP "127.0.0.1"
#define SLAVE1_IP "127.0.0.1"
#define SLAVE2_IP "127.0.0.1"

// ASCII 静态分区表：Worker i 负责首字符 ASCII ∈ [range_start, range_end] 的 key
const int WORKER_RANGES[WORKER_COUNT][2] = {{0, 42}, {43, 85}, {86, 127}};

// ==================== 全局状态 ====================

static volatile bool g_running = true;          // 运行标志，信号处理器设为 false 触发安全退出
static leveldb::DB* g_db = nullptr;             // WiscKey LSM-Tree 数据库指针（no_vlog=true）

// 从节点连接管理：
//   g_worker_fds[i] = TCP fd of Worker i（-1 表示断开）
std::vector<int> g_worker_fds;

// 连接类型枚举：区分客户端连接和从节点连接
enum ConnType { CONN_CLIENT, CONN_WORKER };

// 每个 TCP 连接的上下文信息
struct Connection {
  std::string send_buffer;      // 用户态发送缓冲区（待通过 EPOLLOUT 发送的数据）
  std::string recv_buffer;      // 用户态接收缓冲区（已从内核读出但未解析的数据）
  ConnType conn_type;           // CONN_CLIENT 或 CONN_WORKER
  std::string client_ip;        // 若为客户端连接，对端 IP 地址
  int client_port;              // 若为客户端连接，对端端口号
  int worker_id;                // 若为从节点连接，记录其 worker 编号（0/1/2）
  bool close_after_send;        // 发送缓冲区清空后是否主动关闭连接（HELLO/QUIT 用）
  Connection() : client_port(0), conn_type(CONN_CLIENT), worker_id(-1), close_after_send(false) {}
};

// fd → Connection 映射表
// 每个连接必须拥有独立的 send_buffer 和 recv_buffer，
// 防止网络异步乱序导致的多客户端指令混淆。
std::unordered_map<int, Connection> connections;

// 已注册客户端 IP 列表（HELLO 时加入，QUIT 时移除）
std::list<std::string> g_client_ips; // TODO 暂不实现,未来可用于广播通知.

// ==================== 通用工具函数 ====================

// 去除字符串首尾空白字符（空格、制表、回车、换行）
// 用途：解析网络协议行时清理用户输入的冗余空白
static inline std::string trim(const std::string& s) {
  auto start = std::find_if_not(s.begin(), s.end(),
                                [](unsigned char ch) { return std::isspace(ch); });
  auto end = std::find_if_not(s.rbegin(), s.rend(),
                              [](unsigned char ch) { return std::isspace(ch); })
                 .base();
  return (start < end) ? std::string(start, end) : std::string();
}

// 将 fd 设为非阻塞模式（O_NONBLOCK）
// ★ ET（边缘触发）epoll 模式需要非阻塞 fd，否则 read/write 会阻塞整个事件循环
void set_nonblocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

// 信号处理器：Ctrl+C (SIGINT) 或 kill (SIGTERM) 时触发
// ★ 仅设置 g_running=false，不在此处执行任何 IO 或内存操作（信号安全）
void signal_handler(int signum) {
  std::cout << "\n收到退出信号: " << signum << "，正在安全关闭..." << std::endl;
  g_running = false;
}

// ==================== 路由与分区 ====================

// 根据 key 首字符的 ASCII 码返回负责的 worker 编号（0/1/2）
// ★ 这是静态 ASCII 范围分区的核心函数。
//   Worker 0: 0-42  (NUL→'*')
//   Worker 1: 43-85 ('+'→'U')
//   Worker 2: 86-127('V'→DEL) + 非ASCII字符
int get_worker_for_key(const std::string& key) {
  if (key.empty()) return 0;   // 空 key 归 Worker 0
  unsigned char ch = static_cast<unsigned char>(key[0]);
  for (int i = 0; i < WORKER_COUNT; ++i) {
    if (ch >= WORKER_RANGES[i][0] && ch <= WORKER_RANGES[i][1]) return i;
  }
  return 2;  // 非 ASCII 字符进入 Worker 2
}

// 获取指定 worker 负责的 ASCII 范围
// 用途：SCAN_ADDR 命令中过滤只属于某个 worker 的 key
void get_worker_range(int worker_id, int* range_start, int* range_end) {
  if (worker_id >= 0 && worker_id < WORKER_COUNT) {
    *range_start = WORKER_RANGES[worker_id][0];
    *range_end = WORKER_RANGES[worker_id][1];
  } else {
    *range_start = 0;
    *range_end = 127;
  }
}

// 构建路由表字符串，发送给客户端用于 HELLO 响应
// 格式：
//   ROUTETABLE\r\n
//   INDEX | IP:PORT | RANGE\r\n
//   0 <SLAVE0_IP>:9000 0-42\r\n
//   1 <SLAVE1_IP>:9001 43-85\r\n
//   2 <SLAVE2_IP>:9002 86-127\r\n
//   END\r\n
std::string build_route_table() {
  std::string table = "ROUTETABLE\r\n";
  table += "INDEX | IP:PORT | RANGE\r\n";
  const char* slave_ips[WORKER_COUNT] = {SLAVE0_IP, SLAVE1_IP, SLAVE2_IP};
  for (int i = 0; i < WORKER_COUNT; ++i) {
    table += std::to_string(i) + " " + slave_ips[i] + ":" +
             std::to_string(SLAVE_CLIENT_PORTS[i]) + " " +
             std::to_string(WORKER_RANGES[i][0]) + "-" +
             std::to_string(WORKER_RANGES[i][1]) + "\r\n";
  }
  table += "END\r\n";
  return table;
}

// ==================== 从节点内部协议处理 ====================

// 处理从节点发来的命令，返回响应字符串（以 \r\n 结尾）
//
// 参数:
//   line      — 从节点发来的完整文本行（已去除换行符）
//   worker_id — 发送命令的从节点编号（-1 表示未知）
//
// 返回值: 响应字符串（可能为空表示无需回复）
//
// ★ 此函数是主节点核心业务逻辑，实现了 LSM-Tree 的 CRUD 操作。
//   所有操作通过 PutAddress/GetAddress/DeleteKey 直接操作 memtable+SSTable，
//   不经过 vlog 层（因为 no_vlog=true）。
std::string process_worker_command(const std::string& line, int worker_id) {
  std::string trimmed = trim(line);
  if (trimmed.empty()) return "";  // 忽略空行

  // 解析命令和参数
  //   iss >> cmd  — 提取第一个空白分隔的单词作为命令
  //   getline      — 提取剩余全部内容作为参数
  std::istringstream iss(trimmed);
  std::string cmd;
  iss >> cmd;
  for (auto& c : cmd) c = std::toupper(static_cast<unsigned char>(c));

  // ===== PUT_ADDR：存储 key→vlog地址 映射 =====
  // 从节点写完 vlog 后调用，将地址注册到主节点的 LSM-Tree
  if (cmd == "PUT_ADDR") {
    std::string key;
    uint64_t vlog_numb = 0, offset = 0, size = 0;
    if (!(iss >> key >> vlog_numb >> offset >> size)) {
      return "ERROR: PUT_ADDR format: PUT_ADDR <key> <vlog_numb> <offset> <size>\r\n";
    }

    // 将 (vlog_number, offset, size) 编码为紧凑的三段 Varint64 字符串
    // ★ 地址格式: <varint64:vlog_number><varint64:offset><varint64:size>
    //   这种编码方式高效紧凑，可被 FetchValueFromVlog 直接解析
    std::string addr;
    leveldb::PutVarint64(&addr, vlog_numb);
    leveldb::PutVarint64(&addr, offset);
    leveldb::PutVarint64(&addr, size);

    // ★ 关键调用: PutAddress 绕过 vlog 层，直接将 key→addr 写入 memtable
    //   DBImpl 内部：mem_->Add(seq, kTypeValue, key, addr)
    leveldb::WriteOptions write_opts;
    write_opts.sync = false;  // 不强制 fsync（性能优先，从节点已持久化 vlog）
    leveldb::Status s = g_db->PutAddress(write_opts, key, addr);

    if (s.ok()) {
      std::cout << "  [PUT_ADDR] key=\"" << key << "\" vlog=" << vlog_numb
                << " offset=" << offset << " size=" << size
                << " (worker " << worker_id << ")" << std::endl;
      return "STORED\r\n";
    } else {
      std::cerr << "  [PUT_ADDR] 失败: " << s.ToString() << std::endl;
      return "ERROR: PutAddress failed\r\n";
    }
  }

  // ===== GET_ADDR / GCGET_ADDR：查询 key 对应的 vlog 地址 =====
  // GET_ADDR   — 从节点收到客户端 GET 时调用
  // GCGET_ADDR — 从节点 GC 线程搬迁验证时调用（功能相同，日志前缀区分）
  else if (cmd == "GET_ADDR" || cmd == "GCGET_ADDR") {
    std::string key;
    if (!(iss >> key)) {
      return "ERROR: GET_ADDR format: GET_ADDR <key>\r\n";
    }

    const char* log_prefix = (cmd == "GCGET_ADDR") ? "[GCGET_ADDR]" : "[GET_ADDR]";

    // ★ 关键调用: GetAddress 从 LSM-Tree 查询 key，返回存储的 vlog 地址
    //   不同于 Get()：不调用 FetchValueFromVlog，直接返回 LSM-Tree 中存储的值
    std::string addr_str;
    leveldb::Status s = g_db->GetAddress(leveldb::ReadOptions(), key, &addr_str);

    if (s.ok()) {
      // 解码地址字符串，提取三个字段
      leveldb::Slice addr_slice(addr_str);
      uint64_t vlog_numb, offset, size;
      if (!leveldb::GetVarint64(&addr_slice, &vlog_numb) ||
          !leveldb::GetVarint64(&addr_slice, &offset) ||
          !leveldb::GetVarint64(&addr_slice, &size)) {
        return "ERROR: Corrupted address in LSM-Tree\r\n";
      }
      // 打印信息
      std::cout << "  " << log_prefix << " key=\"" << key << "\" -> vlog=" << vlog_numb
                << " offset=" << offset << " size=" << size << std::endl;
      // 发送信息
      return "ADDR " + std::to_string(vlog_numb) + " " +
             std::to_string(offset) + " " + std::to_string(size) + "\r\n";
    } else if (s.IsNotFound()) {
      std::cout << "  " << log_prefix << " key=\"" << key << "\" -> NOT_FOUND" << std::endl;
      return "NOT_FOUND\r\n";
    } else {
      std::cerr << "  " << log_prefix << " 错误: " << s.ToString() << std::endl;
      return "ERROR: GetAddress failed\r\n";
    }
  }

  // ===== DELETE_ADDR：从 LSM-Tree 中标记删除 key =====
  // 从节点收到客户端 DELETE 时调用
  else if (cmd == "DELETE_ADDR") {
    std::string key;
    if (!(iss >> key)) {
      return "ERROR: DELETE_ADDR format: DELETE_ADDR <key>\r\n";
    }

    // ★ 关键调用: DeleteKey 在 memtable 中插入删除标记（kTypeDeletion）
    //   LSM-Tree 的 compaction 会在此后真正丢弃该条目
    leveldb::WriteOptions write_opts;
    write_opts.sync = false;
    leveldb::Status s = g_db->DeleteKey(write_opts, key);

    if (s.ok()) {
      std::cout << "  [DELETE_ADDR] key=\"" << key << "\" (worker " << worker_id << ")" << std::endl;
      return "DELETED\r\n";
    } else {
      std::cerr << "  [DELETE_ADDR] 失败: " << s.ToString() << std::endl;
      return "ERROR: DeleteKey failed\r\n";
    }
  }

  // ===== SCAN_ADDR：范围查询 key→addr 对 =====
  // 从节点收到客户端 SCAN 时调用，返回范围内所有 (key, addr) 对
  else if (cmd == "SCAN_ADDR") {
    std::string start_key, end_key;
    int req_worker_id = -1;
    if (!(iss >> start_key >> end_key >> req_worker_id)) {
      return "ERROR: SCAN_ADDR format: SCAN_ADDR <start_key> <end_key> <worker_id>\r\n";
    }

    std::cout << "  [SCAN_ADDR] start=\"" << start_key << "\" end=\"" << end_key
              << "\" worker=" << req_worker_id << std::endl;

    // 获取该 worker 的 ASCII 范围，过滤不属于它的 key
    int range_start, range_end;
    get_worker_range(req_worker_id, &range_start, &range_end);

    // ★ 使用 NewAddrIterator：返回原始 LSM-Tree 的 key→value 迭代器
    //   不经过 FetchValueFromVlog，value 直接是编码的 vlog 地址
    leveldb::Iterator* it = g_db->NewAddrIterator(leveldb::ReadOptions());
    std::string response;
    int count = 0;

    for (it->Seek(start_key);
         it->Valid() && it->key().ToString() <= end_key;
         it->Next()) {
      std::string key = it->key().ToString();

      // ASCII 范围过滤：只返回属于请求 worker 的 key
      if (!key.empty()) {
        unsigned char ch = static_cast<unsigned char>(key[0]);
        if (ch < static_cast<unsigned char>(range_start) ||
            ch > static_cast<unsigned char>(range_end)) {
          continue;
        }
      }

      // 解码地址
      std::string addr_str = it->value().ToString();
      leveldb::Slice addr_slice(addr_str);
      uint64_t vlog_numb, offset, size;
      if (!leveldb::GetVarint64(&addr_slice, &vlog_numb) ||
          !leveldb::GetVarint64(&addr_slice, &offset) ||
          !leveldb::GetVarint64(&addr_slice, &size)) {
        continue;  // 跳过损坏记录
      }

      response += "KVPAIR_ADDR " + key + " " + std::to_string(vlog_numb) +
                  " " + std::to_string(offset) + " " + std::to_string(size) + "\r\n";
      count++;
    }

    if (!it->status().ok()) {
      std::cerr << "  [SCAN_ADDR] 迭代错误: " << it->status().ToString() << std::endl;
      delete it;
      return "ERROR: Scan iterator failed\r\n";
    }
    delete it;

    response += "END\r\n";
    std::cout << "  [SCAN_ADDR] 返回 " << count << " 条结果" << std::endl;
    return response;
  }

  // // ===== VALIDATE_ADDR：GC 验证地址有效性 =====
  // // 从节点 GC 线程用：检查 key 的当前地址是否与给定地址完全一致
  // else if (cmd == "VALIDATE_ADDR") {
  //   std::string key;
  //   uint64_t vlog_numb = 0, offset = 0, size = 0;
  //   if (!(iss >> key >> vlog_numb >> offset >> size)) {
  //     return "ERROR: VALIDATE_ADDR format: VALIDATE_ADDR <key> <vlog_numb> <offset> <size>\r\n";
  //   }

  //   std::string addr_str;
  //   leveldb::Status s = g_db->GetAddress(leveldb::ReadOptions(), key, &addr_str);
  //   if (!s.ok()) {
  //     if (s.IsNotFound()) return "NOT_FOUND\r\n";
  //     return "INVALID\r\n";
  //   }

  //   // 解码并逐字段比较
  //   leveldb::Slice addr_slice(addr_str);
  //   uint64_t cur_vlog, cur_offset, cur_size;
  //   if (!leveldb::GetVarint64(&addr_slice, &cur_vlog) ||
  //       !leveldb::GetVarint64(&addr_slice, &cur_offset) ||
  //       !leveldb::GetVarint64(&addr_slice, &cur_size)) {
  //     return "INVALID\r\n";
  //   }

  //   // 三个字段完全一致才判定为 VALID
  //   if (cur_vlog == vlog_numb && cur_offset == offset && cur_size == size) {
  //     return "VALID\r\n";
  //   } else {
  //     return "INVALID\r\n";
  //   }
  // }

  // ===== UPDATE_ADDR：GC 搬迁数据后更新地址 =====
  // 从节点 GC 搬迁有效值到新 vlog 位置后调用
  // ★ 关键：使用 CAS (Compare-And-Swap) 语义
  //   先读取当前地址，仅当与 old_* 完全匹配时才写入 new_*，
  //   防止 GC 与并发客户端 PUT 产生竞态条件。
  else if (cmd == "UPDATE_ADDR") {
    std::string key;
    uint64_t old_vlog, old_offset, old_size;
    uint64_t new_vlog, new_offset, new_size;
    if (!(iss >> key >> old_vlog >> old_offset >> old_size
              >> new_vlog >> new_offset >> new_size)) {
      return "ERROR: UPDATE_ADDR format: UPDATE_ADDR <key> <old_vlog> <old_off> <old_sz> <new_vlog> <new_off> <new_sz>\r\n";
    }

    // 阶段1：CAS 检查 —— 确认当前地址仍是 old_*
    std::string addr_str;
    leveldb::Status s = g_db->GetAddress(leveldb::ReadOptions(), key, &addr_str);
    if (s.ok()) {
      leveldb::Slice addr_slice(addr_str);
      uint64_t cur_vlog, cur_offset, cur_size;
      if (leveldb::GetVarint64(&addr_slice, &cur_vlog) &&
          leveldb::GetVarint64(&addr_slice, &cur_offset) &&
          leveldb::GetVarint64(&addr_slice, &cur_size)) {
        if (cur_vlog != old_vlog || cur_offset != old_offset || cur_size != old_size) {
          std::cout << "  [UPDATE_ADDR] CAS 失败 key=\"" << key
                    << "\" 旧地址已变更（并发写入覆盖）" << std::endl;
          return "ERROR: Address changed, CAS failed\r\n";
        }
      }
    }

    // 阶段2：写入新地址
    std::string new_addr;
    leveldb::PutVarint64(&new_addr, new_vlog);
    leveldb::PutVarint64(&new_addr, new_offset);
    leveldb::PutVarint64(&new_addr, new_size);

    leveldb::WriteOptions write_opts;
    write_opts.sync = false;
    s = g_db->PutAddress(write_opts, key, new_addr);

    if (s.ok()) {
      std::cout << "  [UPDATE_ADDR] key=\"" << key
                << "\" old=(" << old_vlog << "," << old_offset << "," << old_size << ")"
                << " new=(" << new_vlog << "," << new_offset << "," << new_size << ")"
                << std::endl;
      return "UPDATED\r\n";
    } else {
      std::cerr << "  [UPDATE_ADDR] 失败: " << s.ToString() << std::endl;
      return "ERROR: UpdateAddress failed\r\n";
    }
  }

  else {
    std::cout << "  [WORKER] 未知内部命令: " << cmd << std::endl;
    return "ERROR: Unknown internal command\r\n";
  }
}

// ==================== 客户端协议处理 ====================

// 处理客户端发来的命令，返回响应字符串
//
// ★ 客户端协议：HELLO（获取路由表）和 QUIT（退出）和 COMPACT（手动合并）
//   PUT/GET/DELETE/SCAN/GC 由客户端直接发往从节点，不经过主节点。
std::string process_client_command(const std::string& line,
                                   const std::string& client_ip,
                                   int client_port) {
  std::string trimmed = trim(line);
  if (trimmed.empty()) return "";

  std::string cmd = trimmed;
  for (auto& c : cmd) c = std::toupper(static_cast<unsigned char>(c));

  // HELLO: 客户端请求路由表
  //   主节点注册客户端 IP，返回路由表，然后主动断开连接（close_after_send）
  if (cmd == "HELLO") {
    g_client_ips.push_back(client_ip);  // 注册客户端 IP 到全局列表
    std::cout << "客户端IP注册: " << client_ip << std::endl;
    return build_route_table();  // 返回路由表字符串
  }

  // QUIT: 客户端退出
  //   主节点注销客户端 IP，返回 BYE，然后主动断开连接
  else if (cmd == "QUIT") {
    g_client_ips.remove(client_ip);  // 从全局列表移除客户端 IP
    std::cout << "客户端IP注销: " << client_ip << std::endl;
    return "BYE\r\n";
  }

  // COMPACT: 强制主节点执行全量 Compaction
  //   将 memtable 刷盘为 SSTable，合并所有层级的 SSTable，
  //   丢弃删除标记和过期版本，完成后返回 COMPACT OK
  else if (cmd == "COMPACT") {
    std::cout << "客户端 " << client_ip << " 请求手动 Compaction" << std::endl;
    g_db->CompactRange(nullptr, nullptr);  // nullptr = 全范围压缩
    std::cout << "手动 Compaction 完成" << std::endl;
    return "COMPACT OK\r\n";
  }

  else {
    return "ERROR: Unknown command. Use HELLO, QUIT or COMPACT\r\n";
  }
}

// ==================== 主函数 ====================

int main() {
  // 注册信号处理器：Ctrl+C 或 kill 时触发安全退出流程
  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);

  // ===== 阶段0：打开 WiscKey LSM-Tree 数据库（no_vlog=true） =====
  // ★ no_vlog=true 意味着 WiscKey 不创建 .log (vlog) 文件，
  //   仅使用 memtable + SSTable (.ldb) + MANIFEST + CURRENT
  std::string db_path = "./diskv_master_db";

  leveldb::Options options;
  options.create_if_missing = true;          // 目录不存在则自动创建
  options.no_vlog = true;                    // ★ 关闭 vlog 层
  options.write_buffer_size = 1 * 1024 * 1024;  // memtable 大小 1MB（地址很小）
  options.max_file_size = 2 * 1024 * 1024;       // SSTable 大小上限 2MB

  leveldb::Status status = leveldb::DB::Open(options, db_path, &g_db);
  if (!status.ok()) {
    std::cerr << "Master: 无法打开 WiscKey LSM-Tree 数据库: " << status.ToString() << std::endl;
    return 1;
  }
  std::cout << "Master: WiscKey LSM-Tree 数据库已打开 (no_vlog=true), 路径: " << db_path << std::endl;
  std::cout << "  - 存储: key -> (vlog_number, offset, size)" << std::endl;
  std::cout << "  - 不生成 .log 文件，仅 .ldb / MANIFEST / CURRENT / LOG" << std::endl;

  // ===== 阶段1：创建监听套接字 =====

  // --- 1a. 客户端监听套接字 (port 8888) ---
  int master_client_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (master_client_fd < 0) {
    std::cerr << "创建 master_client 套接字失败" << std::endl; delete g_db; return 1;
  }
  int opt_c = 1;
  setsockopt(master_client_fd, SOL_SOCKET, SO_REUSEADDR, &opt_c, sizeof(opt_c));

  struct sockaddr_in master_client_addr;
  memset(&master_client_addr, 0, sizeof(master_client_addr));
  master_client_addr.sin_family = AF_INET;
  master_client_addr.sin_addr.s_addr = htonl(INADDR_ANY); // 任意IP
  master_client_addr.sin_port = htons(MASTER_CLIENT_PORT); // 8888

  if (bind(master_client_fd, (struct sockaddr*)&master_client_addr, sizeof(master_client_addr)) < 0) {
    std::cerr << "绑定 master_client 失败" << std::endl;
    close(master_client_fd); delete g_db; return 1;
  }
  if (listen(master_client_fd, 128) < 0) {  // 128: 等待连接队列长度
    std::cerr << "监听 client 端口失败" << std::endl;
    close(master_client_fd); delete g_db; return 1;
  }
  std::cout << "Master: 客户端监听 [0.0.0.0:" << MASTER_CLIENT_PORT << "] (INADDR_ANY)" << std::endl;

  // --- 1b. 从节点监听套接字（Worker 0→8889, Worker 1→8890, Worker 2→8891） ---
  // 设计要点：每个从节点独占一个端口，主节点根据 accept 的端口直接确定 worker_id
  int worker_listen_fds[WORKER_COUNT];
  for (int i = 0; i < WORKER_COUNT; ++i) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
      std::cerr << "创建 worker_listen_fds[" << i << "] 失败" << std::endl;
      for (int j = 0; j < i; ++j) close(worker_listen_fds[j]);
      close(master_client_fd); delete g_db; return 1;
    }
    int opt_w = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt_w, sizeof(opt_w));

    struct sockaddr_in waddr;
    memset(&waddr, 0, sizeof(waddr));
    waddr.sin_family = AF_INET;
    waddr.sin_addr.s_addr = htonl(INADDR_ANY);
    waddr.sin_port = htons(MASTER_WORKER_PORT_BASE + i);  // 8889 + i

    if (bind(fd, (struct sockaddr*)&waddr, sizeof(waddr)) < 0) {
      std::cerr << "绑定 worker_listen_fds[" << i << "] 失败 (port "
                << (MASTER_WORKER_PORT_BASE + i) << ")" << std::endl;
      close(fd);
      for (int j = 0; j < i; ++j) close(worker_listen_fds[j]);
      close(master_client_fd); delete g_db; return 1;
    }
    if (listen(fd, 1) < 0) {  // backlog=1: 每个端口只需接受一个连接
      std::cerr << "监听 worker port " << (MASTER_WORKER_PORT_BASE + i) << " 失败" << std::endl;
      close(fd);
      for (int j = 0; j < i; ++j) close(worker_listen_fds[j]);
      close(master_client_fd); delete g_db; return 1;
    }
    worker_listen_fds[i] = fd;
    std::cout << "Master: Worker " << i << " 监听 [0.0.0.0:"
              << (MASTER_WORKER_PORT_BASE + i) << "] (INADDR_ANY)" << std::endl;
  }

  // ===== 阶段2：等待所有从节点连接（非阻塞轮询，支持任意顺序接入） =====
  g_worker_fds.resize(WORKER_COUNT, -1);  // 初始化为 -1（未连接）
  std::cout << "Master: 等待 " << WORKER_COUNT << " 个从节点连接..."
            << " (port " << MASTER_WORKER_PORT_BASE << "-"
            << (MASTER_WORKER_PORT_BASE + WORKER_COUNT - 1) << ")" << std::endl;

  // 将所有监听 fd 设为非阻塞，支持轮询 accept
  for (int i = 0; i < WORKER_COUNT; ++i) set_nonblocking(worker_listen_fds[i]);

  int connected_count = 0;
  while (connected_count < WORKER_COUNT && g_running) {
    for (int i = 0; i < WORKER_COUNT; ++i) {
      if (g_worker_fds[i] >= 0) continue;  // 该 worker 已连接，跳过

      struct sockaddr_in slave_addr;
      socklen_t slave_len = sizeof(slave_addr);
      int slave_fd = accept(worker_listen_fds[i], (struct sockaddr*)&slave_addr, &slave_len);
      if (slave_fd >= 0) {
        // ★ 根据 accept 的端口直接确定 worker_id = i
        g_worker_fds[i] = slave_fd;
        connected_count++;

        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &slave_addr.sin_addr, ip, sizeof(ip));
        int port = ntohs(slave_addr.sin_port);
        std::cout << "  Worker " << i << " 已连接: fd=" << slave_fd
                  << " (port " << (MASTER_WORKER_PORT_BASE + i) << ")"
                  << " [" << connected_count << "/" << WORKER_COUNT << "]"
                  << std::endl;
      } else if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
        std::cerr << "accept Worker " << i << " 失败: " << strerror(errno) << std::endl;
        for (int j = 0; j < WORKER_COUNT; ++j) close(worker_listen_fds[j]);
        close(master_client_fd); delete g_db; return 1;
      }
    }
    if (connected_count < WORKER_COUNT) usleep(50000);  // 50ms 轮询间隔，降低 CPU 占用
  }
  if (!g_running) {
    for (int j = 0; j < WORKER_COUNT; ++j) { close(worker_listen_fds[j]); if (g_worker_fds[j] >= 0) close(g_worker_fds[j]); }
    close(master_client_fd); delete g_db; return 0;
  }
  std::cout << "Master: 全部 " << WORKER_COUNT << " 个从节点已连接" << std::endl;

  // ===== 阶段3：创建 epoll 实例并注册所有 fd =====
  int epoll_fd = epoll_create1(0);
  if (epoll_fd < 0) {
    std::cerr << "创建 epoll 失败" << std::endl;
    for (int i = 0; i < WORKER_COUNT; ++i) { close(worker_listen_fds[i]); if (g_worker_fds[i] >= 0) close(g_worker_fds[i]); }
    close(master_client_fd); delete g_db; return 1;
  }

  struct epoll_event ev;

  // 客户端监听 → LT 模式（EPOLLIN），监听新连接
  ev.events = EPOLLIN;
  ev.data.fd = master_client_fd;
  epoll_ctl(epoll_fd, EPOLL_CTL_ADD, master_client_fd, &ev);

  // 从节点监听 + 已连接从节点 fd
  for (int i = 0; i < WORKER_COUNT; ++i) {
    // 监听从节点连接的套接字（用于重连）
    ev.events = EPOLLIN;
    ev.data.fd = worker_listen_fds[i];
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, worker_listen_fds[i], &ev);

    // 已连接的从节点 fd → ET 模式（EPOLLIN | EPOLLOUT | EPOLLET）
    int wfd = g_worker_fds[i];
    if (wfd >= 0) {
      set_nonblocking(wfd);
      Connection wconn;
      wconn.conn_type = CONN_WORKER;
      wconn.worker_id = i;
      connections[wfd] = wconn;
      ev.events = EPOLLIN | EPOLLOUT | EPOLLET;       // ET 模式 + 读写监听
      ev.data.fd = wfd;
      if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, wfd, &ev) < 0) {
        std::cerr << "添加 worker_fd=" << wfd << " 到 epoll 失败" << std::endl;
      }
      std::cout << "  worker_fd=" << wfd << " (worker " << i << ") 已加入 epoll 监视" << std::endl;
    }
  }

  std::cout << "Master: 进入事件循环，等待请求..." << std::endl;

  // ===== 阶段4：主事件循环（epoll 驱动） =====
  // ★ 采用 ET（边缘触发）+ 非阻塞 IO 的事件驱动架构
  //   每个循环:
  //     1. epoll_wait 等待事件（1s 超时，用于定期检查 g_running）
  //     2. 新客户端连接 → accept → 注册到 epoll
  //     3. 从节点重连 → 根据端口确定 worker_id → 注册到 epoll
  //     4. 数据可读 → 读入 recv_buffer → 按 \n 分割行 → 调用 process_*_command
  //     5. 数据可写 → 从 send_buffer 发送到内核
  struct epoll_event events[MAX_EVENTS];
  while (g_running) {
    int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, 1000);  // 1秒超时
    if (nfds < 0) {
      if (!g_running) break;
      if (errno == EINTR) continue;
      std::cerr << "epoll_wait 错误" << std::endl; break;
    }

    for (int i = 0; i < nfds; ++i) {
      int fd = events[i].data.fd;

      // —— 新客户端连接事件 ——
      if (fd == master_client_fd) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(master_client_fd, (struct sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) {
          if (errno == EAGAIN || errno == EWOULDBLOCK) continue;  // 无新连接
          std::cerr << "accept 客户端失败" << std::endl; continue;
        }
        set_nonblocking(client_fd);

        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
        int client_port = ntohs(client_addr.sin_port);

        Connection conn;
        conn.client_ip = client_ip;
        conn.client_port = client_port;
        conn.conn_type = CONN_CLIENT;
        connections[client_fd] = conn;

        // ★ 客户端连接使用 ET 模式 + EPOLLRDHUP（对端关闭感知）
        ev.events = EPOLLIN | EPOLLOUT | EPOLLET | EPOLLRDHUP;
        ev.data.fd = client_fd;
        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev) < 0) {
          std::cerr << "添加客户端 fd=" << client_fd << " 到 epoll 失败" << std::endl;
          close(client_fd); continue;
        }
        std::cout << "新客户端: fd=" << client_fd
                  << " socket=" << client_ip << ":" << client_port << std::endl;
        continue;  //  关键：跳过后续的 worker listen 检查和 connections 查找，
                   //  防止 master_client_fd 被误当作已断开连接而 close()
      }

      // —— 从节点重连事件（根据端口确定 worker_id） ——
      bool is_worker_listen = false;
      for (int k = 0; k < WORKER_COUNT; ++k) {
        if (fd == worker_listen_fds[k]) {
          is_worker_listen = true;
          struct sockaddr_in slave_addr;
          socklen_t slave_len = sizeof(slave_addr);
          int slave_fd = accept(worker_listen_fds[k], (struct sockaddr*)&slave_addr, &slave_len);
          if (slave_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            std::cerr << "accept Worker " << k << " 重连失败" << std::endl; break;
          }
          set_nonblocking(slave_fd);

          int wid = k;  // ★ 端口号直接确定 worker_id
          // 清理该 worker 的旧连接（如果存在）
          if (g_worker_fds[wid] >= 0 && g_worker_fds[wid] != slave_fd) {
            int old_fd = g_worker_fds[wid];
            std::cout << "  [MASTER] 关闭 Worker " << wid << " 旧连接 fd=" << old_fd << std::endl;
            epoll_ctl(epoll_fd, EPOLL_CTL_DEL, old_fd, nullptr);
            close(old_fd);
            connections.erase(old_fd);
          }
          g_worker_fds[wid] = slave_fd;

          Connection wconn;
          wconn.conn_type = CONN_WORKER;
          wconn.worker_id = wid;
          connections[slave_fd] = wconn;

          ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
          ev.data.fd = slave_fd;
          if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, slave_fd, &ev) < 0) {
            std::cerr << "添加 Worker " << wid << " 重连 fd=" << slave_fd << " 失败" << std::endl;
            close(slave_fd); g_worker_fds[wid] = -1; break;
          }
          std::cout << "Worker " << wid << " 重连成功: fd=" << slave_fd
                    << " (port " << (MASTER_WORKER_PORT_BASE + wid) << ")" << std::endl;
          break;
        }
      }
      if (is_worker_listen) continue;

      // —— 已连接 fd 的 IO 读写事件 ——
      {
        bool closed = false;
        auto conn_it = connections.find(fd);
        if (conn_it == connections.end()) { close(fd); continue; }
        Connection& conn = conn_it->second;

        // 对端正常关闭（EPOLLRDHUP）
        if (events[i].events & EPOLLRDHUP) {
          if (conn.conn_type == CONN_CLIENT) std::cout << "客户端断开: fd=" << fd << std::endl;
          else {
            std::cout << "从节点断开: fd=" << fd << " worker_id=" << conn.worker_id << std::endl;
            if (conn.worker_id >= 0 && conn.worker_id < WORKER_COUNT) g_worker_fds[conn.worker_id] = -1;
          }
          closed = true;
        }

        // 错误/异常断开
        if (!closed && (events[i].events & (EPOLLERR | EPOLLHUP))) {
          std::cerr << "fd=" << fd << " 异常" << std::endl; closed = true;
        }

        // —— 可写事件（EPOLLOUT）：发送 send_buffer 中的数据 ——
        if (!closed && (events[i].events & EPOLLOUT)) {
          while (!conn.send_buffer.empty()) {
            int bw = write(fd, conn.send_buffer.data(), conn.send_buffer.size());
            if (bw < 0) {
              if (errno == EAGAIN || errno == EWOULDBLOCK) break;  // 内核发送缓冲区满
              std::cerr << "写入错误 fd=" << fd << std::endl; closed = true; break;
            } else if (bw == 0) continue;
            else conn.send_buffer.erase(0, bw);
          }
          // 发送完毕后的状态处理
          if (conn.send_buffer.empty()) {
            if (conn.close_after_send && conn.conn_type == CONN_CLIENT) {
              closed = true;  // HELLO/QUIT 后主动断开客户端
            } else {
              // 取消 EPOLLOUT 监听（降低不必要的事件触发）
              ev.events = EPOLLIN | EPOLLET | (conn.conn_type == CONN_CLIENT ? EPOLLRDHUP : 0);
              ev.data.fd = fd;
              epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev);
            }
          }
        }

        // —— 可读事件（EPOLLIN）：读取数据并解析命令 ——
        if (!closed && (events[i].events & EPOLLIN)) {
          while (true) {  // ★ ET 模式下必须循环 read 直到 EAGAIN
            char temp_buffer[256] = {0};
            int br = read(fd, temp_buffer, sizeof(temp_buffer) - 1);
            if (br > 0) {
              conn.recv_buffer.append(temp_buffer, br);  // 追加到接收缓冲区

              // 按 \n 分割出完整的命令行
              size_t pos;
              while ((pos = conn.recv_buffer.find('\n')) != std::string::npos) {
                std::string line = conn.recv_buffer.substr(0, pos);
                conn.recv_buffer.erase(0, pos + 1);  // 移除已处理的行（含 \n）

                std::string response;
                if (conn.conn_type == CONN_CLIENT) {
                  // 客户端协议：仅 HELLO/QUIT
                  response = process_client_command(line, conn.client_ip, conn.client_port);
                  if (!response.empty()) {
                    conn.close_after_send = true;  // 回复后主动断开
                    conn.send_buffer += response;
                  }
                } else {
                  // 从节点内部协议：PUT_ADDR/GET_ADDR/DELETE_ADDR/SCAN_ADDR...
                  response = process_worker_command(line, conn.worker_id);
                  if (!response.empty()) conn.send_buffer += response;
                }
              }
            } else if (br == 0) {
              // read 返回 0：对端关闭连接
              if (conn.conn_type == CONN_CLIENT) std::cout << "客户端关闭连接: fd=" << fd << std::endl;
              else {
                std::cout << "从节点关闭连接: fd=" << fd << " worker_id=" << conn.worker_id << std::endl;
                if (conn.worker_id >= 0 && conn.worker_id < WORKER_COUNT) g_worker_fds[conn.worker_id] = -1;
              }
              closed = true; break;
            } else {
              // read 返回 -1
              if (errno == EAGAIN || errno == EWOULDBLOCK) break;  // 数据读完
              std::cerr << "读取错误 fd=" << fd << std::endl; closed = true; break;
            }
          }
          // 读完后若还有待发送数据，重新注册 EPOLLOUT
          if (!closed && !conn.send_buffer.empty()) {
            ev.events = EPOLLIN | EPOLLOUT | EPOLLET | (conn.conn_type == CONN_CLIENT ? EPOLLRDHUP : 0);
            ev.data.fd = fd; epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev);
          }
        }

        // 连接关闭：从 epoll 移除，关闭 fd，释放资源
        if (closed) {
          epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
          close(fd);
          connections.erase(fd);  // 释放 send_buffer/recv_buffer 内存
        }
      }
    }
  }

  // ===== 阶段5：安全关闭 =====
  std::cout << "Master: 正在关闭..." << std::endl;
  close(epoll_fd);

  // 关闭所有从节点连接和监听 fd
  for (int i = 0; i < WORKER_COUNT; ++i) {
    if (g_worker_fds[i] >= 0) close(g_worker_fds[i]);
    close(worker_listen_fds[i]);
  }
  close(master_client_fd);

  // ★ 关键：关闭数据库前强制执行 Compaction，将 memtable 刷盘为 SSTable
  //    CompactRange(nullptr, nullptr) → TEST_CompactMemTable() → WriteLevel0Table()
  //    → 生成 .ldb 文件 + 更新 MANIFEST，确保数据持久化
  std::cout << "Master: 正在关闭 LSM-Tree 数据库（先执行 Compaction 落盘）..." << std::endl;
  g_db->CompactRange(nullptr, nullptr);
  delete g_db;       // ~DBImpl() 等待后台 compaction 完成并释放资源
  g_db = nullptr;
  std::cout << "Master: 已安全退出" << std::endl;
  return 0;
}
