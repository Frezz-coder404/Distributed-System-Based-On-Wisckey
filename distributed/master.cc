// =============================================================================
// master.cc - DisKV 主节点：存储 LSM-Tree（键→vlog地址映射），不存储 vlog 文件
// =============================================================================
//
// 架构说明（节点级键值分离）：
//   主节点 (Master)  : 运行 WiscKey 的 LSM-Tree（no_vlog=true），
//                      存储 key → (vlog_number, offset, size) 映射。
//                      数据库目录下只有 *.ldb / MANIFEST / CURRENT 等文件，
//                      不生成 *.log（vlog）文件。
//   从节点 (Slave)   : 只存储 vlog 文件（*.log），保存实际的值数据。
//                      从节点通过网络与主节点通信，获取/存储地址映射。
//   客户端 (Client)  : 与从节点直接交互，协议不变。
//
// 内部协议（主节点 ↔ 从节点，基于文本行，换行符 \n）：
//   PUT_ADDR <key> <vlog_numb> <offset> <size>
//     → STORED\r\n         （地址已存入 LSM-Tree）
//     → ERROR <msg>\r\n    （存储失败）
//   GET_ADDR <key>
//     → ADDR <vlog_numb> <offset> <size>\r\n  （找到地址）
//     → NOT_FOUND\r\n                          （键不存在）
//     → ERROR <msg>\r\n                        （查询失败）
//   DELETE_ADDR <key>
//     → DELETED\r\n        （已标记删除）
//     → ERROR <msg>\r\n    （删除失败）
//   SCAN_ADDR <start_key> <end_key> <worker_id>
//     → KVPAIR_ADDR <key> <vlog_numb> <offset> <size>\r\n  ...
//     → END\r\n
//     → ERROR <msg>\r\n
//
// 外部协议（主节点 ↔ 客户端，与原来一致）：
//   HELLO → ROUTETABLE ... END\r\n
//   QUIT  → BYE\r\n
// =============================================================================

#include <arpa/inet.h>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <list>
#include <netinet/in.h>
#include <sstream>
#include <string>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "leveldb/iterator.h"
#include "util/coding.h"

const int MAX_EVENTS = 128;
const int MASTER_CLIENT_PORT = 8888;
const int MASTER_WORKER_PORT = 8889;
const int WORKER_COUNT = 3;

const int SLAVE_CLIENT_PORTS[WORKER_COUNT] = {9000, 9001, 9002};
const std::string SLAVE_IP = "127.0.0.1";
const int WORKER_RANGES[WORKER_COUNT][2] = {{0, 42}, {43, 85}, {86, 127}};

static volatile bool g_running = true;
static leveldb::DB* g_db = nullptr;
std::vector<int> g_worker_fds;
std::vector<int> g_worker_ids;

enum ConnType { CONN_CLIENT, CONN_WORKER };

struct Connection {
  std::string send_buffer;
  std::string recv_buffer;
  std::string client_ip;
  int client_port;
  ConnType conn_type;
  int worker_id;
  bool close_after_send;
  Connection() : client_port(0), conn_type(CONN_CLIENT), worker_id(-1), close_after_send(false) {}
};

std::unordered_map<int, Connection> connections;
std::list<std::string> g_client_ips;

static inline std::string trim(const std::string& s) {
  auto start = std::find_if_not(s.begin(), s.end(), [](unsigned char ch) { return std::isspace(ch); });
  auto end = std::find_if_not(s.rbegin(), s.rend(), [](unsigned char ch) { return std::isspace(ch); }).base();
  return (start < end) ? std::string(start, end) : std::string();
}

void set_nonblocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

void signal_handler(int signum) {
  std::cout << "\n收到退出信号: " << signum << "，正在安全关闭..." << std::endl;
  g_running = false;
}

int get_worker_for_key(const std::string& key) {
  if (key.empty()) return 0;
  unsigned char ch = static_cast<unsigned char>(key[0]);
  for (int i = 0; i < WORKER_COUNT; ++i) {
    if (ch >= WORKER_RANGES[i][0] && ch <= WORKER_RANGES[i][1]) return i;
  }
  return 2;
}

void get_worker_range(int worker_id, int* range_start, int* range_end) {
  if (worker_id >= 0 && worker_id < WORKER_COUNT) {
    *range_start = WORKER_RANGES[worker_id][0];
    *range_end = WORKER_RANGES[worker_id][1];
  } else {
    *range_start = 0;
    *range_end = 127;
  }
}

std::string build_route_table() {
  std::string table = "ROUTETABLE\r\n";
  table += "INDEX | IP:PORT | RANGE\r\n";
  for (int i = 0; i < WORKER_COUNT; ++i) {
    table += std::to_string(i) + " " + SLAVE_IP + ":" +
             std::to_string(SLAVE_CLIENT_PORTS[i]) + " " +
             std::to_string(WORKER_RANGES[i][0]) + "-" +
             std::to_string(WORKER_RANGES[i][1]) + "\r\n";
  }
  table += "END\r\n";
  return table;
}

// ==================== 内部协议处理（从节点 → 主节点） ====================

std::string process_worker_command(const std::string& line, int worker_id) {
  std::string trimmed = trim(line);
  if (trimmed.empty()) return "";

  std::istringstream iss(trimmed);
  std::string cmd;
  iss >> cmd;
  for (auto& c : cmd) c = std::toupper(static_cast<unsigned char>(c));

  if (cmd == "PUT_ADDR") {
    std::string key;
    uint64_t vlog_numb = 0, offset = 0, size = 0;
    if (!(iss >> key >> vlog_numb >> offset >> size)) {
      return "ERROR: PUT_ADDR format: PUT_ADDR <key> <vlog_numb> <offset> <size>\r\n";
    }

    std::string addr;
    leveldb::PutVarint64(&addr, vlog_numb);
    leveldb::PutVarint64(&addr, offset);
    leveldb::PutVarint64(&addr, size);

    leveldb::WriteOptions write_opts;
    write_opts.sync = false;
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
  else if (cmd == "GET_ADDR") {
    std::string key;
    if (!(iss >> key)) {
      return "ERROR: GET_ADDR format: GET_ADDR <key>\r\n";
    }

    std::string addr_str;
    leveldb::Status s = g_db->GetAddress(leveldb::ReadOptions(), key, &addr_str);

    if (s.ok()) {
      leveldb::Slice addr_slice(addr_str);
      uint64_t vlog_numb, offset, size;
      if (!leveldb::GetVarint64(&addr_slice, &vlog_numb) ||
          !leveldb::GetVarint64(&addr_slice, &offset) ||
          !leveldb::GetVarint64(&addr_slice, &size)) {
        return "ERROR: Corrupted address in LSM-Tree\r\n";
      }
      std::cout << "  [GET_ADDR] key=\"" << key << "\" -> vlog=" << vlog_numb
                << " offset=" << offset << " size=" << size << std::endl;
      return "ADDR " + std::to_string(vlog_numb) + " " +
             std::to_string(offset) + " " + std::to_string(size) + "\r\n";
    } else if (s.IsNotFound()) {
      std::cout << "  [GET_ADDR] key=\"" << key << "\" -> NOT_FOUND" << std::endl;
      return "NOT_FOUND\r\n";
    } else {
      std::cerr << "  [GET_ADDR] 错误: " << s.ToString() << std::endl;
      return "ERROR: GetAddress failed\r\n";
    }
  }
  else if (cmd == "DELETE_ADDR") {
    std::string key;
    if (!(iss >> key)) {
      return "ERROR: DELETE_ADDR format: DELETE_ADDR <key>\r\n";
    }

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
  else if (cmd == "SCAN_ADDR") {
    std::string start_key, end_key;
    int req_worker_id = -1;
    if (!(iss >> start_key >> end_key >> req_worker_id)) {
      return "ERROR: SCAN_ADDR format: SCAN_ADDR <start_key> <end_key> <worker_id>\r\n";
    }

    std::cout << "  [SCAN_ADDR] start=\"" << start_key << "\" end=\"" << end_key
              << "\" worker=" << req_worker_id << std::endl;

    int range_start, range_end;
    get_worker_range(req_worker_id, &range_start, &range_end);

    leveldb::Iterator* it = g_db->NewAddrIterator(leveldb::ReadOptions());
    std::string response;
    int count = 0;

    for (it->Seek(start_key); it->Valid() && it->key().ToString() <= end_key; it->Next()) {
      std::string key = it->key().ToString();
      if (!key.empty()) {
        unsigned char ch = static_cast<unsigned char>(key[0]);
        if (ch < static_cast<unsigned char>(range_start) ||
            ch > static_cast<unsigned char>(range_end)) {
          continue;
        }
      }

      std::string addr_str = it->value().ToString();
      leveldb::Slice addr_slice(addr_str);
      uint64_t vlog_numb, offset, size;
      if (!leveldb::GetVarint64(&addr_slice, &vlog_numb) ||
          !leveldb::GetVarint64(&addr_slice, &offset) ||
          !leveldb::GetVarint64(&addr_slice, &size)) {
        continue;
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
  else {
    std::cout << "  [WORKER] 未知内部命令: " << cmd << std::endl;
    return "ERROR: Unknown internal command\r\n";
  }
}

// ==================== 客户端协议处理 ====================

std::string process_client_command(const std::string& line,
                                   const std::string& client_ip,
                                   int client_port) {
  std::string trimmed = trim(line);
  if (trimmed.empty()) return "";

  std::string cmd = trimmed;
  for (auto& c : cmd) c = std::toupper(static_cast<unsigned char>(c));

  if (cmd == "HELLO") {
    g_client_ips.push_back(client_ip);
    std::cout << "客户端IP注册: " << client_ip << std::endl;
    return build_route_table();
  } else if (cmd == "QUIT") {
    g_client_ips.remove(client_ip);
    std::cout << "客户端IP注销: " << client_ip << std::endl;
    return "BYE\r\n";
  } else {
    return "ERROR: Unknown command. Use HELLO or QUIT\r\n";
  }
}

// ==================== 主函数 ====================

int main() {
  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);

  // ===== 0. 打开 WiscKey LSM-Tree 数据库 (no_vlog=true) =====
  std::string db_path = "./diskv_master_db";

  leveldb::Options options;
  options.create_if_missing = true;
  options.no_vlog = true;
  options.write_buffer_size = 1 * 1024 * 1024;
  options.max_file_size = 2 * 1024 * 1024;

  leveldb::Status status = leveldb::DB::Open(options, db_path, &g_db);
  if (!status.ok()) {
    std::cerr << "Master: 无法打开 WiscKey LSM-Tree 数据库: " << status.ToString() << std::endl;
    return 1;
  }
  std::cout << "Master: WiscKey LSM-Tree 数据库已打开 (no_vlog=true), 路径: " << db_path << std::endl;
  std::cout << "  - 存储: key -> (vlog_number, offset, size)" << std::endl;
  std::cout << "  - 不生成 .log 文件，仅 .ldb / MANIFEST / CURRENT / LOG" << std::endl;

  // ===== 1. 创建监听套接字 =====

  int master_client_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (master_client_fd < 0) { std::cerr << "创建 master_client 套接字失败" << std::endl; delete g_db; return 1; }
  int opt_c = 1;
  setsockopt(master_client_fd, SOL_SOCKET, SO_REUSEADDR, &opt_c, sizeof(opt_c));

  struct sockaddr_in master_client_addr;
  memset(&master_client_addr, 0, sizeof(master_client_addr));
  master_client_addr.sin_family = AF_INET;
  master_client_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  master_client_addr.sin_port = htons(MASTER_CLIENT_PORT);

  if (bind(master_client_fd, (struct sockaddr*)&master_client_addr, sizeof(master_client_addr)) < 0) {
    std::cerr << "绑定 master_client 失败" << std::endl;
    close(master_client_fd); delete g_db; return 1;
  }
  if (listen(master_client_fd, 128) < 0) {
    std::cerr << "监听 client 端口失败" << std::endl;
    close(master_client_fd); delete g_db; return 1;
  }
  std::cout << "Master: 客户端监听 [127.0.0.1:" << MASTER_CLIENT_PORT << "]" << std::endl;

  int master_worker_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (master_worker_fd < 0) { std::cerr << "创建 master_worker 套接字失败" << std::endl; close(master_client_fd); delete g_db; return 1; }
  int opt_w = 1;
  setsockopt(master_worker_fd, SOL_SOCKET, SO_REUSEADDR, &opt_w, sizeof(opt_w));

  struct sockaddr_in master_worker_addr;
  memset(&master_worker_addr, 0, sizeof(master_worker_addr));
  master_worker_addr.sin_family = AF_INET;
  master_worker_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  master_worker_addr.sin_port = htons(MASTER_WORKER_PORT);

  if (bind(master_worker_fd, (struct sockaddr*)&master_worker_addr, sizeof(master_worker_addr)) < 0) {
    std::cerr << "绑定 master_worker 失败" << std::endl;
    close(master_worker_fd); close(master_client_fd); delete g_db; return 1;
  }
  if (listen(master_worker_fd, WORKER_COUNT) < 0) {
    std::cerr << "监听 worker 端口失败" << std::endl;
    close(master_worker_fd); close(master_client_fd); delete g_db; return 1;
  }
  std::cout << "Master: 从节点监听 [127.0.0.1:" << MASTER_WORKER_PORT << "]" << std::endl;

  // ===== 2. 等待所有从节点连接 =====
  std::cout << "Master: 等待 " << WORKER_COUNT << " 个从节点连接..." << std::endl;
  while (static_cast<int>(g_worker_fds.size()) < WORKER_COUNT) {
    if (!g_running) { close(master_worker_fd); close(master_client_fd); delete g_db; return 0; }
    struct sockaddr_in slave_addr;
    socklen_t slave_len = sizeof(slave_addr);
    int slave_fd = accept(master_worker_fd, (struct sockaddr*)&slave_addr, &slave_len);
    if (slave_fd < 0) { if (errno == EINTR) continue; std::cerr << "accept slave 失败" << std::endl; close(master_worker_fd); close(master_client_fd); delete g_db; return 1; }

    int worker_idx = static_cast<int>(g_worker_fds.size());
    g_worker_fds.push_back(slave_fd);
    g_worker_ids.push_back(worker_idx);

    char ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &slave_addr.sin_addr, ip, sizeof(ip));
    int port = ntohs(slave_addr.sin_port);
    std::cout << "从节点已连接: slave_fd=" << slave_fd << " worker_index=" << worker_idx << " socket=" << ip << ":" << port << std::endl;
  }
  std::cout << "Master: 全部 " << WORKER_COUNT << " 个从节点已连接" << std::endl;

  // ===== 3. 创建 epoll =====
  int epoll_fd = epoll_create1(0);
  if (epoll_fd < 0) {
    std::cerr << "创建 epoll 失败" << std::endl;
    for (int fd : g_worker_fds) close(fd);
    close(master_worker_fd); close(master_client_fd); delete g_db; return 1;
  }

  struct epoll_event ev;
  ev.events = EPOLLIN;
  ev.data.fd = master_client_fd;
  epoll_ctl(epoll_fd, EPOLL_CTL_ADD, master_client_fd, &ev);

  ev.events = EPOLLIN;
  ev.data.fd = master_worker_fd;
  epoll_ctl(epoll_fd, EPOLL_CTL_ADD, master_worker_fd, &ev);

  for (size_t i = 0; i < g_worker_fds.size(); ++i) {
    int wfd = g_worker_fds[i];
    set_nonblocking(wfd);
    Connection wconn;
    wconn.conn_type = CONN_WORKER;
    wconn.worker_id = static_cast<int>(i);
    connections[wfd] = wconn;
    ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
    ev.data.fd = wfd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, wfd, &ev) < 0) {
      std::cerr << "添加 worker_fd=" << wfd << " 到 epoll 失败" << std::endl;
    }
    std::cout << "  worker_fd=" << wfd << " 已加入 epoll 监视" << std::endl;
  }

  std::cout << "Master: 进入事件循环，等待请求..." << std::endl;

  // ===== 4. 事件循环 =====
  struct epoll_event events[MAX_EVENTS];
  while (g_running) {
    int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, 1000);
    if (nfds < 0) { if (!g_running) break; if (errno == EINTR) continue; std::cerr << "epoll_wait 错误" << std::endl; break; }

    for (int i = 0; i < nfds; ++i) {
      int fd = events[i].data.fd;

      if (fd == master_client_fd) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(master_client_fd, (struct sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) { if (errno == EAGAIN || errno == EWOULDBLOCK) continue; std::cerr << "accept 客户端失败" << std::endl; continue; }
        set_nonblocking(client_fd);

        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
        int client_port = ntohs(client_addr.sin_port);

        Connection conn;
        conn.client_ip = client_ip;
        conn.client_port = client_port;
        conn.conn_type = CONN_CLIENT;
        connections[client_fd] = conn;

        ev.events = EPOLLIN | EPOLLOUT | EPOLLET | EPOLLRDHUP;
        ev.data.fd = client_fd;
        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev) < 0) {
          std::cerr << "添加客户端 fd=" << client_fd << " 到 epoll 失败" << std::endl;
          close(client_fd); continue;
        }
        std::cout << "新客户端: fd=" << client_fd << " socket=" << client_ip << ":" << client_port << std::endl;
      }
      else if (fd == master_worker_fd) {
        struct sockaddr_in slave_addr;
        socklen_t slave_len = sizeof(slave_addr);
        int slave_fd = accept(master_worker_fd, (struct sockaddr*)&slave_addr, &slave_len);
        if (slave_fd < 0) { if (errno == EAGAIN || errno == EWOULDBLOCK) continue; std::cerr << "accept 从节点失败" << std::endl; continue; }
        set_nonblocking(slave_fd);

        int worker_idx = -1;
        for (size_t j = 0; j < g_worker_fds.size(); ++j) {
          if (g_worker_fds[j] < 0) { worker_idx = static_cast<int>(j); g_worker_fds[j] = slave_fd; break; }
        }
        if (worker_idx < 0) { worker_idx = static_cast<int>(g_worker_fds.size()); g_worker_fds.push_back(slave_fd); g_worker_ids.push_back(worker_idx); }

        Connection wconn;
        wconn.conn_type = CONN_WORKER;
        wconn.worker_id = worker_idx;
        connections[slave_fd] = wconn;

        ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
        ev.data.fd = slave_fd;
        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, slave_fd, &ev) < 0) {
          std::cerr << "添加重连从节点到 epoll 失败" << std::endl;
          close(slave_fd); continue;
        }
        std::cout << "从节点重连: fd=" << slave_fd << " worker_idx=" << worker_idx << std::endl;
      }
      else {
        bool closed = false;
        auto conn_it = connections.find(fd);
        if (conn_it == connections.end()) { close(fd); continue; }
        Connection& conn = conn_it->second;

        if (events[i].events & EPOLLRDHUP) {
          if (conn.conn_type == CONN_CLIENT) std::cout << "客户端断开: fd=" << fd << std::endl;
          else {
            std::cout << "从节点断开: fd=" << fd << " worker_id=" << conn.worker_id << std::endl;
            if (conn.worker_id >= 0 && conn.worker_id < static_cast<int>(g_worker_fds.size())) g_worker_fds[conn.worker_id] = -1;
          }
          closed = true;
        }
        if (!closed && (events[i].events & (EPOLLERR | EPOLLHUP))) { std::cerr << "fd=" << fd << " 异常" << std::endl; closed = true; }

        if (!closed && (events[i].events & EPOLLOUT)) {
          while (!conn.send_buffer.empty()) {
            int bw = write(fd, conn.send_buffer.data(), conn.send_buffer.size());
            if (bw < 0) { if (errno == EAGAIN || errno == EWOULDBLOCK) break; std::cerr << "写入错误 fd=" << fd << std::endl; closed = true; break; }
            else if (bw == 0) continue;
            else conn.send_buffer.erase(0, bw);
          }
          if (conn.send_buffer.empty()) {
            if (conn.close_after_send && conn.conn_type == CONN_CLIENT) closed = true;
            else { ev.events = EPOLLIN | EPOLLET | (conn.conn_type == CONN_CLIENT ? EPOLLRDHUP : 0); ev.data.fd = fd; epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev); }
          }
        }

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
                std::string response;
                if (conn.conn_type == CONN_CLIENT) {
                  response = process_client_command(line, conn.client_ip, conn.client_port);
                  if (!response.empty()) conn.close_after_send = true;
                } else {
                  response = process_worker_command(line, conn.worker_id);
                }
                if (!response.empty()) conn.send_buffer += response;
              }
            } else if (br == 0) {
              if (conn.conn_type == CONN_CLIENT) std::cout << "客户端关闭连接: fd=" << fd << std::endl;
              else { std::cout << "从节点关闭连接: fd=" << fd << " worker_id=" << conn.worker_id << std::endl; if (conn.worker_id >= 0 && conn.worker_id < static_cast<int>(g_worker_fds.size())) g_worker_fds[conn.worker_id] = -1; }
              closed = true; break;
            } else { if (errno == EAGAIN || errno == EWOULDBLOCK) break; std::cerr << "读取错误 fd=" << fd << std::endl; closed = true; break; }
          }
          if (!closed && !conn.send_buffer.empty()) {
            ev.events = EPOLLIN | EPOLLOUT | EPOLLET | (conn.conn_type == CONN_CLIENT ? EPOLLRDHUP : 0);
            ev.data.fd = fd; epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev);
          }
        }

        if (closed) { epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr); close(fd); connections.erase(fd); }
      }
    }
  }

  // ===== 5. 安全关闭 =====
  std::cout << "Master: 正在关闭..." << std::endl;
  close(epoll_fd);
  for (int fd : g_worker_fds) if (fd >= 0) close(fd);
  close(master_worker_fd);
  close(master_client_fd);
  std::cout << "Master: 正在关闭 LSM-Tree 数据库..." << std::endl;
  delete g_db;
  g_db = nullptr;
  std::cout << "Master: 已安全退出" << std::endl;
  return 0;
}
