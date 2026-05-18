# DisKV 分布式操作流程图

本文档描述 DisKV 分布式 KV 系统中 HELLO / PUT / GET / QUIT 四种操作的完整数据流，包括节点间的网络通信顺序和节点内部的函数调用链与全局数据结构变化。

---

## 1. HELLO — 获取路由表

```
┌────────┐                  ┌────────────┐                  ┌─────────┐
│ Client │                  │   Master   │                  │ Slave N │
│ (8888) │                  │  (8888)    │                  │(9000+N) │
└───┬────┘                  └─────┬──────┘                  └─────────┘
    │                             │
    │ ① connect(127.0.0.1:8888)  │
    │ ──────────────────────────▶ │
    │                             │  accept() → client_fd=N
    │                             │  set_nonblocking(client_fd)
    │                             │  connections[N] = {CONN_CLIENT, close_after_send=false}
    │                             │  epoll_ctl(ADD, client_fd, EPOLLIN|EPOLLOUT|ET|RDHUP)
    │                             │
    │ ② write("HELLO\n")         │
    │ ──────────────────────────▶ │
    │                             │  EPOLLIN → read() → recv_buffer += "HELLO\n"
    │                             │  recv_buffer.find('\n') → line = "HELLO"
    │                             │
    │                             │  process_client_command("HELLO", ip, port):
    │                             │    g_client_ips.push_back(ip)     ← 注册客户端 IP
    │                             │    return build_route_table()     ← 构建路由表字符串
    │                             │      └─ 遍历 WORKER_RANGES[] + SLAVE_CLIENT_PORTS[]
    │                             │
    │                             │  conn.close_after_send = true    ← 发送后主动断开
    │                             │  conn.send_buffer += response    ← 加入发送缓冲区
    │                             │
    │                             │  EPOLLOUT → write() → 发送路由表
    │                             │  send_buffer 清空 → close(client_fd)  ← 主动断开
    │                             │  connections.erase(client_fd)
    │                             │
    │ ③ read() ← ROUTETABLE...   │
    │ ◀────────────────────────── │
    │ parse_route_table(response) │
    │   → route_table = [         │
    │     {0, 127.0.0.1:9000, 0-42},
    │     {1, 127.0.0.1:9001, 43-85},
    │     {2, 127.0.0.1:9002, 86-127}
    │   ]
    │                             │
    ▼                             ▼
 Client 断开与 Master 的连接，持有路由表，后续直连 Slave
```

**关键数据结构变化**：

| 步骤 | 组件 | 变化 |
|------|------|------|
| ① | Master `connections` | 新增 `{client_fd → Connection}` |
| ① | Master `epoll_fd` | 注册 client_fd 监听 |
| ② | Master `g_client_ips` | 新增客户端 IP 地址 |
| ② | Master `send_buffer` | 填入路由表字符串 |
| ② | Master `close_after_send` | 设为 true（发送完毕主动断开） |
| ③ | Client `route_table` | 填充 3 条路由条目 |
| ③ | Client `slave_fds` | 保持为空（延迟连接） |

---

## 2. PUT — 写入键值对

```
┌────────┐            ┌────────┐            ┌────────────┐
│ Client │            │ Slave  │            │   Master   │
└───┬────┘            │ (Vlog) │            │ (LSM-Tree) │
    │                  └───┬────┘            └─────┬──────┘
    │                      │                       │
    │ ① find_worker_for_key(key)                   │
    │   → worker_idx = 0 (ASCII '1'=49 ∈ [0,42])  │
    │   get_slave_fd(0) → connect(127.0.0.1:9000)  │
    │   slave_fds[0] = fd                          │
    │                      │                       │
    │ ② write("PUT 1 Hello\n")                     │
    │ ────────────────────▶ │                       │
    │                      │ EPOLLIN → read        │
    │                      │ process_command("PUT 1 Hello"):
    │                      │                       │
    │                      │ ③ write_value_to_vlog(key="1", value="Hello"):
    │                      │   pthread_mutex_lock(&g_vlog_mutex)
    │                      │   g_vlog_manager->GetWritePos()  ← 获取当前写入位置
    │                      │   encode_vlog_entry("1","Hello") → raw_data
    │                      │     └─ kTypeValue|Varint32(1)|"1"|Varint32(5)|"Hello"
    │                      │   data_offset = write_pos + 12   ← 计算偏移
    │                      │   g_vlog_manager->AddRecord(raw_data) ← 追加写入 vlog
    │                      │   pthread_mutex_unlock(&g_vlog_mutex)
    │                      │   addr = Varint64(vlog_numb)|Varint64(offset)|Varint64(size)
    │                      │                       │
    │                      │ ④ send_to_master("PUT_ADDR 1 1 12 28")
    │                      │   pthread_mutex_lock(&g_master_mutex)
    │                      │   write(g_master_fd, "PUT_ADDR 1 1 12 28\n")
    │                      │   ───────────────────▶│
    │                      │                       │ EPOLLIN → read
    │                      │                       │ process_worker_command("PUT_ADDR 1 1 12 28", 0):
    │                      │                       │   addr = Varint64(1)|Varint64(12)|Varint64(28)
    │                      │                       │   g_db->PutAddress(write_opts, "1", addr)
    │                      │                       │     └─ DBImpl::PutAddress():
    │                      │                       │       mutex_.Lock()
    │                      │                       │       MakeRoomForWrite(false) ← 确保 memtable 空间
    │                      │                       │         └─ 若 mem_ 满 → imm_=mem_, 新建 mem_, 调度 Compaction
    │                      │                       │       seq = LastSequence + 1
    │                      │                       │       mem_->Add(seq, kTypeValue, "1", addr) ← 插入 memtable
    │                      │                       │       SetLastSequence(seq)
    │                      │                       │       mutex_.Unlock()
    │                      │                       │   return "STORED\r\n"
    │                      │   ◀───────────────────│
    │                      │   read() ← "STORED\n"
    │                      │   pthread_mutex_unlock(&g_master_mutex)
    │                      │                       │
    │                      │ ⑤ save_vlog_meta()   ← 持久化写入位置到 VLOG_META
    │                      │   ofstream ← "vlog_number: 1\nwrite_pos: 40\n"
    │                      │                       │
    │                      │ send_buffer += "STORED\r\n"
    │                      │ EPOLLOUT → write     │
    │ ⑥ read() ← "STORED"  │                       │
    │ ◀──────────────────── │                       │
    ▼                      ▼                       ▼
```

**关键数据结构变化**：

| 步骤 | 组件 | 变化 |
|------|------|------|
| ① | Client `slave_fds` | 新增 `{0 → fd}` (TCP 连接) |
| ③ | Slave `g_vlog_manager` | VlogInfo::buffer_ 追加 raw_data, size_ 增加 |
| ③ | Slave `VLOG_META` | 更新 vlog_number 和 write_pos |
| ④ | Master `g_db->mem_` | memtable 新增 `("1"@seq, addr)` 条目 |
| ④ | Master `LastSequence` | 自增 1 |

---

## 3. GET — 读取键值对

```
┌────────┐            ┌────────┐            ┌────────────┐
│ Client │            │ Slave  │            │   Master   │
└───┬────┘            │ (Vlog) │            │ (LSM-Tree) │
    │                  └───┬────┘            └─────┬──────┘
    │                      │                       │
    │ ① find_worker_for_key("1")                   │
    │   → worker_idx = 0                           │
    │   get_slave_fd(0) → 复用已有连接              │
    │                      │                       │
    │ ② write("GET 1\n")   │                       │
    │ ────────────────────▶ │                       │
    │                      │ EPOLLIN → read        │
    │                      │ process_command("GET 1"):
    │                      │                       │
    │                      │ ③ send_to_master("GET_ADDR 1")
    │                      │   pthread_mutex_lock(&g_master_mutex)
    │                      │   write(g_master_fd, "GET_ADDR 1\n")
    │                      │   ───────────────────▶│
    │                      │                       │ EPOLLIN → read
    │                      │                       │ process_worker_command("GET_ADDR 1", 0):
    │                      │                       │   g_db->GetAddress(ReadOptions(), "1", &addr_str)
    │                      │                       │     └─ DBImpl::GetAddress():
    │                      │                       │       mutex_.Lock()
    │                      │                       │       snapshot = LastSequence
    │                      │                       │       lkey = LookupKey("1", snapshot)
    │                      │                       │       mutex_.Unlock()
    │                      │                       │       ★ 在 mem/imm/SSTable 中查找:
    │                      │                       │       if mem_->Get(lkey, &addr, &s)         ← 先查 memtable
    │                      │                       │       else if imm_->Get(lkey, &addr, &s)    ← 再查 immutable memtable
    │                      │                       │       else current->Get(options,lkey,&addr)  ← 最后查 SSTable
    │                      │                       │       ★ 不调用 FetchValueFromVlog！
    │                      │                       │       mutex_.Lock()
    │                      │                       │       若找到 → addr_str 即为编码的 vlog 地址
    │                      │                       │       UpdateStats() → MaybeScheduleCompaction()
    │                      │                       │   return "ADDR 1 12 28\r\n"
    │                      │   ◀───────────────────│
    │                      │   read() ← "ADDR 1 12 28\n"
    │                      │   pthread_mutex_unlock(&g_master_mutex)
    │                      │                       │
    │                      │ ④ 解析: vlog=1, offset=12, size=28
    │                      │   encoded_addr = Varint64(1)|Varint64(12)|Varint64(28)
    │                      │   pthread_mutex_lock(&g_vlog_mutex)
    │                      │   g_vlog_manager->FetchValueFromVlog(encoded_addr, &value)
    │                      │     └─ VlogFetcher::Get(offset=12, size=28, &value):
    │                      │       file_->Read(offset=12, size=28) → raw_bytes
    │                      │       Parse(&raw_bytes, &value):
    │                      │         assert(raw[0] == kTypeValue)  ← 验证 tag
    │                      │         raw.remove_prefix(1)          ← 跳过 tag
    │                      │         GetLengthPrefixedSlice(&raw, &key)    ← 读取 key
    │                      │         GetLengthPrefixedSlice(&raw, &value)  ← 读取 value → "Hello"
    │                      │   pthread_mutex_unlock(&g_vlog_mutex)
    │                      │                       │
    │                      │ send_buffer += "VALUE Hello\r\n"
    │                      │ EPOLLOUT → write     │
    │ ⑤ read() ← "VALUE Hello"                    │
    │ ◀──────────────────── │                       │
    ▼                      ▼                       ▼
```

**关键数据结构变化**：

| 步骤 | 组件 | 变化 |
|------|------|------|
| ③ | Master `snapshot` | 记录当前 LastSequence（保证一致性读） |
| ③ | Master `lkey` | 构建 `LookupKey("1", snapshot)` 用于查询 |
| ③ | Master `mutex_` | Unlock → 查询 → Lock（避免长时间持锁） |
| ④ | Slave `g_vlog_mutex` | Lock → FetchValueFromVlog → Unlock |
| ④ | Slave `VlogFetcher` | 调用 `RandomAccessFile::Read(offset, size)` |
| ④ | Slave `Parse()` | 解码 vlog 记录，剥离 tag+key，返回纯 value |

---

## 4. QUIT — 客户端退出

```
┌────────┐                  ┌────────────┐
│ Client │                  │   Master   │
└───┬────┘                  └─────┬──────┘
    │                             │
    │ ① connect(127.0.0.1:8888)  │
    │ ──────────────────────────▶ │
    │                             │  accept() → client_fd
    │                             │  connections[client_fd] = {CONN_CLIENT}
    │                             │
    │ ② write("QUIT\n")          │
    │ ──────────────────────────▶ │
    │                             │  EPOLLIN → read → recv_buffer += "QUIT\n"
    │                             │  recv_buffer.find('\n') → line = "QUIT"
    │                             │
    │                             │  process_client_command("QUIT", ip, port):
    │                             │    g_client_ips.remove(ip)     ← 注销客户端 IP
    │                             │    return "BYE\r\n"             ← 生成响应
    │                             │
    │                             │  conn.close_after_send = true ← 发送后主动断开
    │                             │  conn.send_buffer += "BYE\r\n"
    │                             │
    │                             │  EPOLLOUT → write → "BYE\r\n"
    │                             │  send_buffer 清空 → close(client_fd)
    │                             │  connections.erase(client_fd)
    │                             │
    │ ③ read() ← "BYE"           │
    │ ◀────────────────────────── │
    │                             │
    │ ④ close(所有 slave 连接)      │
    │   for each slave_fds:       │
    │    close(fd)                │
    │   slave_fds.clear()         │
    │   return 0                  │
    ▼                             ▼
```

**关键数据结构变化**：

| 步骤 | 组件 | 变化 |
|------|------|------|
| ② | Master `g_client_ips` | 移除该客户端 IP |
| ② | Master `close_after_send` | 设为 true（发送 BYE 后主动断开） |
| ③ | Client `slave_fds` | 遍历关闭所有 fd，清空映射表 |

---

## 完整数据流架构图

```
                      ┌──────────────────────────────┐
                      │         Master :8888         │
                      │   ┌──────────────────────┐   │
   Client             │   │  WiscKey LSM-Tree    │   │
   ──────             │   │  (no_vlog=true)      │   │
  │HELLO│─────────────│──▶│  ┌───────────────┐   │   │
  │QUIT │◀────────────│───│  │ memtable      │   │   │
   ──────             │   │  │  key→addr     │   │   │
                      │   │  └───────┬───────┘   │   │
                      │   │          │ Compaction│   │
                      │   │  ┌───────▼───────┐   │   │
                      │   │  │ SSTable(.ldb) │   │   │
                      │   │  └───────────────┘   │   │
                      │   └──────────────────────┘   │
                      │                              │
                      │   Port 8889: Worker 0        │
                      │   Port 8890: Worker 1        │
                      │   Port 8891: Worker 2        │
                      └──┬────────┬────────┬─────────┘
                         │        │        │
              内部协议   │        │        │
           PUT_ADDR     │        │        │
           GET_ADDR     │        │        │
           DELETE_ADDR  │        │        │
           SCAN_ADDR    │        │        │
           UPDATE_ADDR  │        │        │
                         │        │        │
    ┌────────────────────┼────────┼────────┼──────────────────┐
    │                    ▼        ▼        ▼                   │
    │  ┌────────────┐ ┌────────────┐ ┌────────────┐           │
    │  │  Slave 0   │ │  Slave 1   │ │  Slave 2   │           │
    │  │  :9000     │ │  :9001     │ │  :9002     │           │
    │  │            │ │            │ │            │           │
    │  │ ┌────────┐ │ │ ┌────────┐ │ │ ┌────────┐ │           │
    │  │ │Vlog Mgr│ │ │ │Vlog Mgr│ │ │ │Vlog Mgr│ │           │
    │  │ │┌──────┐│ │ │ │┌──────┐│ │ │ │┌──────┐│ │           │
    │  │ ││ .log ││ │ │ ││ .log ││ │ │ ││ .log ││ │           │
    │  │ │├──────┤│ │ │ │├──────┤│ │ │ │├──────┤│ │           │
    │  │ ││ .log ││ │ │ ││ .log ││ │ │ ││ .log ││ │           │
    │  │ │└──────┘│ │ │ │└──────┘│ │ │ │└──────┘│ │           │
    │  │ │VLOG_META│ │ │ │VLOG_META│ │ │ │VLOG_META│ │         │
    │  │ └────────┘ │ │ └────────┘ │ │ └────────┘ │           │
    │  │            │ │            │ │            │           │
    │  │ ┌────────┐ │ │ ┌────────┐ │ │ ┌────────┐ │           │
    │  │ │GC Thread│ │ │ │GC Thread│ │ │ │GC Thread│ │         │
    │  │ └────────┘ │ │ └────────┘ │ │ └────────┘ │           │
    │  └────┬───────┘ └────┬───────┘ └────┬───────┘           │
    │       │              │              │                    │
    │       │    Client 直连从节点 (PUT/GET/DELETE/SCAN)        │
    │       └──────────────┼──────────────┘                    │
    │                      │                                   │
    │              ┌───────▼───────┐                            │
    │              │    Client     │                            │
    │              │ route_table   │                            │
    │              │ slave_fds[0..N]│                           │
    │              └───────────────┘                            │
    └──────────────────────────────────────────────────────────┘
```
