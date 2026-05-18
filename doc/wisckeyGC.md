# WiscKey Vlog 编码与垃圾回收 (GC) 分析

> 本文档基于对 Wisckey 工程源代码的全面分析，聚焦于 vlog 记录的磁盘编码格式、
> 垃圾回收（Garbage Collection）的触发判断与执行机制。

---

## 1. 架构概览

Wisckey 采用**键值分离**（Key-Value Separation）设计：

```
写入路径：
  Key + Value  →  [完整 WriteBatch 写入 Vlog]  →  vlog 文件 (.log)
       ↓
  Key + Address →  [存入 MemTable / LSM-Tree]   →  SSTable 文件 (.ldb)

读取路径：
  Key  →  [查询 LSM-Tree]  →  获取 Address (vlog_number, offset, size)
       →  [VlogFetcher]   →  从 vlog 文件中随机读取，解析出 Value
```

核心模块及文件位置：

| 模块 | 文件 | 职责 |
|------|------|------|
| `VlogManager` | `db/vlog_manager.h/.cc` | 管理所有 vlog 文件生命周期，协调读写与 GC |
| `VlogInfo` | `db/vlog_manager.h` | 单个 vlog 文件的元数据（缓冲区、读写指针、垃圾计数） |
| `VWriter` | `db/vlog_writer.h/.cc` | 向当前 vlog 文件追加记录 |
| `VReader` | `db/vlog_reader.h/.cc` | 顺序读取 vlog 文件中的记录 |
| `VlogFetcher` | `db/vlog_fetcher.h/.cc` | 根据地址随机读取 vlog 中的值 |
| `VersionEdit` | `db/version_edit.h/.cc` | 持久化 vlog 头/尾位置到 MANIFEST |

---

## 2. Vlog 记录编码格式

### 2.1 整体结构

每条 vlog 记录由 **12 字节定长 Header** + **变长 Body** 组成：

```
┌──────────────────────┬─────────────────────────────────────────┐
│    Header (12 B)     │              Body (变长)                 │
├──────────┬───────────┼─────────────────────────────────────────┤
│ CRC32    │ Length    │      WriteBatch 序列化内容                │
│ (4 B)    │ (8 B)     │   (包含键值对的 LevelDB 内部编码)         │
└──────────┴───────────┴─────────────────────────────────────────┘
```

**Header 定义**（`db/vlog_manager.h:16`）：

```cpp
static const int kVHeaderSize = 4 + 8;
// checksum: 4 bytes (CRC32C, Masked 格式)
// length:   8 bytes (fixed64, Body 的字节长度)
```

### 2.2 Header 写入逻辑

`db/vlog_writer.cc:26-30`：

```cpp
Status VWriter::AddRecord(const Slice& slice) {
    const char* ptr = slice.data();
    size_t left = slice.size();
    char head[kVHeaderSize];
    uint32_t crc = crc32c::Extend(0, ptr, left);
    crc = crc32c::Mask(crc);           // 调整 CRC 用于存储 (避免全零)
    EncodeFixed32(head, crc);          // 前 4 字节: CRC32C
    EncodeFixed64(&head[4], left);     // 后 8 字节: 长度
    // ...
}
```

### 2.3 Body 内容：WriteBatch 编码

Body 是一整块 `WriteBatch` 的序列化内容。WriteBatch 的内部格式为：

```
┌──────────────────┬──────────────────┬───────────────────────────────────┐
│  Sequence (8B)   │  Count (4B)      │  Record[0] ... Record[Count-1]     │
│  fixed64         │  fixed32         │  (每条记录 = Tag + Key + Value)     │
└──────────────────┴──────────────────┴───────────────────────────────────┘
```

每条 Record 格式：

```
Tag (1B) | Varint32(len_key) | Key | Varint32(len_value) | Value

Tag 取值:
  kTypeValue    = 1  → Put(key, value)
  kTypeDeletion = 0  → Delete(key)  (value 为空)
```

### 2.4 写入缓冲机制

VlogWriter 使用 4KB 写缓冲区（`WriteBufferSize = 1 << 12`）：

- 数据先写入 `VlogInfo::buffer_[]` 内存缓冲区
- 当缓冲区剩余空间不足以容纳新记录时，调用 `SyncedAppend` 刷盘
- 若单条记录超过缓冲区大小，直接绕过缓冲区写盘

### 2.5 与 LevelDB WAL 格式的区别

| 特性 | LevelDB WAL (log_format.h) | Wisckey Vlog (vlog_manager.h) |
|------|---------------------------|-------------------------------|
| Header 大小 | 7 B (CRC 4B + Len 2B + Type 1B) | **12 B** (CRC 4B + Len 8B) |
| 长度字段 | 2 字节 (max 32KB) | **8 字节** (支持大记录) |
| 块分片 | 支持 32KB 块分片 (kFirstType/kMiddleType/kLastType) | **不分片**，每条记录完整写入 |
| 用途 | 崩溃恢复 (WAL) | 值存储 (Value-Log) |

---

## 3. Vlog 地址格式

LSM-Tree 中存储的值并非原始 value，而是 **vlog 地址**，格式为三个连续的 Varint64：

```
Address = Varint64(vlog_file_number) | Varint64(offset) | Varint64(size)
```

**写入过程**（`db/write_batch.cc:100-115` 的 `InsertAddressInto`）：

```cpp
// key 的 value 被替换为 vlog 地址
address.clear();
PutVarint64(&address, vlog_number);   // 哪个 vlog 文件
PutVarint64(&address, *vlog_head);    // 文件内偏移
PutVarint64(&address, size);          // 记录大小
handler->Put(key, address);           // 存入 MemTable / LSM-Tree
```

**读取过程**（`db/vlog_manager.cc:52-70` 的 `FetchValueFromVlog`）：

```cpp
// 从地址中解析出三个字段
GetVarint64(&addr, &file_numb);   // vlog 文件号
GetVarint64(&addr, &offset);      // 偏移
GetVarint64(&addr, &size);        // 大小
// 然后通过 VlogFetcher 从 vlog 文件中随机读取
cache->Get(offset, size, value);
```

**值解析**（`db/vlog_fetcher.cc:13-22`）：

```cpp
inline Status Parse(Slice* r, std::string* value) {
    Slice k, v;
    assert((*r)[0] == kTypeValue);
    r->remove_prefix(1);                              // 跳过 Tag
    if (GetLengthPrefixedSlice(r, &k) &&              // 读取 Key
        GetLengthPrefixedSlice(r, &v)) {              // 读取 Value
        value->assign(v.data(), v.size());
        return Status::OK();
    }
    return Status::Corruption("failed to decode value from vlog");
}
```

---

## 4. 垃圾回收 (GC) 机制

### 4.1 GC 核心数据结构

#### VlogInfo — 单文件垃圾跟踪

`db/vlog_manager.h:20-36`：

```cpp
class VlogInfo {
    char buffer_[WriteBufferSize];   // 4KB 写缓冲区
    size_t size_;                    // 缓冲区已用大小
    VlogFetcher* vlog_fetch_;        // 随机读访问器
    VWriter* vlog_write_;            // 追加写访问器
    size_t head_;                    // 当前 vlog 文件写入位置
    uint64_t count_;                 // ⚠️ 该 vlog 文件中的垃圾 KV 条数
    port::SharedMutex* rwlock_;      // 读写锁
};
```

#### VlogManager — GC 全局管理

`db/vlog_manager.h:42-66`：

```cpp
class VlogManager {
    std::map<uint64_t, VlogInfo*> manager_;        // vlog 文件号 → VlogInfo
    std::set<uint64_t> cleaning_vlog_set_;         // ⚠️ 正在 GC 的 vlog 集合
    uint64_t clean_threshold_;                     // GC 触发阈值
    uint64_t cur_vlog_;                            // 当前活跃 vlog 编号
};
```

#### VersionEdit — vlog 头尾位置持久化

`db/version_edit.h:110-126`：

```cpp
bool has_head_info_;
bool has_tail_info_;

uint64_t head_info_;          // vlog 头部位置 (写入点)
uint64_t tail_info_;          // vlog 尾部位置 (GC 读取起始点)
uint64_t tail_vlog_number_;   // 尾部所在的 vlog 文件号

bool has_vlog_info_;
std::string vlog_info_;       // vlog 垃圾统计等元数据
```

这些信息通过 MANIFEST 文件持久化。对应的 MANIFEST Tag：

```cpp
enum Tag {
    // ...
    kHead      = 10,   // PutVarint64(head_info_)
    kVlogInfo  = 11,   // PutLengthPrefixedSlice(vlog_info_)
    kTail      = 12,   // PutVarint64(tail_vlog_number_) + PutVarint64(tail_info_)
};
```

### 4.2 GC 配置参数

`include/leveldb/options.h:147-157`：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `clean_write_buffer_size` | `128 << 10` (128KB) | GC 写缓冲区，必须 > 12 字节 |
| `clean_threshold` | `1000` | vlog 文件垃圾记录条数达到此值时触发 GC |
| `min_clean_threshold` | `clean_threshold / 2` (500) | 手动清理时，只有垃圾数 ≥ 此值才执行 |
| `log_dropCount_threshold` | `100` | 新增此数量的垃圾后持久化 vlog info 到 MANIFEST |
| `max_vlog_size` | `1 << 30` (1GB) | 单个 vlog 文件大小上限，超限后创建新文件 |

### 4.3 GC 触发的设计意图

根据代码注释和数据结构设计，GC 的触发和判断逻辑应为：

```
触发条件: VlogInfo::count_ >= clean_threshold_

判断位置: 应在 BackgroundCompaction() 循环中或独立的 GC 后台线程中

触发后动作:
  1. 将该 vlog 文件号加入 cleaning_vlog_set_
  2. 启动 VlogFetcher 从 tail_info_ 处读取旧值
  3. 对每条记录查询 LSM-Tree 判断有效性
  4. 将有效值重写到 head_info_
  5. 调用 DeallocateDiskSpace 回收空间
  6. 更新 tail_info_ / head_info_ 并持久化到 MANIFEST
```

### 4.4 当前实现状态 ⚠️

**GC 核心执行逻辑在本版本中尚未完整实现。** 以下是各项组件的实现状态：

| 组件 | 位置 | 状态 | 说明 |
|------|------|------|------|
| `VlogInfo::count_` (垃圾计数) | `vlog_manager.h:30` | ❌ **未递增** | 仅初始化为 0，从未有代码对其递增 |
| `clean_threshold_` (触发阈值) | `vlog_manager.h:64` | ❌ **未被比较** | 仅在 `VlogManager` 构造函数中保存 |
| `cleaning_vlog_set_` (GC 进行中集合) | `vlog_manager.h:63` | ❌ **未使用** | 声明后无任何读写操作 |
| `head_info_` / `tail_info_` (头尾指针) | `version_edit.h:115-122` | ✅ **已持久化** | 编码/解码逻辑完整，MANIFEST 可存取 |
| `vlog_info_` (垃圾统计元数据) | `version_edit.h:127-128` | ✅ **已持久化** | 编码/解码逻辑完整，但内容未被实际填充 |
| `VReader::DeallocateDiskSpace()` | `vlog_reader.cc:156-158` | ❌ **未被调用** | 函数已定义，可释放磁盘空间，但无调用者 |
| `VlogFetcher` (随机读取器) | `vlog_fetcher.h/.cc` | ✅ **已实现** | 用于 Get 请求的值查找，但无 GC 使用 |
| vlog 文件轮转 | `db_impl.cc:1351-1358` | ✅ **已实现** | 当 `vlog_head_ >= max_vlog_size` 时创建新 vlog 文件 |

**当前版本中唯一存在的"空间管理"行为是 vlog 文件轮转：**

```cpp
// db_impl.cc MakeRoomForWrite():
if (vlog_head_ >= options_.max_vlog_size) {
    uint32_t new_log_number = versions_->NewVlogNumber();
    vlog_head_ = 0;
    vlogfile_number_ = new_log_number;
    vlog_manager_.AddVlog(dbname_, options_, new_log_number);
}
```

当写入位置超过 `max_vlog_size`（默认 1GB）时，创建新的 vlog 文件，但**旧 vlog 文件中的垃圾数据永不回收**。

### 4.5 GC 的预期执行流程

根据 `version_edit.h:117-121` 中的官方注释，GC 的设计流程为：

```
During garbage collection, WiscKey first reads a chunk of key-value pairs
(e.g., several MBs) from the tail of the vLog, then finds which of those
values are valid (not yet overwritten or deleted) by querying the LSM-tree.
WiscKey then appends valid values back to the head of the vLog.
```

**详细步骤推演：**

```
┌─ 1. 判断触发 ─────────────────────────────────────────────┐
│  VlogInfo::count_ >= clean_threshold_                     │
│  → 将该 vlog 号加入 cleaning_vlog_set_                      │
│  → 从 MANIFEST 读取该 vlog 的 tail_info_ 起始位置           │
└──────────────────────────────────────────────────────────┘
                          ↓
┌─ 2. 从尾部读取 ───────────────────────────────────────────┐
│  VReader(file, tail_info_) 从 tail 位置顺序读取一批记录    │
│  每条记录 = 一个完整的 WriteBatch                          │
│  WriteBatch 中包含若干 key-value 对                         │
└──────────────────────────────────────────────────────────┘
                          ↓
┌─ 3. 有效性检查 (核心逻辑) ────────────────────────────────┐
│  for each key in batch:                                   │
│      current_addr = LSM-Tree.Get(key)  // 查询当前地址    │
│      if current_addr == (vlog_number, offset, size):      │
│          → 有效 (未被覆盖/删除)                            │
│          → 追加到 GC 写缓冲区                              │
│      else:                                                │
│          → 垃圾 (已被覆盖或删除)                           │
│          → count_-- (或跳过)                               │
└──────────────────────────────────────────────────────────┘
                          ↓
┌─ 4. 重写有效值 ───────────────────────────────────────────┐
│  将 GC 写缓冲区的内容通过 VWriter 追加到当前 vlog 头部      │
│  (head_info_ 位置)                                        │
│  同时更新 LSM-Tree 中该 key 的地址指向新位置               │
└──────────────────────────────────────────────────────────┘
                          ↓
┌─ 5. 空间回收 ─────────────────────────────────────────────┐
│  VReader::DeallocateDiskSpace(offset, length)             │
│  → 底层调用 fallocate(FALLOC_FL_PUNCH_HOLE) 释放磁盘空间   │
│  更新 tail_info_ 指向新的尾部位置                          │
└──────────────────────────────────────────────────────────┘
                          ↓
┌─ 6. 持久化 ───────────────────────────────────────────────┐
│  VersionEdit::SetVlogHeadPos(new_head)                    │
│  VersionEdit::SetVlogTailPos(vlog_number, new_tail)       │
│  VersionEdit::SetVlogInfo(垃圾统计信息)                   │
│  → 通过 VersionSet::LogAndApply 写入 MANIFEST             │
└──────────────────────────────────────────────────────────┘
```

---

## 5. 垃圾产生的路径

在 Wisckey 中，垃圾在以下两种情况下产生：

### 5.1 覆盖写 (Overwrite)

```
时刻 T1: PUT key="foo", value="old_value"
  → vlog 写入: [foo, old_value]  // vlog offset = 100
  → LSM-Tree:   foo → (vlog=1, offset=100, size=30)

时刻 T2: PUT key="foo", value="new_value"
  → vlog 写入: [foo, new_value]  // vlog offset = 150
  → LSM-Tree:   foo → (vlog=1, offset=150, size=30)
  → vlog offset 100 处的 old_value 成为 **垃圾**
```

### 5.2 删除 (Delete)

```
时刻 T1: PUT key="bar", value="some_value"
  → vlog 写入: [bar, some_value]  // vlog offset = 200
  → LSM-Tree:   bar → (vlog=1, offset=200, size=30)

时刻 T2: DELETE key="bar"
  → vlog 写入: [bar, kTypeDeletion]  // vlog offset = 250
  → LSM-Tree:   bar → 删除标记
  → vlog offset 200 处的 some_value 成为 **垃圾**
```

### 5.3 LSM-Tree Compaction 中的垃圾丢弃

LSM-Tree Compaction (`DoCompactionWork`) 会丢弃旧版本的键条目和删除标记，但这**只减小 SSTable 的体积，不影响 vlog 文件大小**。vlog 中的旧值数据需要通过独立的 vlog GC 流程来回收。

---

## 6. 关键代码索引

| 功能 | 文件 | 关键函数/行号 |
|------|------|---------------|
| vlog 记录编码写入 | `db/vlog_writer.cc:25-55` | `VWriter::AddRecord()` |
| vlog 记录解码读取 | `db/vlog_reader.cc:40-150` | `VReader::ReadRecord()` |
| vlog 地址编码 | `db/write_batch.cc:100-115` | `InsertAddressInto()` |
| vlog 值获取 | `db/vlog_fetcher.cc:24-53` | `VlogFetcher::Get()` |
| vlog 地址解析 | `db/vlog_manager.cc:52-70` | `FetchValueFromVlog()` |
| vlog 文件创建/切换 | `db/vlog_manager.cc:30-47` | `VlogManager::AddVlog()` |
| vlog 文件轮转触发 | `db/db_impl.cc:1351-1358` | `MakeRoomForWrite()` |
| vlog head 设置 | `db/vlog_manager.cc:87-94` | `VlogManager::SetHead()` |
| vlog 读取过程 | `db/db_impl.cc:437-484` | `RecoverLogFile()` |
| 写入路径(主) | `db/db_impl.cc:1244-1262` | `DBImpl::Write()` |
| 读取路径(主) | `db/db_impl.cc:1113-1160` | `DBImpl::Get()` |
| MANIFEST head 编码 | `db/version_edit.cc:74-77` | `EncodeTo()` kHead |
| MANIFEST tail 编码 | `db/version_edit.cc:78-82` | `EncodeTo()` kTail |
| MANIFEST vlog_info 编码 | `db/version_edit.cc:83-85` | `EncodeTo()` kVlogInfo |
| MANIFEST head 解码 | `db/version_edit.cc:223-224` | `DecodeFrom()` kHead |
| MANIFEST tail 解码 | `db/version_edit.cc:229-233` | `DecodeFrom()` kTail |
| GC 注释/设计文档 | `db/version_edit.h:117-121` | tail_info_ 注释 |
| 磁盘空间释放 API | `db/vlog_reader.cc:156-158` | `DeallocateDiskSpace()` |
| 配置选项定义 | `include/leveldb/options.h:147-157` | GC 相关 Options |

---

## 7. 总结

| 维度 | 状态 |
|------|------|
| **vlog 读写路径** | ✅ 完整实现 — 键值分离存储可以正常工作 |
| **vlog 记录编码** | ✅ 完整定义 — 12B Header(CRC+Len) + WriteBatch Body |
| **vlog 地址格式** | ✅ 完整实现 — Varint(vlog_number, offset, size) |
| **MANIFEST 持久化** | ✅ 头尾位置、vlog_info 的编解码逻辑完整 |
| **vlog 文件轮转** | ✅ 已实现 — 按 max_vlog_size 切换新文件 |
| **GC 垃圾计数** | ❌ 未递增 — `count_` 始终为 0 |
| **GC 触发判断** | ❌ 未实现 — `clean_threshold_` 未被比较 |
| **GC 执行逻辑** | ❌ 未实现 — 无后台 GC 循环 |
| **空间回收** | ❌ 未实现 — `DeallocateDiskSpace` 无调用者 |

**结论：** 本项目当前版本中，vlog 文件会随着写入持续增长，其中被覆盖和删除的旧值所占用的磁盘空间不会被自动回收。要实现完整的 GC 功能，需要补充以下核心逻辑：

1. **垃圾计数** — 在覆盖写和删除时递增 `VlogInfo::count_`
2. **GC 触发判断** — 在后台线程中监控 `count_ >= clean_threshold_`
3. **GC 执行循环** — 实现尾部读取 → LSM-Tree 有效性检查 → 头部重写 → 空间释放
4. **MANIFEST 同步** — GC 完成后将新的 head/tail/vlog_info 持久化
