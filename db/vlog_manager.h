#ifndef STORAGE_LEVELDB_DB_VLOG_MANAGER_H_
#define STORAGE_LEVELDB_DB_VLOG_MANAGER_H_

#include "db/vlog_fetcher.h"
#include "db/vlog_reader.h"
#include "db/vlog_writer.h"
#include <atomic>
#include <map>
#include <set>

#include "port/port_stdcxx.h"

namespace leveldb {
namespace vlog {
// Header is checksum (4 bytes), length (8 bytes).
static const int kVHeaderSize = 4 + 8;

static const int WriteBufferSize = 1 << 12;

class VlogFetcher;
class VWriter;

class VlogInfo {
  char buffer_[WriteBufferSize];
  size_t size_;
  VlogFetcher* vlog_fetch_;
  VWriter* vlog_write_;
  size_t head_;

  uint64_t count_;  //代表该vlog文件垃圾kv的数量

  port::SharedMutex* rwlock_;

 public:
  VlogInfo() : size_(0), head_(0), rwlock_(new port::SpinSharedMutex) {}
  ~VlogInfo() { delete rwlock_; }

  friend class VWriter;
  friend class VlogFetcher;
  friend class VlogManager;
};

class VlogManager {
 public:
  explicit VlogManager(uint64_t clean_threshold);
  ~VlogManager();

  void AddVlog(const std::string& dbname, const Options& options,
               uint64_t vlog_numb);

  Status AddRecord(const Slice& slice);

  Status SetHead(size_t offset);

  Status Sync();

  Status FetchValueFromVlog(Slice addr, std::string* value);

  // 返回当前 vlog 文件的写入位置（文件偏移量），用于计算记录的地址，返回到主节点
  // 返回值为 head_ + size_，即已刷盘字节数 + 缓冲区中尚未刷盘的字节数
  uint64_t GetWritePos();

  // 将当前 vlog 的内存缓冲区强制刷盘（SyncedAppend + head_ 递增 + size_ 清零）
  // 调用时机：创建新 vlog 文件前（确保旧文件数据落盘）、正常退出前
  void FlushCurrentBuffer();

  void SetCurrentVlog(uint64_t vlog_numb);

 private:
  std::map<uint64_t, VlogInfo*> manager_;
  std::set<uint64_t> cleaning_vlog_set_;
  uint64_t clean_threshold_;
  uint64_t cur_vlog_;
};

}  // namespace vlog
}  // namespace leveldb

#endif
