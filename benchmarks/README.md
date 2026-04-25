测试集使用说明：
Author:Frezz

# 可能的问题说明：
1、在编译 Wisckey 项目时遇到 gmock/gmock.h: No such file or directory 错误，说明构建过程缺少 Google Mock 头文件。使用 sudo apt-get install libgtest-dev libgmock-dev 安装。
2、在使用 GCC（如 STM32CubeIDE）编译时，如果代码中使用了 __attribute__((at(address)))，会出现 warning: 'at' attribute directive ignored [-Wattributes]，这是因为 GCC 不支持 at 这种直接指定变量绝对地址的语法，它是 Keil/ARMCC 的扩展。使用Clang编译器即可。
3、大部分文件会在include处报红，不用担心，编译不会出现问题，因为CMakeLists.txt中使用了生成器表达式 $<BUILD_INTERFACE:...>来在编译时指定路径，而VSCode的IDE静态检测无法解析这种高级的表达式。如果你非要解决这个问题，可以在 cmake 阶段加入参数 -DCMAKE_EXPORT_COMPILE_COMMANDS=ON ，它会在build目录生成 compile_commands.json 文件，VSCode 的 C/C++ 扩展正是通过这个文件来获取准确的包含路径和编译选项。并在 cmake 完成后按下 Ctrl+Shift+P，执行 CMake: Configure ，使 CMake Tools 检测 build 目录已存在，并直接读取其中的配置（你可在OUTPUT中看到进度），最后按下 Ctrl+Shift+P，执行 Developer: Reload Window 刷新窗口即可。

# 1、构建与编译：
编译流程：（在根目录下）
sudo apt install libsnappy-dev                                        安装Snappy开发库(当然git与GCC也要装)
git submodule update --init --recursive                               下载工程子模块，确保构建时完备(稍慢，建议挂梯子，或在cmake时忽略)
rm -rf build                                                          清理旧build目录
mkdir -p build && cd build                                            创建build文件夹并打开
cmake -DCMAKE_BUILD_TYPE=Release -DLEVELDB_BUILD_BENCHMARKS=ON -DCMAKE_EXPORT_COMPILE_COMMANDS=ON ..     执行cmake构建并开启编译优化
make -j$(nproc) master slave                                    依据Makefile进行编译
完成后会在build目录下看到可执行文件 master、slave，按照如下示例执行命令即可进行测试。
# 2、打开三个终端并全进入build目录：
       (New Terminal)
       cd build
# 3、启动所有 Slave：
       终端1：启动 Slave 0（端口 8889）
       ./slave 8889
       终端2：启动 Slave 1（端口 8890）
       ./slave 8890
       终端3：启动 Slave 2（端口 8891）
       ./slave 8891
# 4、启动 Master：
       终端4：启动 Master（端口 8888）
       ./master
# 5、连接到 Master：
       telnet 127.0.0.1 8888
# 6、进行数据操作测试：
注：目前只支持 ASCII 字符。
例：
       PUT hello_world
       返回: OK 1

       PUT foo_bar
       返回: OK 2

       GET 1
       返回: VALUE hello_world

       DELETE 1
       返回: DELETED

       GET 1
       返回: NOT FOUND
执行成功后默认会在 wisckey/build/wisckey_db_worker_**** 下生成数据库数据与日志
# 7、进行持久化测试： // TODO 为什么一定要先退出从节点，这是bug？如果实际中主节点先崩应当怎么办。
一定要先 Ctrl+C 退出从节点！
再用 Ctrl+C 退出主节点！
当显示已安全关闭数据库后，数据落盘，这时重新连接测试即可。

数据库目录下的文件格式如下：
       CURRENT	       文本	  指向当前 MANIFEST 文件
       MANIFEST-*	       二进制	  记录数据库版本等（所有 SSTable 的元数据）
       *.ldb	              二进制	  一个SSTable，只存储（键+地址），因而非常小
       *.log	              二进制	  用来存储值的vlog。(注：在LevelDB中.log存储的是WAL预写日志，随MemTable落盘而消失)
       LOCK	              空/文本  进程锁文件
       LOG / LOG.old	       文本	  数据库运行日志，当数据库重启后，原先的LOG会变为LOG.old

# // TODO 测试删除后，合并后的id从何开始
# // TODO 在分布式上实现如下测试集操作
关键操作：
--benchmarks	        指定要运行的测试（逗号分隔），如 fillseq,readrandom	内置多个测试项
--num	               总记录数	1000000
--reads	        读取操作次数，若为 -1 则等于 num	-1
--threads	        并发线程数	1
--value_size	        值的大小（字节）100
--key_prefix          键前缀字符数（字节），前缀越大压缩效果越好？ 0
--compression_ratio   压缩后大小比例，用于生成可压缩数据	0.5
--histogram           是否输出耗时直方图（0 或 1）	0
--cache_size	        块缓存大小（字节），-1 表示使用默认	-1
--bloom_bits	        布隆过滤器每键比特数，-1 表示默认	-1
--write_buffer_size	 内存表大小（字节），默认 4MB	由 Options 决定
--max_file_size	 最大 SSTable 文件大小（字节），默认 2MB	由 Options 决定
--block_size	        数据块大小（字节），默认 4KB	由 Options 决定
--compression	        是否启用压缩（Snappy）	1（启用）
--use_existing_db	 是否使用已有数据库（若为 0 则新建库覆盖旧库，为 1 则使用旧数据库）0
--reuse_logs          重用日志/清单文件（默认 false）
--comparisons         统计比较次数
--db	               数据库存储路径，默认自动创建临时目录	/tmp/leveldbtest-1000/dbbench
打印KV键值对           详见PrintKV.cc文件

测试项目：
fillseq	             按顺序写入 N 条记录（异步）
fillrandom	             随机顺序写入 N 条记录（异步）
overwrite	             随机覆盖写入 N 条记录（异步）
fillsync	             随机写入 N/100 条记录，每次 sync（同步刷盘）
fill100K	             随机写入 N/1000 条 100KB 的大记录
deleteseq	             按顺序删除 N 条记录
deleterandom	             随机删除 N 条记录
readseq                    顺序读取 N 条记录
readreverse	             逆序读取 N 条记录
readrandom	             随机读取 N 条记录
readmissing	             随机读取 N 条不存在的键
readhot      	             随机读取 DB 前 1% 的热点数据
readaddrseq               （Wisckey独有）顺序“键从小到大”读地址
readaddrreverse           （Wisckey独有）逆序“键从大到小”读地址
fetchvaluefromaddr        （Wisckey独有）根据地址取数据（必须在readaddr后执行且必须在同一benchmarks命令下）
seekrandom	             随机执行 N 次 Seek 操作
seekordered	             有序执行 N 次 Seek（键递增）
open	                    打开数据库 N/10000 次（测量打开开销）
crc32c	                    对 4KB 数据反复计算 CRC32C（约 500MB）
compact	             手动触发全量 Compaction
stats	                    打印 DB 统计信息
sstables	             打印 SSTable 信息, LSM-Tree下的各SSTable存储情况。
heapprofile	             生成堆内存快照（需平台支持）
snappycomp / snappyuncomp	    测试 Snappy 压缩/解压 1GB 数据的吞吐
zstdcomp / zstduncomp	    测试 Zstd 压缩/解压 1GB 数据的吞吐

示例：(在build目录下)
./create_test_db    生成数据库（无输出）
./iterate_test_db   测试迭代器（无输出）
./db_bench --benchmarks=fillseq --num=8192 --value_size=100000 --histogram=1  设置参数测试基准测试集
       理论大小num*value_size           实际大小                        差值
       819200000                       819568640                      368640
       81920000                        82280448                       360448
       8192000                         8552448                        360448
       819200                          1171456                        352256
       81920                           434176                         352256
       8192                            360448                         352256

./db_bench --benchmarks=readaddrseq --histogram=1 --use_existing_db=1  使用当前数据库测试读取键性能
./db_bench --benchmarks=readaddrseq --benchmarks=fetchvaluefromaddr --histogram=1 --use_existing_db=1  使用当前数据库测试读取键与值性能

指标解释：（这是在执行上述指令后得到的相关指标）
Keys:       16 bytes each                                     键的大小
Values:     100 bytes each (50 bytes after compression)       值的大小
Entries:    100000                                            输入的记录数
RawSize:    11.1 MB (estimated)                               原始数据量（数学估计）
FileSize:   6.3 MB (estimated)                                前缀压缩后的数据量（数学估计）
------------------------------------------------
fillseq      :       0.655 micros/op;  168.9 MB/s             每次操作的平均耗时； 吞吐量
Microseconds per op:
Count: 100000  Average: 0.6529  StdDev: 16.87                 操作次数； 平均值； 标准差
Min: 0.0000  Median: 0.9160  Max: 5270.0000                   最小耗时； 中位数； 最大耗时
------------------------------------------------------
                    直方图
       耗时区间        操作数  本行占比  累计占比   柱状图
[       0,       1 )   54588  54.588%  54.588% ###########
[       1,       2 )   40906  40.906%  95.494% ########
[       2,       3 )    1806   1.806%  97.300% 
[       3,       4 )    1971   1.971%  99.271% 
[       4,       5 )     283   0.283%  99.554% 
[       5,       6 )      89   0.089%  99.643% 
[       6,       7 )      43   0.043%  99.686% 
[       7,       8 )      23   0.023%  99.709% 
[       8,       9 )      15   0.015%  99.724% 
[       9,      10 )      24   0.024%  99.748% 
[      10,      12 )      58   0.058%  99.806% 
[      12,      14 )      41   0.041%  99.847% 
[      14,      16 )      21   0.021%  99.868% 
[      16,      18 )      17   0.017%  99.885% 
[      18,      20 )      13   0.013%  99.898% 
[      20,      25 )      30   0.030%  99.928% 
[      25,      30 )      17   0.017%  99.945% 
[      30,      35 )       9   0.009%  99.954% 
[      35,      40 )      13   0.013%  99.967% 
[      40,      45 )       4   0.004%  99.971% 
[      45,      50 )       3   0.003%  99.974% 
[      50,      60 )       3   0.003%  99.977% 
[      60,      70 )       2   0.002%  99.979% 
[      70,      80 )       3   0.003%  99.982% 
[      80,      90 )       4   0.004%  99.986% 
[      90,     100 )       3   0.003%  99.989% 
[     100,     120 )       2   0.002%  99.991% 
[     120,     140 )       2   0.002%  99.993% 
[     180,     200 )       1   0.001%  99.994% 
[     200,     250 )       2   0.002%  99.996% 
[     250,     300 )       1   0.001%  99.997% 
[     300,     350 )       2   0.002%  99.999% 
[    5000,    6000 )       1   0.001% 100.000% 

