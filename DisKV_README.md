使用说明：
Author:Frezz

# 什么是DisKV
DisKV是我基于LevelDB/Wisckey设计的节点级键值分离分布式系统。我设计的分布式主节点、从节点、客户端相关代码存放在 distributed 目录下，您可通过修改宏定义的"IP"以装配到离散集群中。
用于手动测试的客户端为 distributed/test_client，自动测试的客户端为 distributed/test_client ，注意 benchmarks 目录是 LevelDB 的测试集，请不要选错，具体编译运行方法见下：

# 可能的问题与报错说明：
1、在编译 Wisckey 项目时遇到 gmock/gmock.h: No such file or directory 错误，说明构建过程缺少 Google Mock 头文件。使用 sudo apt-get install libgtest-dev libgmock-dev 安装。
2、在使用 GCC（如 STM32CubeIDE）编译时，如果代码中使用了 __attribute__((at(address)))，会出现 warning: 'at' attribute directive ignored [-Wattributes]，这是因为 GCC 不支持 at 这种直接指定变量绝对地址的语法，它是 Keil/ARMCC 的扩展。使用Clang编译器即可。
3、大部分文件会在include处报红，不用担心，编译不会出现问题，因为CMakeLists.txt中使用了生成器表达式 $<BUILD_INTERFACE:...>来在编译时指定路径，而VSCode的IDE静态检测无法解析这种高级的表达式。如果你非要解决这个问题，可以在 cmake 阶段加入参数 -DCMAKE_EXPORT_COMPILE_COMMANDS=ON ，它会在build目录生成 compile_commands.json 文件，VSCode 的 C/C++ 扩展正是通过这个文件来获取准确的包含路径和编译选项。并在 cmake 完成后按下 Ctrl+Shift+P，执行 CMake: Configure ，使 CMake Tools 检测 build 目录已存在，并直接读取其中的配置（你可在OUTPUT中看到进度），最后按下 Ctrl+Shift+P，执行 Developer: Reload Window 刷新窗口即可。

# 1、修改宏定义：
修改 master.cc 中的宏定义，将其配置为三个从节点的实际 IP 地址：
#define SLAVE0_IP "127.0.0.1"
#define SLAVE1_IP "127.0.0.1"
#define SLAVE2_IP "127.0.0.1"
修改 slave.cc 中的全局变量，将其配置为主节点的实际 IP 地址：
const std::string MASTER_HOST = "127.0.0.1";
# 2、构建与编译：
编译流程：（在根目录下）
sudo apt install libsnappy-dev                                        安装Snappy开发库(当然git与GCC也要装)
git submodule update --init --recursive                               下载工程子模块，确保构建时完备(稍慢，可跳过此步在cmake时忽略)
rm -rf build                                                          清理旧build目录
mkdir -p build && cd build                                            创建build文件夹并打开
cmake -DCMAKE_BUILD_TYPE=Release -DLEVELDB_BUILD_BENCHMARKS=ON -DLEVELDB_BUILD_TESTS=OFF -DCMAKE_EXPORT_COMPILE_COMMANDS=ON ..       执行cmake构建并开启编译优化,忽略子模块
make -j$(nproc) master slave client test_client                       依据Makefile进行编译
完成后会在build目录下看到可执行文件 master、slave、client，按照如下示例执行命令即可进行测试。
# 3、打开五个终端并全进入build目录：
       (New Terminal)
       cd build
# 4、启动所有 Slave：
       终端1：启动 Slave 0
       ./slave 0
       终端2：启动 Slave 1
       ./slave 1
       终端3：启动 Slave 2
       ./slave 2
# 5、启动 Master：
       终端4：启动 Master
       ./master
# 6、启动 Client：
       终端5：启动 client 并连接到主节点
       ./client <主节点实际IP地址> 8888
# 7、获取路由表
       在 client 终端输入：
       HELLO
       后会获取当前从节点的路由表，并断开与主节点的连接。
# 8、进行数据操作测试：
注：目前只支持 ASCII 字符，其余字符会全部丢给 Worker2 。
// 注：主节点为每个从节点分配独立端口，避免重连顺序错位：
//     Worker 0 连接端口 8889
//     Worker 1 连接端口 8890
//     Worker 2 连接端口 8891
// Worker 0: 首字符 ASCII 0 ~ 42  : 从 空字符(0) 到 '*' (42)
// Worker 1: 首字符 ASCII 43 ~ 85 : 从 '+' (43) 到 'U' (85)
// Worker 2: 首字符 ASCII 86 ~ 127: 从 'V' (86) 到 DEL (127)
例：
       PUT 1 hello_world
       返回: STORED

       PUT a foo_bar
       返回: STORED

       GET 1
       返回: VALUE hello_world

       DELETE 1
       返回: DELETED

       GET 1
       返回: NOT FOUND

       SCAN * z
       返回：a foo_bar

       GC 2
       返回：GC OK / GC DENY
执行成功后默认会在 wisckey/build/wisckey_db_worker_* 下生成数据库数据与日志
# 9、进行持久化测试：
使用 Ctrl+C 退出,
当显示已安全关闭数据库后，数据落盘，这时重新连接测试即可。

数据库目录下的文件格式如下：
diskv_master_db目录：
       CURRENT	       文本	  指向当前 MANIFEST 文件
       MANIFEST-*	       二进制	  记录数据库版本等（所有 SSTable 的元数据）
       *.ldb	              二进制	  一个SSTable，只存储（键+地址），因而非常小
       LOCK	              空/文本  进程锁文件
       LOG / LOG.old	       文本	  数据库运行日志，当数据库重启后，原先的LOG会变为LOG.old
diskv_slave_vlog_*目录：
       *.log	              二进制	  用来存储值的vlog，不会消失。
       (注：在LevelDB中.log存储的是WAL预写日志，或者叫内存中的MemTable在磁盘的副本，随MemTable落盘而消失，而在Wisckey中沿用了其名称和功能，该文件变为用于存储vlog的文件，不会消失)
       VLOGMETA             文本     用来存储有关vlog的元数据，包括当前写入位置与当前阈值
# 10、进行自动化测试集测试：
进入build目录，运行以下命令以进入自动测试端：

./test_client <主节点实际IP地址> 8888

在测试端可输入以下命令来选择操作与参数：
--benchmarks	        指定要运行的测试（逗号分隔），内置多个测试项
--num	               总记录数	1000000
--key_size            键的大小（字节）
--value_size	        值的大小（字节）100
--histogram           是否输出耗时直方图（0 或 1）	0
--use_existing_db	 是否使用已有数据库（若为 0 则新建库覆盖旧库，为 1 则使用旧数据库）0

操作项目：
fillseq	             按顺序写入 N 条记录（异步）
fillrandom	             随机顺序写入 N 条记录（异步）
overwrite	             随机覆盖写入 N 条记录（异步）
deleteseq	             按顺序删除 N 条记录
deleterandom	             随机删除 N 条记录
readseq                    顺序读取 N 条记录
readreverse	             逆序读取 N 条记录
readrandom	             随机读取 N 条记录
readmissing	             随机读取 N 条不存在的键
readhot      	             随机读取 DB 前 1% 的热点数据

输入命令和参数进行测试即可。
示例：
--benchmarks=fillseq --num=40960 --value_size=100000 --histogram=1


# 对于Wisckey代码的变更说明：
文件	             变更说明
options.h	     新增 bool no_vlog = false 选项
db.h	            新增 PutAddress、GetAddress、DeleteKey 虚方法
db_impl.h	     声明 PutAddress、GetAddress、DeleteKey
db_impl.cc	     实现三个方法（绕过 vlog，直接操作 LSM-Tree）；DB::Open 和 MakeRoomForWrite   中跳过 vlog 创建
vlog_manager.h     新增 GetWritePos() 方法声明
vlog_manager.cc    实现 GetWritePos()，用于记录写入的位置，便于传回主节点进行记录
CMakeLists.txt     为master、slave、client增加构建条目
