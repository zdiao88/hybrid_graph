graph-processing
================

Leveraging best-fit graph format can improve the performance of graph processing. 
GraphChi is the first platform for graph processing on a sigle PC. The code is used to verifying our idea and learned from GraphChi. Also the configuration is similar to GraphChi.

DataSet: Twitter, LiveJournal,Netflix. 
Also the testdata/Test.txt is used to verify the correctness of SGD and ALS in a intuitive way. 

平台结构：
./application:应用程序
./bin:可执行文件
./core:图处理平台Engine
./preprocess:图数据预处理
./testdata:测试数据
./util:工具代码

命令行参数项：
1,file:指定图文件路径
2,niters:迭代次数
3,nsub:子图个数
4,nedge:待处理图边条数
5,nvertices:待处理图顶点个数

主要模块说明：
1,util/parse_config.hpp: ParseCmd类用于加载配置文件，并解析命令行。
2,util/graph_filename.hpp: GraphFileName类管理图处理过程的中间文件
3,preprocess.hpp: 
