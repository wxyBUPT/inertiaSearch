#inertiaSearch

**本项目是单人组队参加第二届阿里中间件性能挑战[中间件性能挑战第二季排名](https://tianchi.shuju.aliyun.com/programming/rankingList.htm?spm=0.0.0.0.rzkb9w&raceId=231533),[赛题与数据](https://tianchi.shuju.aliyun.com/programming/information.htm?spm=0.0.0.0.cclfbM&raceId=231533),使用Hash + bPlusTree + nio 实现的简单的数据库(排序想法类似于TeraSort的map阶段),因为单人组队和使用java
时间不长，导致几个关键的想法没有落实(同类index 分散存储 VS 集中存储；按照题意如何减少io次数)，最终成绩为33名。但是因为程序倾注了个人很多的心血，技术实现甚至能够超过排名靠前的队伍，并且有很多借鉴的地方，故在此做了一个总结**

##本项目使用到的算法包括

* 内排序：avlTree，quickSort，shellSort，binarySearchTree，lru队列，败者树（归并排序）。
* 外排序：败者树，bPlusTree。
* 其他：Hash

##项目相关技术连接

* [Log-structured merge-tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree)  
* [TeraSort的一篇中文介绍](http://dongxicheng.org/mapreduce/hadoop-terasort-analyse/)
* [Sort Benchmark Home Page](http://sortbenchmark.org/)  

##项目底层与文件系统打交道包括

* 项目大量的使用了nio中的mmap（MappedByteBuffer）
* 对于原始的数据文件做内存映射，并做对应索引，所有索引做hash 后统一对待，索引单个文件为2G，并被分为16个Extent（可以使用参数配置，配置文件在RaceConf 中）

##查询索引使用惰性查找

本程序使用Hash + bPlusTree 结合的方式查询，但是因为io 有限，一个小时不能创建百G数据的bTree，所以使用惰性求值，即首先按照数据的hash 将不同的索引分到不同的partion 中，查询阶段查找到这个partion ，这个partion 会首先创建bPlusTree，之后查询。bPlusTree 的创建使用批量快排和败者树创建。

##比赛体会  

* io使用：多线程不一定快、一个盘读最好在另外一个盘写、内存映射文件......
* 一个人的力量和想法都是有限的，别人都是三个人一份代码或者三个人借鉴了另外三个人的代码（大牛排除），所以很吃亏。
* 思路比实现更重要，写代码之前一定要好好思考
* 暴露出跟人能力不足，比如估算内存占用方面。
* 目前代码中不理理想的地方可能要等我今后java 知识丰富了之后再回来总结

##程序中的亮点

* 充分使用了多核cpu：多处使用了生产者消费者模式，通过调整参数可以避免木桶效应（但是也增加了程序的开销）。使用了回调。例如一个index 完成的数据流如下：读取 -> 解析字段并填入内存 -> 排序并写入index 文件。
* 惰性查找，第一次查找创建b+ ，（越查越快）

**后续会追加其他选手的解法**

##程序结构

*构建阶段数据处理流程*  
![](http://7xrgjg.com1.z0.glb.clouddn.com/%E6%95%B0%E6%8D%AE%E5%A4%84%E7%90%86%E6%B5%81%E7%A8%8B%EF%BC%88%E6%9E%84%E5%BB%BA%EF%BC%89%20%281%29.jpg)

*查询阶段数据处理流程*  
**待完成**  

##借鉴别人的代码
*别人的代码*  

2016年8月13

今天看了两个开源实现，其中一个能够达到前三的效果，发现简单的hash既能完成大量的查询(我依旧相信hash + bPlus能够效果更好)。 比人的代码量也很少。

###[三位北邮校友的实现](https://github.com/immortalCockroach/alibabaMiddlewareRace-s2)

####“技不如人” 的地方

* 相同的hash存在相同的文件 VS 只维护一个Current index file（2G），所有hash都顺序填入，填满了之后创建新文件（本程序）。对比之下本程序的缺点是：对于单条记录查询，在没有创建b+ 的情况下，mmap 需要大量的换页。对于范围查询，不管是否创建了b+，index 分散在大量的文件中，同样有大量的mmap 换页。而一个Hash 文件则不会有这个问题。
* HashMap rehash有开销，所以他们的程序在缓存good 与 buyer 直接设置了大小，我虽然没有使用hash，但是可以借鉴
* join 优化：查询一个buyer 一段时间订单的时候buyer 只查询一次 VS buyer 查询多次（本程序）。（我已经无力吐槽，和人沟通少，自己也没想到）
* join 优化，有关key 的查询：查到key了就不在访问文件系统了 VS 访问完文件系统再做过滤(本程序！)。（没毛病！！）
* join 优化（求和）：如果key 在good 和 buyer 中，只需要知道count，查到一次就 * count 就可以 VS 查询之后做过滤（本程序！）。（！）

2016年8月26日
*今天阿里公布了答辩相关的ppt与食品,在旺旺群上看到了看到很多选手开源了相关实现，本周末决定做最后的总结*

[答辩视频](https://tianchi.shuju.aliyun.com/video.htm)  

###[GammaGo中科院_暂未开源_第一名](https://github.com)  
[答辩ppt](http://yunpan.taobao.com/s/10veL8VtCS0#/)  

*这个队伍使用了TeraSort，所以先对TeraSort进行简单的介绍，并添加TearSort简单的实现(./src/main/java/com/alibaba/middleware/race/decoupling/TearSort.java)*  

####TeraSort简介  

下图是Map-Reduce的一张非常经典的流程图：

![Map-Reduce流程图](http://kubicode.me/img/TeraSort-in-Hadoop/shuffle.gif)

Map-Reduce 用于大规模数据集的并行运算。Map(映射) 和 Reduce(归约) 是他们的主要思想。

下图是最Hadoop最简单的排序方法，这种方法丢失了reduce端的并行度。

![Hadoop最简单的排序方法](http://kubicode.me/img/TeraSort-in-Hadoop/sortInOneReduce.jpg)  

借鉴快速排序方法，TeraSort 流程如下，其中每个Partion之间都是有序的。  

![TearSort](http://kubicode.me/img/TeraSort-in-Hadoop/sortInMulReduce.jpg)  

**TearSort算法流程**  

三步骤：采样 ->> map task 对于数据记录进行标记 ->> reduce task 进行局部排序。  

数据采样在JobClient 端进行，首先从输入数据中抽取一部分数据，将这些数据进行排序，然后将它们划分成R个数据块，找出每个数据块的上线和下限，成为“分割点”，并将分割点保存到缓存中。

在map 阶段，每个map task 首先从分布式缓存中读取分割点，并对这些分割点建立trie 树(两层trie树，树的叶子节点保存有该节点对应的reduce task编号）。然后正式处理数据，对于每条数据，在trie 树种查找它属于的reduce task 的编号并保存起来。

在reduce 阶段，每个reduce task 从每个map task 中读取其对应的数据进行局部排序，最后将 reduce task 处理后的结果按照reduce task 编号一次输出即可。

####磁盘读写控制  

**读：**  
使用buff，并对磁盘加锁(即使有多个读线程，但是只有一个线程能够读)
