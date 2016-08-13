#inertiaSearch

**本项目是我个人组队参加第二届阿里中间件性能挑战[中间件性能挑战第二季排名](https://tianchi.shuju.aliyun.com/programming/rankingList.htm?spm=0.0.0.0.rzkb9w&raceId=231533),[赛题与数据](https://tianchi.shuju.aliyun.com/programming/information.htm?spm=0.0.0.0.cclfbM&raceId=231533),使用Hash + bPlusTree + nio 实现的简单的数据库,因为单人组队和使用java
时间不长，实现方法并不适用于大量数据短时间排序，最终成绩为33名。但是因为程序倾注了个人很多的心血，并且有很多借鉴的地方，故在此做了一个总结**

##本项目使用到的算法包括

* 内排序：avlTree，quickSort，shellSort，binarySearchTree，lru队列，败者树（归并排序）。
* 外排序：败者树，bPlusTree。
* 其他：Hash

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

##借鉴别人的代码
*别人的代码*  

2016年8月13

今天看了两个开源实现，其中一个能够达到前三的效果，发现简单的hash既能完成大量的查询(我依旧相信hash + bPlus能够效果更好)。代码量也很少。

###[三位北邮校友的实现](https://github.com/immortalCockroach/alibabaMiddlewareRace-s2)

####“技不如人” 的地方

* 相同的hash存在文件 VS 只维护一个Current index file（2G），所有hash都顺序填入，填满了之后创建新文件（本程序）。对比之下本程序的缺点是：对于单条记录查询，在没有创建b+ 的情况下，mmap 需要大量的换页。对于范围查询，不管是否创建了b+，index 分散在大量的文件中，同样有大量的mmap 换页。而一个Hash 文件则不会有这个问题。
* HashMap rehash有开销，所以他们的程序在缓存good 与 buyer 直接设置了大小，我虽然没有使用hash，但是可以借鉴
* join 优化：查询一个buyer 一段时间订单的时候buyer 只查询一次 VS buyer 查询多次（本程序）。（我已经无力吐槽，和人沟通少，自己也没想到）
* join 优化，有关key 的查询：查到key了就不在访问文件系统了 VS 访问完文件系统再做过滤(本程序！)。（没毛病！！）
* join 优化（求和）：如果key 在good 和 buyer 中，只需要知道count，查到一次就 * count 就可以 VS 查询之后做过滤（本程序！）。（！）
