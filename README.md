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

**后续会追加其他选手的解法**



