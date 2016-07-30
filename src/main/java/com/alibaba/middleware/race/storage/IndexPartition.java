package com.alibaba.middleware.race.storage;

import com.alibaba.middleware.race.RaceConf;
import com.alibaba.middleware.race.cache.ConcurrentLruCacheForBigData;
import com.alibaba.middleware.race.cache.LRUCache;
import com.alibaba.middleware.race.decoupling.FlushUtil;
import com.alibaba.middleware.race.decoupling.QuickSort;
import com.alibaba.middleware.race.decoupling.ShellSort;
import com.alibaba.middleware.race.models.Row;
import com.sun.corba.se.impl.orbutil.concurrent.SyncUtil;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

/**
 * Created by xiyuanbupt on 7/26/16.
 * 一类 index 的一个分片,负责管理hash() 值相同的
 * 重构之后, 很多indexNameSpace 的工作交给了partion 处理,indexNameSpace 只负责路由的功能
 */
public class IndexPartition<T extends Comparable<? super T> & Serializable & Indexable> {

    static final Logger LOG = Logger.getLogger(IndexPartition.class.getName());
    /**
     * 一类Key 值被分为64部分
     */
    private final int myHashCode;

    /**
     * Vector 中每一个元素都是在磁盘中排好序的key 值,此部分
     */
    private Queue<LinkedList<DiskLoc>> sortedKeysInDisk;

    /**
     * 用来缓存b+ 树的跟节点
     */
    public IndexNode<T> rootIndex;

    /**
     * 查询阶段的缓存放在partition 中
     */
    private final LRUCache<DiskLoc,IndexNode> myLRU;

    /**
     * 两个用于和底层db 交互的代理
     */
    private IndexExtentManager indexExtentManager;
    private OriginalExtentManager originalExtentManager;

    private FlushUtil<T> flushUtil;

    /**
     * 两个用于添加数据和排序的ArrayList
     * 每一个partion 会启动新的线程排序,并将排序后的索引添加到磁盘,并不会阻塞数据插入线程
     */
    private LinkedBlockingQueue<List<T>> keysCacheQueue;

    public List<T> currentCache;
    /**
     * 当前缓存中有多少个元素
     */
    private int elementCount;

    /**
     * 执行quickSort
     */
    private QuickSort<T> quickSort;

    /**
     * 排序锁,只有一个线程能够执行排序任务
     */
    private Lock sortLock ;

    /**
     * 构造函数
     * @param myHashCode
     */
    public IndexPartition(int myHashCode){
        indexExtentManager = IndexExtentManager.getInstance();
        originalExtentManager = OriginalExtentManager.getInstance();

        this.myHashCode = myHashCode;
        this.sortedKeysInDisk = new LinkedList<>();
        /**
         * 假定当前只对 order key 的值分片
         */
        myLRU = new ConcurrentLruCacheForBigData<>(RaceConf.N_ORDER_INDEX_CACHE_COUNT);
        flushUtil = new FlushUtil<>();
        /**
         * 队列里面只能有一个没有被填满的缓存
         */
        keysCacheQueue = new LinkedBlockingQueue<>(1);
        try {
            keysCacheQueue.add(new ArrayList<T>(RaceConf.PARTITION_CACHE_COUNT));
            currentCache = new ArrayList<>(RaceConf.PARTITION_CACHE_COUNT);
        }catch (Exception e){
            e.printStackTrace();System.exit(-1);
        }
        elementCount = 0;
        quickSort = new QuickSort<>();
        sortLock = new ReentrantLock();
        rootIndex = null;
    }

    public String getInfo(){
        StringBuilder sb = new StringBuilder();
        sb.append("My hashCode is " + myHashCode);
        sb.append("My root is " + rootIndex);
        return sb.toString();
    }

    /**
     * 将key 值添加到partion 中去
     * @param t
     */
    public void addKey(T t){
        /**
         * 如果当前缓存元素个数满
         */
        //if(elementCount>=802){
        if(elementCount>=RaceConf.PARTITION_CACHE_COUNT){
            /**
             * 将数据放到戴排序的队列中,等待外部排序线程排序
             */
            List<T> tmp = currentCache;
            try {
                currentCache = keysCacheQueue.take();
                elementCount = 0;
            }catch (Exception e){
                e.printStackTrace();
                System.exit(-1);
            }
            new Thread(new SortThread(tmp)).start();
        }
        elementCount++;
        currentCache.add(t);
    }

    class SortThread implements Runnable{
        List<T> cache;
        SortThread(List<T> cache){
            this.cache = cache;
        }
        @Override
        public void run() {
            List<T> sortedList = quickSort.quicksort(cache);
            sortedKeysInDisk.add(flushUtil.moveListDataToDisk(sortedList));
            keysCacheQueue.offer(new ArrayList<T>(RaceConf.PARTITION_CACHE_COUNT));
        }
    }

    /**
     * 归并合并 sortedKeysInDisk,并创建b树
     * 由于partion 之间是没有共享数据的,所以以线程为单位执行
     * 当前是两路归并排序,
     */
    public synchronized void merageAndBuildMe(){
        /**
         * 避免重复创建
         */
        if(rootIndex!=null){
            LOG.info("bTree already build by query!");
            return ;
        }else {
            LOG.info("构建线程创建索引");
        }
        /**
         * 清空当前缓存队列到硬盘中,因为有两个缓存队列,一个在另外的线程中执行,所以写下面的代码出现bug 的可能性比较大
         */
        List<T> sortedList = quickSort.quicksort(currentCache);
        sortedKeysInDisk.add(flushUtil.moveListDataToDisk(sortedList));
        /**
         * 等待排序线程执行完毕
         */
        try{
            keysCacheQueue.take();
        }catch (Exception e){
            e.printStackTrace();
            System.exit(-1);
        }

        while(sortedKeysInDisk.size()>1){
            LinkedList<DiskLoc> diskLocs = sortedKeysInDisk.poll();
            LinkedList<DiskLoc> diskLocs1 = sortedKeysInDisk.poll();
            IndexLeafNodeIterator<T> iterator = new IndexLeafNodeIterator<>(diskLocs,indexExtentManager);
            IndexLeafNodeIterator<T> iterator1 = new IndexLeafNodeIterator<>(diskLocs1,indexExtentManager);
            sortedKeysInDisk.add(flushUtil.mergeIterator(iterator,iterator1));
        }
        if(sortedKeysInDisk.size()<1){
            LOG.info("hashCode 为" + myHashCode + "中没有元素");
            rootIndex = null;
            return;
        }
        LinkedList<DiskLoc> diskLocs = sortedKeysInDisk.poll();
        DiskLoc diskLoc = flushUtil.buildBPlusTree(diskLocs);
        rootIndex = indexExtentManager.getIndexNodeFromDiskLocForInsert(diskLoc);
        LOG.info("创建索引完成,HashCode 是: " + myHashCode + "rootIndex 是 : " + rootIndex);
    }

    /**
     * 查询一个元素
     * @param t
     * @return
     */
    public synchronized Row queryByKey(T t){
        if(rootIndex==null){
            LOG.info("Start build bTree in query!!! query is " + t);
            merageAndBuildMe();
            LOG.info("查询阶段创建索引完成!! 查询的key 是" + t + "当前index root为" + rootIndex + ", hashCode 是" + myHashCode);
        }
        IndexNode<T> indexNode = rootIndex;
        while(!indexNode.isLeafNode()){
            DiskLoc diskLoc = indexNode.search(t);
            if(diskLoc == null )return null;

            IndexNode cacheNode = myLRU.get(diskLoc);
            indexNode = cacheNode==null?indexExtentManager.getIndexNodeFromDiskLocForInsert(diskLoc):cacheNode;
            /**
             * 如果节点是非叶子节点,并且缓存中没有数据,则缓存
             */
        if(cacheNode==null&&!indexNode.isLeafNode())myLRU.put(diskLoc,indexNode);
        }
        DiskLoc diskLoc = indexNode.search(t);
        if(diskLoc==null)return null;
        return originalExtentManager.getRowFromDiskLoc(diskLoc);
    }

    /**
     * 因为范围查询和只查询一个元素不会再相同的partion 中同时出现
     * @param startKey
     * @param endKey
     * @return
     */
    public synchronized Deque<Row> rangeQuery(T startKey,T endKey) {
        if(rootIndex==null){
            LOG.info("查询阶段创建bplus, query min key is  " + startKey + " query max key is " + endKey);
            merageAndBuildMe();
            LOG.info("查询阶段创建bplus创建完成, query min key is " + startKey +"query max key is " + endKey + "当前indexroot 为" + rootIndex + ", hashCode 是:" + myHashCode);
        }
        /**
         * 对磁盘中进行层序遍历
         */
        return levelTraversal(rootIndex,startKey,endKey);
    }

    /**
     * 范围寻找
     * @param root
     * @param minKey
     * @param maxKey
     * @param <V>
     * @return
     */
    private <V extends Comparable&Serializable&Indexable> LinkedList<Row> levelTraversal(IndexNode root, V minKey, V maxKey){
        LinkedList<Row> result = new LinkedList<>();

        Queue<IndexNode> nodes = new LinkedList<>();
        nodes.add(root);
        while(!nodes.isEmpty()){
            IndexNode node = nodes.remove();
            if(node.isLeafNode()){
                Queue<DiskLoc> diskLocs = node.searchBetween(minKey,maxKey);
                DiskLoc diskLoc = diskLocs.poll();
                while(diskLoc!=null){
                    Row row = originalExtentManager.getRowFromDiskLoc(diskLoc);
                    result.add(row);
                    diskLoc = diskLocs.poll();
                }
            }else {
                Queue<DiskLoc> diskLocs = node.searchBetween(minKey,maxKey);
                if(diskLocs!=null) {
                    while (!diskLocs.isEmpty()) {
                        DiskLoc diskLoc = diskLocs.remove();
                        IndexNode cacheNode = myLRU.get(diskLoc);
                        IndexNode indexNode = cacheNode==null?indexExtentManager.getIndexNodeFromDiskLocForInsert(diskLoc):cacheNode;
                        if(cacheNode==null&& !indexNode.isLeafNode()){
                            myLRU.put(diskLoc,indexNode);
                        }
                        nodes.add(indexNode);
                    }
                }
            }
        }
        return result;
    }
}

