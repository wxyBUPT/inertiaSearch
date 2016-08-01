package com.alibaba.middleware.race.storage;

import com.alibaba.middleware.race.RaceConf;
import com.alibaba.middleware.race.cache.ConcurrentLruCacheForBigData;
import com.alibaba.middleware.race.cache.LRUCache;
import com.alibaba.middleware.race.decoupling.FlushUtil;
import com.alibaba.middleware.race.decoupling.QuickSort;
import com.alibaba.middleware.race.decoupling.ShellSort;
import com.alibaba.middleware.race.models.Row;

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
    protected final int myHashCode;

    /**
     * Vector 中每一个元素都是在磁盘中排好序的key 值,此部分
     */
    protected Queue<LinkedList<DiskLoc>> sortedKeysInDisk;

    /**
     * 用来缓存b+ 树的跟节点
     */
    public IndexNode<T> rootIndex;

    /**
     * 查询阶段的缓存放在partition 中
     */
    protected LRUCache<DiskLoc,IndexNode> myLRU;

    /**
     * 两个用于和底层db 交互的代理
     */
    protected IndexExtentManager indexExtentManager;
    protected OriginalExtentManager originalExtentManager;

    private FlushUtil<T> flushUtil;

    /**
     * 两个用于添加数据和排序的ArrayList
     * 每一个partion 会启动新的线程排序,并将排序后的索引添加到磁盘,并不会阻塞数据插入线程
     */
    private LinkedBlockingQueue<Vector<T>> keysCacheQueue;

    public Vector<T> currentCache;
    /**
     * 当前缓存中有多少个元素
     */
    private int elementCount;

    /**
     * 执行quickSort
     */
    private ShellSort<T> shellSort;

    /**
     * 构造函数
     * @param myHashCode
     */

    private Long totalCount;
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
            keysCacheQueue.add(new Vector<T>(RaceConf.PARTITION_CACHE_COUNT));
            currentCache = new Vector<>(RaceConf.PARTITION_CACHE_COUNT);
        }catch (Exception e){
            e.printStackTrace();System.exit(-1);
        }
        elementCount = 0;
        shellSort = new ShellSort<>();
        rootIndex = null;
        totalCount = 0L;
    }

    public String getInfo(){
        StringBuilder sb = new StringBuilder();
        sb.append("My hashCode is " + myHashCode);
        sb.append("My root is " + rootIndex);
        sb.append("Total Key count is " + totalCount);
        return sb.toString();
    }

    public Long getTotalCount(){
        return totalCount;
    }

    /**
     * 将key 值添加到partion 中去
     * @param t
     */
    public void addKey(T t){
        totalCount ++;
        /**
         * 如果当前缓存元素个数满
         */
        //if(elementCount>=802){
        if(elementCount>=RaceConf.PARTITION_CACHE_COUNT){
            /**
             * 将数据放到戴排序的队列中,等待外部排序线程排序
             */
            Vector<T> tmp = currentCache;
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
        Vector<T> cache;
        SortThread(Vector<T> cache){
            this.cache = cache;
        }
        @Override
        public void run() {
            List<T> sortedList = shellSort.shellsort(cache);
            sortedKeysInDisk.add(flushUtil.moveListDataToDisk(sortedList));
            keysCacheQueue.offer(new Vector<T>(RaceConf.PARTITION_CACHE_COUNT));
        }
    }

    /**
     * 归并合并 sortedKeysInDisk,并创建b树
     * 由于partion 之间是没有共享数据的,所以以线程为单位执行
     * 当前是两路归并排序,
     */
    public synchronized void merageAndBuildMe(){
        /**
         * 清空当前缓存队列到硬盘中,因为有两个缓存队列,一个在另外的线程中执行,所以写下面的代码出现bug 的可能性比较大
         */
        if(rootIndex!=null){
            LOG.info("查询阶段已经帮助创建b树,所以不用创建");
            return ;
        }
        List<T> sortedList = shellSort.shellsort(currentCache);
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

        /**
         * 使用败者树合并归并排序节点
         */
        while(sortedKeysInDisk.size()>1){
            System.out.println(sortedKeysInDisk.size());
            List<Iterator<T>> branchs = new ArrayList<>();
            /**
             * 对所有路进行归并排序,使用败者树
             */
            while(sortedKeysInDisk.size()>0){
                LinkedList<DiskLoc> diskLocs = sortedKeysInDisk.poll();
                if(diskLocs!=null){
                    branchs.add(new IndexLeafNodeIterator<T>(diskLocs,indexExtentManager));
                }
            }
            sortedKeysInDisk.add(flushUtil.loserTreeMerge(branchs));
        }
        if(sortedKeysInDisk.size()<1){
            LOG.info("hashCode 为" + myHashCode + "中没有元素");
            rootIndex = null;
            return;
        }
        LinkedList<DiskLoc> diskLocs = sortedKeysInDisk.poll();
        DiskLoc diskLoc = flushUtil.buildBPlusTree(diskLocs);
        rootIndex = indexExtentManager.getIndexNodeFromDiskLocForInsert(diskLoc);
    }

    /**
     * 查询一个元素
     * @param t
     * @return
     */
    public synchronized Row queryByKey(T t){
        if(rootIndex==null)merageAndBuildMe();
        IndexNode<T> indexNode = rootIndex;
        while(!indexNode.isLeafNode()){
            DiskLoc diskLoc = indexNode.search(t);

            indexNode = indexExtentManager.getIndexNodeFromDiskLocForInsert(diskLoc);
            if(diskLoc == null )return null;
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
        if(rootIndex==null)merageAndBuildMe();
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
                        IndexNode indexNode = indexExtentManager.getIndexNodeFromDiskLocForInsert(diskLoc);
                        nodes.add(indexNode);
                    }
                }
            }
        }
        return result;
    }
}

