package com.alibaba.middleware.race.storage;

import com.alibaba.middleware.race.models.Row;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByGoodId;

import java.util.HashMap;

/**
 * Created by xiyuanbupt on 8/1/16.
 * goodid 索引的分片
 * 继承自partion ,因为good 索引要比真实数据小很多,所以查询的时候需要缓存good 索引的叶子节点和
 */
public class GoodIndexPartion extends IndexPartition<ComparableKeysByGoodId>{

    private HashMap<DiskLoc,IndexNode<ComparableKeysByGoodId>> myCache;
    public GoodIndexPartion(int myHashCode){
        super(myHashCode);
        /**
         * 不适用lru 队列,因为数据量特别小,所以所有节点都会被缓存
         */
        myLRU = null;
        myCache = new HashMap<>(2);
    }

    @Override
    public synchronized Row queryByKey(ComparableKeysByGoodId keys){
        /**
         * 前半部分和父类基本相同
         */
        if(rootIndex==null)merageAndBuildMe();
        IndexNode<ComparableKeysByGoodId> indexNode = rootIndex;
        while(!indexNode.isLeafNode()){
            DiskLoc diskLoc = indexNode.search(keys);

            IndexNode<ComparableKeysByGoodId> cacheNode = myCache.get(diskLoc);
            indexNode = cacheNode==null?indexExtentManager.getIndexNodeFromDiskLocForInsert(diskLoc):cacheNode;
            if(diskLoc==null)return null;
            if(cacheNode==null)myCache.put(diskLoc,indexNode);
        }
        DiskLoc diskLoc = indexNode.search(keys);
        if(diskLoc==null)return null;
        return originalExtentManager.getRowFromDiskLoc(diskLoc);
    }
}
