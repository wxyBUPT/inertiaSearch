package com.alibaba.middleware.race.storage;

import com.alibaba.middleware.race.models.Row;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByBuyerId;

import java.util.HashMap;

/**
 * Created by xiyuanbupt on 8/1/16.
 * buyerid 索引的一个"桶"
 * 继承自 indexPartion ,buyer 的索引数据量和真是数据相比真是数据小一些,因此可以
 * 尝试将buyer 的索引数据持久化到内存中
 */
public class BuyerIndexPartion extends IndexPartition<ComparableKeysByBuyerId>{

    private HashMap<DiskLoc,IndexNode<ComparableKeysByBuyerId>> myCache;

    public BuyerIndexPartion(int myHashCode){
        super(myHashCode);
        myLRU = null;
        /**
         * 正常参数范围内一个partion 一般三个 indexLeafNode
         */
        myCache = new HashMap<>(3);
    }

    @Override
    public synchronized Row queryByKey(ComparableKeysByBuyerId keys){
        /**
         * 前半部分基本相同
         */
        if(rootIndex == null)merageAndBuildMe();
        IndexNode<ComparableKeysByBuyerId> indexNode = rootIndex;
        while(!indexNode.isLeafNode()){
            DiskLoc diskLoc = indexNode.search(keys);

            IndexNode<ComparableKeysByBuyerId> cacheNode = myCache.get(diskLoc);
            indexNode = cacheNode == null?indexExtentManager.getIndexNodeFromDiskLocForInsert(diskLoc):cacheNode;
            if(diskLoc==null)return null;
            if(cacheNode==null)myCache.put(diskLoc,indexNode);
        }
        DiskLoc diskLoc = indexNode.search(keys);
        if(diskLoc == null)return null;
        /**
         * 获得原始数据
         */
        return originalExtentManager.getRowFromDiskLoc(diskLoc);
    }
}
