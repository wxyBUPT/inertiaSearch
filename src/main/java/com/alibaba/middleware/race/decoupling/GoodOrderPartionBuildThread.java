package com.alibaba.middleware.race.decoupling;

import com.alibaba.middleware.race.codec.HashKeyHash;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByGoodOrderId;
import com.alibaba.middleware.race.storage.IndexPartition;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by xiyuanbupt on 7/26/16.
 * 创建 goodid + orderid 索引的线程
 */
public class GoodOrderPartionBuildThread extends PartionBuildThread<ComparableKeysByGoodOrderId>{

    public GoodOrderPartionBuildThread(AtomicInteger nRemain, CountDownLatch sendFinishSingle){
        super(nRemain,sendFinishSingle);
        this.keysQueue = DiskLocQueues.comparableKeysByGoodOrderId;
        this.myPartions = indexNameSpace.mGoodOrderPartions;
    }

    @Override
    protected void putIndexToPartion(ComparableKeysByGoodOrderId comparableKeysByGoodOrderId) {
        int hasCode = HashKeyHash.hashKeyHash(comparableKeysByGoodOrderId.hashCode());
        myPartions.get(hasCode).addKey(comparableKeysByGoodOrderId);
    }

    /**
     * 重构,目的是按照partion 的数目分成多路构建
     */
    @Override
    protected void createBPlusTree() {
        for(Map.Entry<Integer,IndexPartition<ComparableKeysByGoodOrderId>> entry: myPartions.entrySet()){
            entry.getValue().merageAndBuildMe();
        }
    }
}
