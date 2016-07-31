package com.alibaba.middleware.race.decoupling;

import com.alibaba.middleware.race.codec.HashKeyHash;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByGoodId;
import com.alibaba.middleware.race.storage.IndexPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by xiyuanbupt on 7/27/16.
 */
public class GoodPartionBuildThread extends PartionBuildThread<ComparableKeysByGoodId>{



    public GoodPartionBuildThread(AtomicInteger nRemain, CountDownLatch sendFinishSingle){
        super(nRemain,sendFinishSingle);
        this.keysQueue = DiskLocQueues.comparableKeysByGoodIdQueue;
        this.myPartions = indexNameSpace.mGood;
    }
    @Override
    protected void putIndexToPartion(ComparableKeysByGoodId comparableKeysByGoodId) {
        int hashCode = HashKeyHash.hashKeyHash(comparableKeysByGoodId.hashCode());
        myPartions.get(hashCode).addKey(comparableKeysByGoodId);
    }


    @Override
    protected void createBPlusTree() {
        for(Map.Entry<Integer,IndexPartition<ComparableKeysByGoodId>> entry:myPartions.entrySet()){
            /**
             * 在partion 中创建btree
             */
            entry.getValue().merageAndBuildMe();
        }
    }
}
