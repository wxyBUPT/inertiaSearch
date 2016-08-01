package com.alibaba.middleware.race.decoupling;

import com.alibaba.middleware.race.codec.HashKeyHash;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByGoodId;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByGoodOrderId;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByOrderId;
import com.alibaba.middleware.race.storage.IndexPartition;
import com.sun.tools.javah.Util;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by xiyuanbupt on 7/26/16.
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
        List<Collection<IndexPartition<ComparableKeysByGoodOrderId>>>
                partionsList = split(myPartions);
        int len = partionsList.size();
        CountDownLatch donSingle = new CountDownLatch(len);
        for(Collection<IndexPartition<ComparableKeysByGoodOrderId>> partitions:partionsList){
            new Thread(
                    new MergeBuildBTreeThread<>(partitions,donSingle)
            ).start();
        }
        try{
            donSingle.await();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
