package com.alibaba.middleware.race.decoupling;

import com.alibaba.middleware.race.codec.HashKeyHash;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByBuyerCreateTimeOrderId;
import com.alibaba.middleware.race.storage.IndexPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by xiyuanbupt on 7/26/16.
 */
public class BuyerTimeOrderPartionBuildThread extends  PartionBuildThread<ComparableKeysByBuyerCreateTimeOrderId>{

    public BuyerTimeOrderPartionBuildThread(AtomicInteger nRemain, CountDownLatch sendFinishSingle){
        super(nRemain,sendFinishSingle);
        this.keysQueue = DiskLocQueues.comparableKeysByBuyerCreateTimeOrderId;
        this.myPartions = indexNameSpace.mBuyerCreateTimeOrderPartion;
    }

    @Override
    protected void putIndexToPartion(ComparableKeysByBuyerCreateTimeOrderId comparableKeysByBuyerCreateTimeOrderId) {
        int hashCode = HashKeyHash.hashKeyHash(comparableKeysByBuyerCreateTimeOrderId.hashCode());
        myPartions.get(hashCode).addKey(comparableKeysByBuyerCreateTimeOrderId);
    }

    @Override
    protected void createBPlusTree() {
        List<Collection<IndexPartition<ComparableKeysByBuyerCreateTimeOrderId>>>
                partionsList = split(myPartions);
        int len = partionsList.size();
        CountDownLatch donSingle = new CountDownLatch(len);
        for(Collection<IndexPartition<ComparableKeysByBuyerCreateTimeOrderId>> partitions:partionsList){
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
