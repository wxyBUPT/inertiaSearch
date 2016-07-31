package com.alibaba.middleware.race.decoupling;

import com.alibaba.middleware.race.codec.HashKeyHash;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByBuyerId;
import com.alibaba.middleware.race.storage.BuyerIndexPartion;
import com.alibaba.middleware.race.storage.IndexPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by xiyuanbupt on 7/27/16.
 */
public class BuyerPartionBuildThread extends PartionBuildThread<ComparableKeysByBuyerId>{

    private HashMap<Integer,BuyerIndexPartion<ComparableKeysByBuyerId>> myPartions;

    @Override
    protected void putIndexToPartion(ComparableKeysByBuyerId comparableKeysByBuyerId) {
        int hashCode = HashKeyHash.hashKeyHash(comparableKeysByBuyerId.hashCode());
        myPartions.get(hashCode).addKey(comparableKeysByBuyerId);
    }

    @Override
    protected void createBPlusTree() {
        for(Map.Entry<Integer,BuyerIndexPartion<ComparableKeysByBuyerId>> entry:myPartions.entrySet()){
            entry.getValue().merageAndBuildMe();
        }
    }

    @Override
    public void run(){
        super.run();
        /**
         * 在buyer 中创建b树
         */
        createBPlusTree();
        LOG.info("BuyerPartionIndex data have been insertd,Now enjoy Search !!!");
    }

    public BuyerPartionBuildThread(AtomicInteger nRemain, CountDownLatch sendFinishSingle){
        super(nRemain,sendFinishSingle);
        this.keysQueue = DiskLocQueues.comparableKeysByBuyerIdQueue;
        this.myPartions = indexNameSpace.mBuyer;
    }
}
