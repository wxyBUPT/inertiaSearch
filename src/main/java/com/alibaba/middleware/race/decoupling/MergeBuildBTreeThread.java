package com.alibaba.middleware.race.decoupling;

import com.alibaba.middleware.race.storage.IndexPartition;
import com.alibaba.middleware.race.storage.Indexable;

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;

/**
 * Created by xiyuanbupt on 8/1/16.
 * 执行归并排序和创建b 树的线程
 */
public class MergeBuildBTreeThread<T extends Comparable<? super T>&Indexable&Serializable> implements Runnable{

    Collection<IndexPartition<T>> partitions;
    CountDownLatch doneSingle;

    @Override
    public void run() {
        for(IndexPartition<T> partition:partitions){
            /**
             * 如果处于查询阶段,则可以停止当前线程,构建线程可以交给查询线程
             */
            if(GlobalSingle.ISQUERYTIME){
                /**
                 * 如果还有需要死的线程,则停止当前线程
                 * 哪个线程停止有随机效应
                 */
                if(GlobalSingle.buildThreadShouldDieCount.decrementAndGet()>=0){

                    break;
                }
            }
            partition.merageAndBuildMe();
        }
        doneSingle.countDown();
    }

    public MergeBuildBTreeThread(Collection<IndexPartition<T>> partitions,CountDownLatch doneSingle){
        this.partitions = partitions;
        this.doneSingle = doneSingle;
    }
}
