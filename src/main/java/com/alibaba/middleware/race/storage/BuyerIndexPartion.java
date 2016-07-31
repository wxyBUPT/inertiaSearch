package com.alibaba.middleware.race.storage;

import com.alibaba.middleware.race.RaceConf;
import com.alibaba.middleware.race.cache.ConcurrentLruCacheForMidData;
import com.alibaba.middleware.race.models.Row;

import java.io.Serializable;

/**
 * Created by xiyuanbupt on 7/31/16.
 */
public class BuyerIndexPartion<T extends Comparable<? super T>&Serializable&Indexable> extends IndexPartition<T>{

    public BuyerIndexPartion(int myHashCode){
        super(myHashCode);
        /**
         * 使用针对小数据量的缓存
         */
        myLRU = new ConcurrentLruCacheForMidData<>(RaceConf.N_BUYER_INDEX_CACHE_COUNT);
    }

    /**
     * 重写查询方法,缓存所有index 节点
     */
    @Override
    public synchronized Row queryByKey(T t){
        return queryByKeyForMinData(t);
    }
}
