package com.alibaba.middleware.race.cache;

import com.alibaba.middleware.race.storage.IndexNode;

/**
 * Created by xiyuanbupt on 7/24/16.
 * 一个有优先级的队列(本项目中,非叶子节点的优先级要比叶子节点的优先级高,故在队列的前面)
 */
public class ConcurrentLruCacheForMidData<KEY,VALUE extends IndexNode> extends ConcurrentLruCache<KEY,VALUE>{

    public ConcurrentLruCacheForMidData(int limit){
        super(limit);
    }

    @Override
    public VALUE get(KEY key) {
        VALUE value = map.get(key);
        /**
         * 非叶子节点优先级更高
         */
        if(value!=null && !value.isLeafNode()) {
            removeThenAddKey(key);
        }
        return value;
}
}
