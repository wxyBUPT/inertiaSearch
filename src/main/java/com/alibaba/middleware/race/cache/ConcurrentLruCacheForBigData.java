package com.alibaba.middleware.race.cache;

/**
 * Created by xiyuanbupt on 7/24/16.
 * 继承与lru 队列,所有放入的元素都会缓存
 */
public class ConcurrentLruCacheForBigData<KEY,VALUE> extends ConcurrentLruCache<KEY,VALUE>{

    public ConcurrentLruCacheForBigData(int limit){
        super(limit);
    }

    @Override
    public VALUE get(KEY key) {
        VALUE value = map.get(key);
        if(value!=null) {
            removeThenAddKey(key);
        }
        return value;
    }

    public static void main(String[] args){
        ConcurrentLruCache<Integer,Integer> lruCache = new ConcurrentLruCacheForBigData<>(4);
        lruCache.put(1,2);
        System.out.println(lruCache.size());

    }
}
