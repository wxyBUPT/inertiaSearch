package com.alibaba.middleware.race.codec;

import com.alibaba.middleware.race.RaceConf;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByOrderId;
import com.alibaba.middleware.race.storage.Indexable;

import java.util.HashMap;

/**
 * Created by xiyuanbupt on 7/26/16.
 * 只有一个函数,将不同的key 值hash 到不同的分片
 *
 */
public class HashKeyHash {

    public static int hashKeyHash(int hashCode){
        return hashCode% RaceConf.N_PARTITION;
    }

    public static void main(String[] args){
        HashMap<Integer,Integer> map = new HashMap<>();
        for(int i = -(100000*RaceConf.N_PARTITION);i<10000* RaceConf.N_PARTITION;i++){
            map.put(hashKeyHash(Math.abs(i)),2);
        }
        System.out.println(map.size());

        ComparableKeysByOrderId comparableKeysByOrderId = new ComparableKeysByOrderId(58455780174L,null);
        System.out.println(hashKeyHash(comparableKeysByOrderId.hashCode()));

        comparableKeysByOrderId = new ComparableKeysByOrderId(53365522365L,null);
        System.out.println(hashKeyHash(comparableKeysByOrderId.hashCode()));
        comparableKeysByOrderId = new ComparableKeysByOrderId(587756401L,null);
        System.out.println(hashKeyHash(comparableKeysByOrderId.hashCode()));
        comparableKeysByOrderId = new ComparableKeysByOrderId(59490325730L,null);
        System.out.println(hashKeyHash(comparableKeysByOrderId.hashCode()));
        comparableKeysByOrderId = new ComparableKeysByOrderId(53365522365L,null);
        System.out.println(hashKeyHash(comparableKeysByOrderId.hashCode()));
    }
}
