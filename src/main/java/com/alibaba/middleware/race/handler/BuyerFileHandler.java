package com.alibaba.middleware.race.handler;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.decoupling.DiskLocQueues;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByBuyerId;
import com.alibaba.middleware.race.storage.DiskLoc;
import com.alibaba.middleware.race.storage.StoreType;

import java.io.IOException;

/**
 * Created by xiyuanbupt on 7/28/16.
 * 处理buyerline
 */
public class BuyerFileHandler extends DataFileHandler{
    /**
     * 下面两个声明是为了减少处理行数据的时候多次声明
     */
    void xhandleLine(String line, DiskLoc diskLoc) throws IOException, OrderSystem.TypeException, InterruptedException {

        String buyerid;
        String[] kvs = line.split("\t");
        buyerid = null;
        for(String kv:kvs){
            int p = kv.indexOf(":");
            String key = kv.substring(0,p);
            String value = kv.substring(p+1);
            if(key.length()==0||value.length()==0){
                throw new RuntimeException("Bad data: " + line);
            }
            if(key.compareTo("buyerid")==0){
                buyerid = value;
                break;
            }
        }

        /**
         * Put index info to queue
         */
        ComparableKeysByBuyerId key = new ComparableKeysByBuyerId(buyerid,diskLoc);
        DiskLocQueues.comparableKeysByBuyerIdQueue.put(key);
    }

    @Override
    void handleLine(String line,DiskLoc diskLoc)throws IOException,OrderSystem.TypeException,InterruptedException{

        String buyerid ;
        int i;
        i = line.indexOf("buyerid:");
        line = line.substring(i+8);
        i = line.indexOf("\t");
        if(i!=-1){
            buyerid = line.substring(0,i);
        }else buyerid = line;

        /**
         * Put index info to queue
         */
        ComparableKeysByBuyerId key = new ComparableKeysByBuyerId(buyerid,diskLoc);
        DiskLocQueues.comparableKeysByBuyerIdQueue.put(key);
    }
}
