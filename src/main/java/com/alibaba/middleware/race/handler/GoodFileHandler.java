package com.alibaba.middleware.race.handler;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.decoupling.DiskLocQueues;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByGoodId;
import com.alibaba.middleware.race.storage.DiskLoc;
import com.alibaba.middleware.race.storage.StoreType;

import java.io.IOException;

/**
 * Created by xiyuanbupt on 7/28/16.
 */
public class GoodFileHandler extends DataFileHandler{

    /**
     * 下面声明是为了减少处理行数据的时候的声明
     */
    void xhandleLine(String line, DiskLoc diskLoc) throws IOException, OrderSystem.TypeException, InterruptedException {
        /**
         * Find goodid and salerid
         */
        String goodid = null;
        String[] kvs = line.split("\t");
        for(String kv: kvs){
            int p = kv.indexOf(":");
            String key = kv.substring(0,p);
            String value = kv.substring(p+1);
            if(key.length()==0||value.length()==0){
                throw new RuntimeException("Bad data: " + line);
            }
            if(key.equals("goodid")){
                goodid = value;
                break;
            }
        }

        if(goodid==null ){
            throw new RuntimeException("Bad data! goodid " + goodid  );
        }
        ComparableKeysByGoodId goodIdKeys = new ComparableKeysByGoodId(goodid,diskLoc);
        DiskLocQueues.comparableKeysByGoodIdQueue.put(goodIdKeys);
    }

    @Override
    void handleLine(String line,DiskLoc diskLoc) throws IOException,OrderSystem.TypeException,InterruptedException{
        String goodid;
        int p;
        p = line.indexOf("goodid:");
        line = line.substring(p+7);
        p = line.indexOf("\t");
        if(p!=-1){
            goodid = line.substring(0,p);
        }else goodid = line;

        /**
         * Put index info to queue
         */
        ComparableKeysByGoodId goodIdKeys = new ComparableKeysByGoodId(goodid,diskLoc);
        DiskLocQueues.comparableKeysByGoodIdQueue.put(goodIdKeys);
    }
}
