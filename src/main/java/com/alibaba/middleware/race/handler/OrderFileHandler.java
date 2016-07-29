package com.alibaba.middleware.race.handler;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.decoupling.DiskLocQueues;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByBuyerCreateTimeOrderId;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByGoodOrderId;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByOrderId;
import com.alibaba.middleware.race.storage.DiskLoc;
import com.alibaba.middleware.race.storage.StoreType;

import java.io.IOException;

/**
 * Created by xiyuanbupt on 7/28/16.
 */
public class OrderFileHandler extends DataFileHandler{
    @Override
    void handleLine(String line, DiskLoc diskLoc) throws IOException, OrderSystem.TypeException, InterruptedException {
        diskLoc.setStoreType(StoreType.ORDERLINE);

        String[] kvs = line.split("\t");
        Long orderid = null;
        String buyerid = null;
        boolean sholdBreak = false;
        Long createtime = null;
        String goodid = null;
        for(String kv:kvs){
            int p = kv.indexOf(":");
            String key = kv.substring(0,p);
            String value = kv.substring(p+1);
            if(key.length()==0||value.length()==0){
                throw new RuntimeException("Bad data: " + line);
            }
            switch (key){
                case "orderid":
                    orderid = Long.parseLong(value);
                    if(buyerid!=null&&createtime!=null&&goodid!=null){
                        sholdBreak = true;
                    }
                    break;
                case "buyerid":
                    buyerid = value;
                    if(createtime!=null&&goodid!=null&&orderid!=null){
                        sholdBreak = true;
                    }
                    break;
                case "createtime":
                    createtime = Long.parseLong(value);
                    if(buyerid!=null&&goodid!=null&&orderid!=null){
                        sholdBreak = true;
                    }
                    break;
                case "goodid":
                    goodid = value;
                    if(buyerid!=null&&createtime!=null&&orderid!=null){
                        sholdBreak = true;
                    }
                    break;
                default:
                    break;
            }
            if(sholdBreak){
                break;
            }
        }


        ComparableKeysByOrderId orderIdKeys = new ComparableKeysByOrderId(orderid,diskLoc);
        DiskLocQueues.comparableKeysByOrderId.put(orderIdKeys);

        ComparableKeysByBuyerCreateTimeOrderId buyerCreateTimeOrderId = new ComparableKeysByBuyerCreateTimeOrderId(
                buyerid, createtime, orderid,diskLoc
        );
        DiskLocQueues.comparableKeysByBuyerCreateTimeOrderId.put(buyerCreateTimeOrderId);

        ComparableKeysByGoodOrderId goodOrderKeys = new ComparableKeysByGoodOrderId(
                goodid,orderid,diskLoc
        );
        DiskLocQueues.comparableKeysByGoodOrderId.put(goodOrderKeys);
    }
}
