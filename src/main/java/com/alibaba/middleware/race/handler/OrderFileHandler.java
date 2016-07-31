package com.alibaba.middleware.race.handler;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.decoupling.DiskLocQueues;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByBuyerCreateTimeOrderId;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByGoodOrderId;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByOrderId;
import com.alibaba.middleware.race.storage.DiskLoc;

import java.io.IOException;

/**
 * Created by xiyuanbupt on 7/28/16.
 */
public class OrderFileHandler extends DataFileHandler{

    void ihandleLine(String line, DiskLoc diskLoc) throws IOException, OrderSystem.TypeException, InterruptedException {
        Long orderid = null;
        String buyerid = null;
        Long createtime = null;
        String goodid = null;

        String[] kvs = line.split("\t");
        boolean sholdBreak = false;
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

    @Override
    void handleLine(String line,DiskLoc diskLoc) throws IOException,OrderSystem.TypeException,InterruptedException{

        /**
         * 处理行数据
         */
        Long orderid ;
        String buyerid ;
        Long createtime ;
        String goodid  ;
        String tmp;
        int p;
        p = line.indexOf("orderid:");
        tmp = line.substring(p+8);
        p = tmp.indexOf("\t");
        if(p!=-1){
            orderid = Long.parseLong(tmp.substring(0,p));
        }else orderid = Long.parseLong(tmp);

        p = line.indexOf("buyerid:");
        tmp = line.substring(p+8);
        p = tmp.indexOf("\t");
        if(p!=-1){
            buyerid = tmp.substring(0,p);
        }else buyerid = tmp;

        p = line.indexOf("createtime:");
        tmp = line.substring(p+11);
        p = tmp.indexOf("\t");
        if(p!=-1){
            createtime = Long.parseLong(tmp.substring(0,p));
        }else createtime = Long.parseLong(tmp);

        p = line.indexOf("goodid:");
        tmp = line.substring(p+7);
        p = tmp.indexOf("\t");
        if(p!=-1){
            goodid = tmp.substring(0,p);
        }else goodid = tmp;

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

    private void indexOfStrategy(String line){
    }

}
