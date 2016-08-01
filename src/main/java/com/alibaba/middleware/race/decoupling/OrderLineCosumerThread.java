package com.alibaba.middleware.race.decoupling;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByBuyerCreateTimeOrderId;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByGoodOrderId;
import com.alibaba.middleware.race.models.comparableKeys.ComparableKeysByOrderId;
import com.alibaba.middleware.race.storage.DiskLoc;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Created by xiyuanbupt on 8/1/16.
 * 消费队列中的order 信息
 */
public class OrderLineCosumerThread implements Runnable{

    private static Logger LOG = Logger.getLogger(OrderLineCosumerThread.class.getName());
    /**
     * 本线程需要消费的队列
     */
    private LinkedBlockingDeque<OrderLineWithDiskLoc> lineQueues;

    /**
     * 如果当前线程处理完成,需要countDown以告知之后的线程我处理完成了
     */
    private AtomicInteger nThreadRemain;

    private final AtomicInteger nRemain;

    public OrderLineCosumerThread(final AtomicInteger nRemain,AtomicInteger nThreadRemain){
        this.nRemain = nRemain;
        this.nThreadRemain = nThreadRemain;
        lineQueues = DiskLocQueues.originalOrderLineWithDisklocQueues;
    }

    @Override
    public void run() {
        while(true){
            try{
                OrderLineWithDiskLoc orderLineWithDiskLoc = lineQueues.poll(1, TimeUnit.SECONDS);
                if(orderLineWithDiskLoc==null){
                    /**
                     * 如果所有读文件线程都完成
                     */
                    if(nRemain.get()==0){
                        LOG.info("finish decode all order data");
                        break;
                    }
                    /**
                     * 还有数据没有读,避免处理空数据
                     */
                    continue;
                }
                /**
                 * 当数据不为空的时候,处理行数据
                 */
                String line = orderLineWithDiskLoc.getLine();
                DiskLoc diskLoc = orderLineWithDiskLoc.getDiskLoc();
                handleLine(line,diskLoc);
            }catch (Exception e){
                e.printStackTrace();
                System.exit(-1);
            }
        }
        /**
         * 已经处理完所有的数据
         */
        nThreadRemain.decrementAndGet();
    }
    private void handleLine(String line, DiskLoc diskLoc) throws IOException,OrderSystem.TypeException,InterruptedException{
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
}
