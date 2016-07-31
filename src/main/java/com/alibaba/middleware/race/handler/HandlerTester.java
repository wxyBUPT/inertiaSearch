package com.alibaba.middleware.race.handler;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by xiyuanbupt on 7/31/16.
 * 主要为了测试两种处理文件的方式哪个更快
 */
public class HandlerTester {
    private void indexofStra(String line){
        Long orderid = null;
        String buyerid = null;
        Long createtime = null;
        String goodid = null;
        String tmp;
        int p;
        p = line.indexOf("orderid:");
        tmp = line.substring(p+8);
        p = line.indexOf("\t");
        if(p!=-1){
            orderid = Long.parseLong(tmp.substring(0,p));
        }else orderid = Long.parseLong(tmp);

        p = line.indexOf("buyerid:");
        tmp = line.substring(p+8);
        p = line.indexOf("\t");
        if(p!=-1){
            buyerid = tmp.substring(0,p);
        }else buyerid = tmp;

        p = line.indexOf("createtime:");
        tmp = line.substring(p+11);
        p = line.indexOf("\t");
        if(p!=-1){
            createtime = Long.parseLong(tmp.substring(0,p));
        }else createtime = Long.parseLong(tmp);

        p = line.indexOf("goodid:");
        tmp = line.substring(p+7);
        p = line.indexOf("\t");
        if(p!=-1){
            goodid = tmp.substring(0,p);
        }else goodid = tmp;
    }

    private void splitLine(String line){
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
                    System.out.println(orderid);
                    if(buyerid!=null&&createtime!=null&&goodid!=null){
                        sholdBreak = true;
                    }
                    break;
                case "buyerid":
                    buyerid = value;
                    System.out.println(buyerid);
                    if(createtime!=null&&goodid!=null&&orderid!=null){
                        sholdBreak = true;
                    }
                    break;
                case "createtime":
                    System.out.println(value);
                    createtime = Long.parseLong(value);
                    if(buyerid!=null&&goodid!=null&&orderid!=null){
                        sholdBreak = true;
                    }
                    break;
                case "goodid":
                    System.out.println(value);
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
    }

    void handleFile1(String file){
        try {
            BufferedReader bfr = new BufferedReader(new FileReader(file));
            String line = bfr.readLine();
            while(line!=null){
                indexofStra(line);
            }
        }catch (Exception e){

        }
    }

    void handleFile2(String file){
        try{
            BufferedReader bfr = new BufferedReader(new FileReader(file));
            String line = bfr.readLine();
            while(line!=null){
                splitLine(line);
            }
        }catch (Exception e){

        }
    }

    void test(){
        List<String> orderFiles = new ArrayList<>();
        orderFiles.add("/dir0/orders/order.0.0");
        orderFiles.add("/dir1/orders/order.1.1");
        orderFiles.add("/dir2/orders/order.2.2");
        orderFiles.add("/dir0/orders/order.0.3");
        Long startTime = System.currentTimeMillis();
        for(String file:orderFiles){
            handleFile1(file);
            System.out.println("我处理了一些文件");
        }
        Long endTime = System.currentTimeMillis();
        Long tmp = endTime - startTime;
        System.out.println("" + tmp);

        startTime = System.currentTimeMillis();
        for(String file:orderFiles){
            System.out.println("我在处理文件");
            handleFile2(file);
        }
        endTime = System.currentTimeMillis();
        tmp = endTime - startTime;
        System.out.println("" + tmp);
    }

    public static void main(String[] args){
        new HandlerTester().test();
    }
}
