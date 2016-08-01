package com.alibaba.middleware.race.decoupling;

/**
 * Created by xiyuanbupt on 8/1/16.
 * 保持一些全局信号量
 */
public class GlobalSingle {

    /**
     * 当前程序是否处于查询状态
     */
    public static boolean ISQUERYTIME = false;
}
