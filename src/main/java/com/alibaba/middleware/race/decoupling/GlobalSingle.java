package com.alibaba.middleware.race.decoupling;

import com.alibaba.middleware.race.RaceConf;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by xiyuanbupt on 8/1/16.
 * 保持一些全局信号量
 */
public class GlobalSingle {

    /**
     * 当前程序是否处于查询状态
     */
    public static boolean ISQUERYTIME = false;

    /**
     * 用于维持当前b 树创建线程还需要死多少
     */
    public static AtomicInteger buildThreadShouldDieCount = new AtomicInteger(
            RaceConf.PARTION_BUILD_DIE_COUNT
    );
}
