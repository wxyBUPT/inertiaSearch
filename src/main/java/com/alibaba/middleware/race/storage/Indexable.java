package com.alibaba.middleware.race.storage;

/**
 * Created by xiyuanbupt on 7/21/16.
 * 一个实现返回disklock 的接口
 */
public interface Indexable {
    DiskLoc getDataDiskLoc();
}
