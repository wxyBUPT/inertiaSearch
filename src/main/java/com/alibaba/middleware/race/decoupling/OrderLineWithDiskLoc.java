package com.alibaba.middleware.race.decoupling;

import com.alibaba.middleware.race.storage.DiskLoc;

/**
 * Created by xiyuanbupt on 8/1/16.
 * 简单的数据结构,里面有orderString 和这个orderString 的diskloc 的信息
 */
public class OrderLineWithDiskLoc {
    private String line;
    private DiskLoc diskLoc;

    public OrderLineWithDiskLoc(String line,DiskLoc diskLoc){
        this.line = line;
        this.diskLoc = diskLoc;
    }

    public String getLine(){
        return line;
    }

    public DiskLoc getDiskLoc(){
        return diskLoc;
    }
}
