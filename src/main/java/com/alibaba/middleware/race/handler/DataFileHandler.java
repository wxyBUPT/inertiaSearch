package com.alibaba.middleware.race.handler;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.storage.*;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.logging.Logger;

/**
 * Created by xiyuanbupt on 7/28/16.
 */
public abstract class DataFileHandler{

    private static final Logger LOG = Logger.getLogger(DataFileHandler.class.getName());


    protected OriginalExtentManager originalExtentManager;
    abstract void handleLine(String line,DiskLoc diskLoc) throws IOException,OrderSystem.TypeException,InterruptedException;

    public void handle(Collection<String> files) throws InterruptedException,IOException,OrderSystem.TypeException{
        this.originalExtentManager= OriginalExtentManager.getInstance();
        for(String file:files) {
            LOG.info("Start handle file: " + file);
            handleFile(file);
        }
    }

    private void handleFile(String file){
        /**
         * 可以从一个file 中创建多个extent.并放入originalExtentManager 中
         */
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            FileChannel channel = raf.getChannel();

            BufferedReader bfr = createReader(file);


            /**
             * 当前extent 的开始位置
             */
            Long currentExtentPosition = 0L;

            /**
             * 当前extent 的逻辑标号
             */

            int currentExtentNum = originalExtentManager.applyExtentNo();

            /**
             * 当前行数的位置
             */
            Long currentLinePosition = 0L;


            /**
             * 当前extent 的位置
             */
            Long currentExtentEnd = 0L;

            /**
             * 当前待处理的数据行
             */
            String line = bfr.readLine();

            /**
             * 当前extent 的位置不包括最后一个换行符
             */
            currentExtentEnd = currentExtentEnd + line.length();

            /**
             * 在一个extent 中已经处理的行数
             */
            Integer lineCount = 0;
            while (line!=null){
                if(lineCount>=1000000){
                    LOG.info("创建新的Extent ,标号是 + " + currentExtentNum);
                    originalExtentManager.putExtent(
                            new OrigionExtent(
                                    channel,
                                    currentExtentPosition,
                                    currentExtentEnd - currentExtentPosition,
                                    currentExtentNum)
                    );
                    /**
                     * 更新当前extent 的位置,同时更新当前extent 的逻辑标号
                     */
                    currentExtentPosition = currentExtentEnd + 1;
                    currentExtentNum = originalExtentManager.applyExtentNo();
                    lineCount = 0;
                }
                int lineByteSize = line.getBytes("UTF-8").length;
                DiskLoc diskLoc = new DiskLoc(currentExtentNum,
                        currentLinePosition.intValue()-currentExtentPosition.intValue(),
                        lineByteSize);
                /**
                 * 当前位置越过一个 /r
                 */
                lineCount ++;
                currentExtentEnd = currentLinePosition + lineByteSize;
                currentLinePosition = currentLinePosition + lineByteSize + 1;
                handleLine(line,diskLoc);
                line = bfr.readLine();
            }

            /**
             * 要定位最后的extent
             */
            if(currentExtentPosition<channel.size()) {
                originalExtentManager.putExtent(
                        new OrigionExtent(
                                channel,
                                currentExtentPosition,
                                currentExtentEnd - currentExtentPosition,
                                currentExtentNum
                        )
                );
            }
        }catch (Exception e){
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private BufferedReader createReader(String file) throws FileNotFoundException {
        return new BufferedReader(new FileReader(file));
    }
}
