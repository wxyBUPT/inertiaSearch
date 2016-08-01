package com.alibaba.middleware.race.decoupling;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Vector;

/**
 * Created by xiyuanbupt on 7/29/16.
 */
public class ShellSort <T extends Comparable<? super T>>{
    /**
     * 执行希尔排序
     * @param input
     * @return
     */
    public List<T> shellsort(Vector<T> input){
        int size = input.size();
        int j;
        for(int dk = size/2; dk>=1;dk = dk/2){
            for(int i = dk;i<size;++i){
                T tmp = input.get(i);
                for(j=i;j>=dk && tmp.compareTo(input.get(j-dk))<0;j-=dk){
                    input.set(j,input.get(j-dk));
                }
                input.set(j,tmp);
            }
        }
        return input;
    }

    /**
     * This method generate a ArrayList with length n containing random integers .
     * @param n the length of the ArrayList to generate.
     * @return ArrayList of random integers with length n.
     */
    private List<Integer> generateRandomNumbers(int n){

        List<Integer> list = new ArrayList<Integer>(n);
        Random random = new Random();

        for (int i = 0; i < n; i++) {
            list.add(random.nextInt(n * 10));
        }

        return list;
    }
    /**
     * Main method.
     * @param args
     */
    public static void main(String[] args) {
    }
}
