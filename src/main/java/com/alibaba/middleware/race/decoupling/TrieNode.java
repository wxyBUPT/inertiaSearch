package com.alibaba.middleware.race.decoupling;

import java.util.HashSet;

/**
 * Created by xiyuanbupt on 8/27/16.
 * 26 个英文字母的Trie
 * 用于统计单词出现的次数
 */
public class TrieNode {
    /**
     * 26个字符,也就是26叉树
     */
    public TrieNode[] childNodes;

    /**
     * 词频统计
     */
    public int freq;

    /**
     * 用于记录该节点的字符
     */
    public char nodeChar;

    /**
     * 插入记录时的编码id
     */
    public HashSet<Integer> hashSet = new HashSet<>();

    /**
     * 初始化
     */
    public TrieNode(){
        childNodes = new TrieNode[26];
        freq = 0;
    }

    /**
     * 插入操作
     */
    public void AddTrieNode(TrieNode root,String word,int id){
        if(word.length()==0)return;

        /**
         * 求字符地址,方便将该字符放入到26叉树中的某一个叉中
         */
        int k =  word.charAt(0) - 'a';

        /**
         * 如果该叉为空,则初始化
         */
        if(root.childNodes[k] == null){
            root.childNodes[k] = new TrieNode();

            /**
             * 记录下当前字符
             */
            root.childNodes[k].nodeChar = word.charAt(0);
        }

        /**
         * 该id 途径的节点
         */

        root.childNodes[k].hashSet.add(id);

        String nextWord = word.substring(1);

        if(nextWord.length()==0){
            root.childNodes[k].freq++;
        }

        AddTrieNode(root.childNodes[k],nextWord,id);

    }

    /**
     * 删除操作
     */

    public void deleteTrieNode(TrieNode root,String word,int id){
        if(word.length()==0){
            return ;
        }

        /**
         * 求字符地址
         */
        int k = word.charAt(0)-'a';

        /**
         * 如果该叉树为空,则说明没有找到删除的点
         */
        if(root.childNodes[k]==null)
            return;

        String nextWord = word.substring(1);

        /**
         * 如果是最后一个单词,则减去词频
         */
        if(word.length()==0 &&root.childNodes[k].freq>0)root.childNodes[k].hashSet.remove(id);

        deleteTrieNode(root.childNodes[k],nextWord,id);
    }
}
