package com.yxjajxy.hadoop.week2.hbase;

import com.yxjajxy.hadoop.week2.hbase.entity.Item;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * java -cp hadoop-1.0.0.jar com.yxjajxy.hadoop.week2.hbase.HBaseApp main
 */
public class HBaseApp {

    public static void main(String[] args) throws IOException {
        MyHBase myHBase = new MyHBase("101.34.255.120", 2181, "101.34.255.120:16000");
        myHBase.createNamespace("yangjingyu");
        myHBase.createTable("yangjingyu:student", "name", "info", "score");

        List<Item> items1 = new ArrayList<Item>();
        items1.add(new Item("name", "", "Tom"));
        items1.add(new Item("info", "student_id", "20210000000001"));
        items1.add(new Item("info", "class", "1"));
        items1.add(new Item("score", "understanding", "75"));
        items1.add(new Item("score", "programming", "82"));
        myHBase.addItem("yangjingyu:student", 1, items1);

        List<Item> items2 = new ArrayList<Item>();
        items2.add(new Item("name", "", "Jerry"));
        items2.add(new Item("info", "student_id", "20210000000002"));
        items2.add(new Item("info", "class", "1"));
        items2.add(new Item("score", "understanding", "85"));
        items2.add(new Item("score", "programming", "67"));
        myHBase.addItem("yangjingyu:student", 2, items2);

        List<Item> items3 = new ArrayList<Item>();
        items3.add(new Item("name", "", "Jack"));
        items3.add(new Item("info", "student_id", "20210000000003"));
        items3.add(new Item("info", "class", "2"));
        items3.add(new Item("score", "understanding", "80"));
        items3.add(new Item("score", "programming", "80"));
        myHBase.addItem("yangjingyu:student", 3, items3);

        List<Item> items4 = new ArrayList<Item>();
        items4.add(new Item("name", "", "Rose"));
        items4.add(new Item("info", "student_id", "20210000000004"));
        items4.add(new Item("info", "class", "2"));
        items4.add(new Item("score", "understanding", "60"));
        items4.add(new Item("score", "programming", "61"));
        myHBase.addItem("yangjingyu:student", 4, items4);

        List<Item> items5 = new ArrayList<Item>();
        items5.add(new Item("name", "", "杨景玉"));
        items5.add(new Item("info", "student_id", "G20190343010073"));
        myHBase.addItem("yangjingyu:student", 5, items5);

        myHBase.close();
    }
}
