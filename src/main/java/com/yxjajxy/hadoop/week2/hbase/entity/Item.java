package com.yxjajxy.hadoop.week2.hbase.entity;

public class Item {

    private String colFamily;
    private String col;
    private String val;

    public Item(String colFamily, String col, String val) {
        this.colFamily = colFamily;
        this.col = col;
        this.val = val;
    }

    public String getColFamily() {
        return colFamily;
    }

    public String getCol() {
        return col;
    }

    public String getVal() {
        return val;
    }
}
