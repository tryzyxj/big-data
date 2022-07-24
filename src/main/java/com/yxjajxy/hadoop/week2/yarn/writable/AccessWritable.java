package com.yxjajxy.hadoop.week2.yarn.writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AccessWritable implements Writable {

    private String phone;
    private long up;
    private long down;
    private long sum;

    public AccessWritable() {

    }

    public AccessWritable(String phone, long up, long down) {
        this.phone = phone;
        this.up = up;
        this.down = down;
        this.sum = up + down;
    }

    @Override
    public String toString() {
        return phone + '\t' + up + '\t' + down + '\t' + sum;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(phone);
        out.writeLong(up);
        out.writeLong(down);
        out.writeLong(sum);
    }

    public void readFields(DataInput in) throws IOException {
        this.phone = in.readUTF();
        this.up = in.readLong();
        this.down = in.readLong();
        this.sum = in.readLong();
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public long getUp() {
        return up;
    }

    public void setUp(long up) {
        this.up = up;
    }

    public long getDown() {
        return down;
    }

    public void setDown(long down) {
        this.down = down;
    }

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }
}
