package com.yxjajxy.hadoop.week2.yarn.map;

import com.yxjajxy.hadoop.week2.yarn.writable.AccessWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AccessMapper extends Mapper<LongWritable, Text, Text, AccessWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, AccessWritable>.Context context) throws IOException, InterruptedException {
        String[] accesses = value.toString().split("\t");

        if (accesses.length != 11) {
            return;
        }

        String phone = accesses[1];
        long up = Long.parseLong(accesses[8]);
        long down = Long.parseLong(accesses[9]);

        context.write(new Text(phone),
                new AccessWritable(phone, up, down));
    }
}
