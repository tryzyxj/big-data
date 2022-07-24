package com.yxjajxy.hadoop.week2.yarn.reduce;

import com.yxjajxy.hadoop.week2.yarn.writable.AccessWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AccessReducer extends Reducer<Text, AccessWritable, NullWritable, AccessWritable> {

    @Override
    protected void reduce(Text key, Iterable<AccessWritable> values, Reducer<Text, AccessWritable, NullWritable, AccessWritable>.Context context) throws IOException, InterruptedException {

        String phone = key.toString();
        long upSum = 0;
        long downSum = 0;

        for (AccessWritable accessWritable : values) {
            upSum += accessWritable.getUp();
            downSum += accessWritable.getDown();
        }

        context.write(NullWritable.get(),
                new AccessWritable(phone, upSum, downSum));
    }
}
