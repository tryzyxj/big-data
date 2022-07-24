package com.yxjajxy.hadoop.week2.yarn;

import com.yxjajxy.hadoop.week2.yarn.map.AccessMapper;
import com.yxjajxy.hadoop.week2.yarn.reduce.AccessReducer;
import com.yxjajxy.hadoop.week2.yarn.writable.AccessWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * hadoop jar hadoop-1.0.0.jar com.yxjajxy.hadoop.week2.yarn.TrafficStatistics /yangjingyu/week2/input/HTTP_20130313143750.dat /yangjingyu/week2/output
 */
public class TrafficStatistics {

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.println("请输入作业参数: <input> <output>");
            System.exit(1);
        }

        String input = args[0];
        String output = args[1];

        System.setProperty("HADOOP_USER_NAME", "hadoop");

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(TrafficStatistics.class);
        job.setMapperClass(AccessMapper.class);
        job.setReducerClass(AccessReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AccessWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(AccessWritable.class);

        TextInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
