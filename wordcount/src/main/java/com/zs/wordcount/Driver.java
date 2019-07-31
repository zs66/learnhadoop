package com.zs.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * <p>
 * 驱动类：代码提交类
 * </p>
 * mapreduce代码运行的3种方式：
 *   1、将代码打成jar包，提交到集群运行，真实生产中用的，缺点是不便于代码的调试和修改
 *   2、
 *
 * @author Amoy
 * @date 2019/07/31
 */
public class Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 加载配置文件
        Configuration conf = new Configuration();
        //conf.set("fs.defaultFS", "hdfs://Hadoop01:9000");
        // 启动一个job，封装计算程序的mapper和reducer
        Job job = Job.getInstance(conf);

        // 设置计算程序的主驱动类，运行的时候打成jar包运行
        job.setJarByClass(Driver.class);

        // 设置mapper和reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 设置mapper的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置reduce的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输入路径和输出路径
        FileInputFormat.addInputPath(job, new Path("/tb_users/input"));
        FileOutputFormat.setOutputPath(job, new Path("/tb_users/output"));

        // 提交job
        job.waitForCompletion(true);
    }
}
