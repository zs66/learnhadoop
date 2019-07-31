package com.zs.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Amoy
 * @date 2019/07/31
 * <p>
 * 需求：统计词频
 * Mapper中的四个类型：
 * KEYIN：输入的key的泛型，类型为Long，这里指的是每一行的偏移量，mapreduce底层文件输入依赖流（这里的流指的是字节流，不是字符流）的方式
 * VALUEIN：输入的是值的类型，这里指的是一行的内容
 * KEYOUT：输出的key的类型，指的就是单词
 * VALUEOUT：输出的value的类型，指的标记 1 ，便于统计
 * <p>
 * 当数据需要持久化到磁盘或进行网络传输的时候，必须进行序列化和反序列化
 * 序列化：原始数据 ---> 二进制
 * 反序列化：二进制 ---> 原始数据
 * mapreduce处理数据的时候必须经过持久化磁盘或者网络传输，所以数据必须经过序列化和反序列化
 * <p>
 * Java中的序列化和反序列化：连同 类结构 一起进行序列化和反序列化
 * 而Hadoop中序列化和反序列化：只对值进行序列化和反序列化，更加轻便
 * int <--> IntWritable
 * long <--> LongWritable
 * double <--> DoubleWritable
 * String <--> Text
 * ...
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**
     * 这个方法的调用频率：一行调用一次
     * @param key 每一行的起始偏移量
     * @param value 每一行的内容，每次读取到的那一行的内容
     * @param context 上下文对象
     * @throws IOException 抛出IO异常
     * @throws InterruptedException 异常
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(",");
        for (String word : words) {
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
