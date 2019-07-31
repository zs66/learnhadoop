package com.zs.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Amoy
 * @date 2019/07/31
 *
 * reduce处理的是map端输出的内容
 * KEYIN：reduce端输入的key类型 <==> map端输出的key类型
 * VALUEIN：reduce端输入的value类型 <==> map端输出的value类型
 * KEYOUT：reduce输出的key类型
 * VALUEOUT：reduce输出的value类型
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    /**
     * 本方法调用频率：每一组调用一次
     *
     * map端输出的数据到达reduce端之前就会对数据进行分组，key相同的为一组，有多少个不同的key就有多少个组
     * @param key 相同的key就分为一个组
     * @param values 每一组中相同的key对应的所有的value值，可理解为一个key对应一个存放值的数组
     * @param context 上下文对象，用于传输，写出到hdfs
     * @throws IOException 抛出异常
     * @throws InterruptedException 抛出异常
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 统计结果，循环遍历values，求和
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
