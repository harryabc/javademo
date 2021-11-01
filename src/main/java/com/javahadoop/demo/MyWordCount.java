package com.javahadoop.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.StringTokenizer;

public class MyWordCount {

    /**
     * Map:读取输入的文件
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //获得每行数据
            String line = value.toString();
            //分割字符串
            //String[] words = line.split(" ");
            StringTokenizer stringTokenizer = new StringTokenizer(line);
            //遍历
            while (stringTokenizer.hasMoreTokens()) {
                //获取每个值
                String wordValue = stringTokenizer.nextToken();
                //设置map输出的key值
                word.set(wordValue);
                //上下文输出map的key和value
                context.write(word, one);
            }
        }
    }

    /**
     * Reduce
     */
    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //用于累加的变量
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    private static void deleteOutDir(Configuration conf, String OUT_DIR) throws IOException, URISyntaxException {
        FileSystem fileSystem = FileSystem.get(new URI(OUT_DIR), conf);
        if (fileSystem.exists(new Path(OUT_DIR))) {
            fileSystem.delete(new Path(OUT_DIR), true);
        }
        fileSystem.close();
    }

    public static void main(String[] args) throws Exception {
        //设置参数
        args = new String[]{"hdfs://73c7ed1999df.dev.staros.local:31490/WordCount/input", "hdfs://73c7ed1999df.dev.staros.local:31490/WordCount/output"};
        //获取配置信息
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", "hdfs://73c7ed1999df.dev.staros.local:31490");
        //删除已存在的output
        deleteOutDir(configuration, args[1]);
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage:wordcount <in> <out>");
            System.exit(2);
        }
        //创建job并且命名
        Job job = Job.getInstance(configuration, "mywordcount");
        //1.设置job运行的类
        job.setJarByClass(MyWordCount.class);
        //2.设置Map和reduce类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        //3.设置输入输出的文件目录
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //4.设置输出结果的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //5.提交job等待运行结果，并在客户端显示运行信息
        boolean isSuccess = job.waitForCompletion(true);
        System.out.println("result:" + isSuccess);
        //结束程序
        System.exit(isSuccess ? 0 : 1);
    }
}
