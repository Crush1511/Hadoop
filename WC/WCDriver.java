package WC;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WCDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //1.Job声明对象
        Job job = Job.getInstance(new Configuration());
        //2.Job声明结构
            //map
        job.setMapperClass(WCMap.class);
            //reduce
        job.setReducerClass(WCReduce.class);
            //Driver
        job.setJarByClass(WCDriver.class);
        //3.job声明数据类型
            //map
        job.setMapOutputKeyClass(Text.class); //k2
        job.setMapOutputValueClass(IntWritable.class);
            //整个mr输出
        job.setOutputKeyClass(Text.class);//v2
        //4.生命输入\输出对象
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //5.提交
        boolean b = job.waitForCompletion(true);
    }
}
