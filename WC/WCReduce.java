package WC;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WCReduce extends Reducer<Text, IntWritable,Text,IntWritable> {
    protected void reduce(Text key,Iterable<IntWritable> values,Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable value:values){
            sum+=1;
        }
        IntWritable reduceValue = new IntWritable();
        reduceValue.set(sum);
        context.write(key,reduceValue);
    }
}
