package WC;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WCMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        String valueString = value.toString();
        String[] words = valueString.split(" ");

        Text mapKey = new Text();
        IntWritable mapValue = new IntWritable();
        for(String word:words){
            mapKey.set(word);
            mapValue.set(1);
            context.write(mapKey,mapValue);
        }
    }
}
