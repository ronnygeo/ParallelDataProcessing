package InClass.ex4;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class SampleMapper 
extends Mapper<Object,Text,NullWritable, Text> {
    public void map(Object key, Text line, 
            Context context) throws IOException, InterruptedException {
        if (Math.random() < 0.05) {
            context.write(NullWritable.get(), line);
        }
    }
}
 
