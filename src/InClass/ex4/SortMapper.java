package InClass.ex4;
//package cs6240;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SortMapper 
extends Mapper<Object, Text, Text, NullWritable> {
    NullWritable nw = NullWritable.get();

    public void map(Object key, Text line, 
            Context context) throws IOException, InterruptedException {
        context.write(line, nw);
    }
}
 
