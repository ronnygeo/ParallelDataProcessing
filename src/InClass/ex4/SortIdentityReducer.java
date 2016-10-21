package InClass.ex4;
//package cs6240;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.io.InterruptedIOException;

/**
 * Created by ronnygeo on 10/7/16.
 */
public class SortIdentityReducer extends Reducer<Text,NullWritable,Text,NullWritable> {
    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException{
        context.write(key, NullWritable.get());
    }
}
