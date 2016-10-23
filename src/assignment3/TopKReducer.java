package assignment3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ronnygeo on 10/22/16.
 */
public class TopKReducer extends Reducer<DoubleWritable,Text,Text,DoubleWritable> {
    int K;
    public void setup(Context ctx) {
        K = ctx.getConfiguration().getInt("K", 100);
    }

    public void reduce(DoubleWritable key, Iterable<Text> nodes, Context ctx) throws InterruptedException, IOException{
        for (Text node: nodes) {
            if (K > 0) {
                K--;
                ctx.write(node, key);
            }
        }
    }
}
