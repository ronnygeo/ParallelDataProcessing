package assignment5;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ronnygeo on 10/22/16.
 */
//TopKReducer class is used to get the top K results from the input.
public class TopKReducer extends Reducer<DoubleWritable,Text,Text,DoubleWritable> {
    int K;
    public void setup(Context ctx) {
        K = ctx.getConfiguration().getInt("K", 100);
    }

    public void reduce(DoubleWritable key, Iterable<Text> nodes, Context ctx) throws InterruptedException, IOException{
        //Iterate through the nodes and decrement count
        for (Text node: nodes) {
            if (K > 0) {
                K--;
                ctx.write(node, key);
            }
        }
    }
}
