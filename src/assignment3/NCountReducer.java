package assignment3;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by ronnygeo on 10/17/16.
 */
//MapNodesReducer class is used to count the number of Nodes in the graph.
    //It add all the nodes to the Set which stores the nodes and stores the length of the set
public class NCountReducer extends Reducer<Text, Text, Node, NullWritable> {
    //Set used to store the nodes
    Set<String> V;
    static enum ReduceCounters { N }

    public void setup(Context ctx) {
        V  = new HashSet<String>();
    }


    public void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
        V.add(key.toString());
        for (Text val : values) {
            V.add(val.toString());
        }
    }

    public void cleanup(Context ctx) {
        System.out.println(V.size());
        ctx.getCounter(ReduceCounters.N).increment(V.size());
    }
}
