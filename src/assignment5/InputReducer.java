package assignment5;

import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by ronnygeo on 10/17/16.
 */
//InputReducer class is used to write the adjacency list from the Input Mapper
//    to the file and create a list of unique nodes. This Class also counts the total number of nodes, N.
public class InputReducer extends Reducer<Text, NodeAndOutlinks, Text, NullWritable> {
    //Setting up multiple outputs
    MultipleOutputs<Text, NullWritable> mos;
    //Boolean to detect dangling node
    boolean dangling;

    public void setup(Context ctx) {
        mos = new MultipleOutputs(ctx);
    }


    public void reduce(Text key, Iterable<NodeAndOutlinks> values, Context ctx) throws IOException, InterruptedException {
        //Initially the node is not dangling
        dangling = false;
        String line;

        //List that keeps track of incoming edges with weights
        ArrayList<NodeAndOutlinks> list = new ArrayList<>();

        //Update the hadoop counter for N
        ctx.getCounter(PageRank.HADOOP_COUNTER.N_COUNT).increment(1);

        //Iterate through the links and add to the value
        for (NodeAndOutlinks val : values) {
            //Check if the input value is a node or outlink count
            if (val.isNode()) {
                list.add(val);
            } else {
                // If outlink count is zero set dangling flag
                if (val.getOutlinks() == 0) {
                    dangling = true;
                }
            }
        }

        //Write the node to unique node file, if there are incoming edges
        if (list.size() > 0) mos.write("adjmapping", key, NullWritable.get(), "adj"+"/"+"adj");

        //For all incoming edges
        for (NodeAndOutlinks node: list) {
            //Checking if node is dangling
            if (!dangling) {
                line = "M\t" + key.toString() + "\t" + node.toString();
                if (node.isNode())
                    mos.write("adjlist", new Text(line), NullWritable.get(), "adjlist/adjlist");
            } else {
                    mos.write("dangling", new Text(node.getName()), NullWritable.get(), "dangling/dangling");
            }
            //            else {
//                //if node is dangling set value as dummy value
//                node.setOutlink(-99999.0);
//                line = "M\t" + key.toString() + "\t" + node.toString();
//            }
//            //Write the adj list to the file

        }
    }

    public void cleanup(Context ctx) throws IOException, InterruptedException {
        //Closing multiple outputs
        mos.close();
    }
}
