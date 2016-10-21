package assignment3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by ronnygeo on 10/17/16.
 */
public class ListReducer extends Reducer<Text, Text, Node, NullWritable> {

    private long nodesCount;

    static enum ReduceCounters { N }

    public void setup(Context ctx) {
        nodesCount = 0;
    }


    public void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
        ArrayList<String> list = new ArrayList<>();
        Node node = new Node(key.toString());
        node.setPageRank(0);
        ctx.getCounter(ReduceCounters.N).increment(1);
        nodesCount++;
        for (Text val : values) {
            list.add(val.toString());
        }
        LinkedEdges links = new LinkedEdges(list.toArray(new String[0]));
        node.setLinks(links);
        ctx.write(node, NullWritable.get());
    }

    public void cleanup(Context ctx) {
        Configuration conf = ctx.getConfiguration();
        conf.setLong("N", nodesCount + conf.getLong("N", 0));
        System.out.println(conf.getLong("N", 0));
//        conf.setLong("N", 1000);
    }
}