package assignment3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.PriorityQueue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * Created by ronnygeo on 10/22/16.
 */
//TopKMapper class is used to map the output from the last iteration to just the page rank and the node name.
//These (k, v) are passed to the TopKReducer
public class OutputMapper extends Mapper<Object,Text,DoubleWritable,Text> {
    PriorityQueue<Node> topK;

    public void setup(Context ctx) {
        topK = new PriorityQueue<Node>(100, new Comparator<Node>() {
            public int compare(Node node1, Node node2) {
                return Double.compare(node2.getPageRank(),node1.getPageRank());
            }
        });
    }

    @Override
    public void map(Object key, Text value, Context ctx){
        //Split the line into parts using :
        String[] line = value.toString().split(":");
        String name = line[0];
        Double pageRank = Double.parseDouble(line[1]);
        Node n = new Node(name, pageRank);
        topK.offer(n);
//        ctx.write(pageRank, name);
    }

    public void cleanup(Context ctx) throws InterruptedException, IOException{
        int count = 0;
        for (Node node: topK) {
            count++;
            if (count < 100)
            ctx.write(new DoubleWritable(node.getPageRank()), new Text(node.getName()));
        }
    }
}
