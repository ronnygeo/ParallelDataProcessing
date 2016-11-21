package assignment5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.PriorityQueue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * Created by ronnygeo on 10/22/16.
 */
//OutputMapper class is used to map the output from the last iteration to just the page rank and the node name.
//These (k, v) are passed to the TopKReducer
public class TopKMapper extends Mapper<Object,Text,DoubleWritable,Text> {
    PriorityQueue<Node> topK;
    public HashMap<Long, String> nodes = new HashMap<>();
    private int K;


    public void setup(Context ctx) throws IOException{
        K   = ctx.getConfiguration().getInt("K", 100);
        Configuration conf = ctx.getConfiguration();
        long ind = 0;
        Path adjmapping = new Path(ctx.getCacheFiles()[0]);
        FileSystem fs = FileSystem.get(adjmapping.toUri(), conf);
        FileStatus[] status = fs.listStatus(adjmapping);
        for (int i=0;i<status.length;i++){
            try{
                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                String node = null;
                while((node = br.readLine()) != null) {
                    nodes.put(ind, node);
                    ind++;
                }
            } catch(IOException ex) {
                System.err.println("Exception while reading adj list file: " + ex.getMessage());
            }
        }

        topK = new PriorityQueue<Node>(K, new Comparator<Node>() {
            public int compare(Node node1, Node node2) {
                return Double.compare(node2.getPageRank(),node1.getPageRank());
            }
        });
    }

    @Override
    public void map(Object key, Text value, Context ctx){
        //Split the line into parts using \t
        String[] line = value.toString().split("\t");
        StringBuilder row = new StringBuilder();
        String name = nodes.get(Long.parseLong(line[1]));
        Double pageRank = Double.parseDouble(line[3]);
        Node n = new Node(name, pageRank);
        topK.offer(n);
    }

    public void cleanup(Context ctx) throws InterruptedException, IOException{
        int count = 0;
//        for (Node node: topK) {
//            if (count < 100) {
//                count++;
//                ctx.write(new DoubleWritable(node.getPageRank()), new Text(node.getName()));
//            }
            while (count < 100) {
                count++;
                Node node = topK.poll();
                if (node != null)
                ctx.write(new DoubleWritable(node.getPageRank()), new Text(node.getName()));
        }
    }
}
