package assignment3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// The input mapper class goes through the input file and parses it into the adjacency matrix
public class InputMapper extends Mapper<Object, Text, Text, Text> {
	private HashMap<String, List> adj;
	private Text node = new Text();
	private Text edgeNode = new Text();
	Bz2Parser bz2Parser;


	public void setup(Context ctx) throws IOException, InterruptedException {
		//Initializing a Map to store the adjacency list
		adj = new HashMap<>();
	}

	public void cleanup(Context ctx) throws IOException, InterruptedException {
		//Write each node and its adjacent edge as value
		for (Map.Entry e : adj.entrySet()) {
			ArrayList<String> edgeNodes = (ArrayList<String>) e.getValue();
				node.set((String) e.getKey());
				for (String eNode : edgeNodes) {
//					System.out.println(node.toString() + " " + eNode);
					edgeNode.set((String) eNode);
					ctx.write(node, edgeNode);
				}
		}
	}

    public void map(Object _k, Text line, Context ctx) throws InterruptedException, IOException {
		try {
			//Get the links and node from the current line
			HashMap<String, List> temp = bz2Parser.getAdjList(line.toString());
			//If the page has links add to the adjacency list
			if (temp != null)
				adj.putAll(temp);
		} catch (Exception e) {
		e.printStackTrace();
	}
    }
}
 
