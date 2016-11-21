package assignment5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// The input mapper class goes through the input file and parses it into the adjacency matrix
public class InputMapper extends Mapper<Object, Text, Text, NodeAndOutlinks> {
	private HashMap<String, List> adj;
	// Keep only html pages not containing tilde (~).
	private static Pattern namePattern = Pattern.compile("^([^~]+)$");

	Bz2Parser bz2Parser;


	public void setup(Context ctx) throws IOException, InterruptedException {
		//Initializing a Map to store the adjacency list
		adj = new HashMap<>();
	}

	public void cleanup(Context ctx) throws IOException, InterruptedException {
		//Write each node and its adjacent edge as value
		for (Map.Entry e : adj.entrySet()) {
			ArrayList<String> edgeNodes = (ArrayList<String>) e.getValue();
			//Emit the node with the outlink count for dangling node detection
			ctx.write(new Text((String) e.getKey()), new NodeAndOutlinks(edgeNodes.size()));
			//If the outlink count is not zero, get weight and emit
			if (edgeNodes.size() != 0) {
				Double outlinkCount = 1.0 / edgeNodes.size();
				//For each outlink, emit the weight from current node
				for (String eNode : edgeNodes) {
					ctx.write(new Text(eNode), new NodeAndOutlinks((String) e.getKey(), outlinkCount));
				}
			}
		}
	}

    public void map(Object _k, Text line, Context ctx) throws InterruptedException, IOException {
		try {
			//Get the links and node from the current line
			int delimLoc = line.toString().indexOf(':');
			String pageName = line.toString().substring(0, delimLoc);
			String html = line.toString().substring(delimLoc + 1);
			Matcher matcher = namePattern.matcher(pageName);
			// Skip this html file, name contains (~).
			if (matcher.find()) {
				List<String> temp = bz2Parser.getAdjList(html);
				//Checking self link
				if (temp.contains(pageName)) {
					temp.remove(pageName);
				}

				//If the page has links add to the adjacency list
				adj.put(pageName, temp);
			}
		} catch (Exception e) {
		e.printStackTrace();
	}
    }
}
 
