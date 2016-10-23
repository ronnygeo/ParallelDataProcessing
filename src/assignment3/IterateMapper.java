package assignment3;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

//The Iterate mapper class is used to map the adjacency list to the linked node and a node and probability
// The Key is the adjacent link and the value either the entire node or the probability, Pr(current node)/Adjacent nodes of current node
public class IterateMapper extends Mapper<Object, Text, Text, NodeAndPR> {
	//Store the total count of the nodes
	private int N;
	//Store the iteration count
	private int itr;

	@Override
	public void setup(Context ctx) throws IOException {
		//Getting values from the configuration
		N   = ctx.getConfiguration().getInt("N", -10);
		itr = ctx.getConfiguration().getInt("itr", -10);
	}
	
	@Override
	public void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
		//Splitting the current line into parts on :
		String[] line = value.toString().split(":");
		//List to store the
		ArrayList<String> list = new ArrayList<>();
		//Create a new node with the key name
		Node node = new Node(line[0]);
		//If the iteration is 0, then set the page rank as 1/N else get from file
		if (itr == 0)
			node.setPageRank(1.0000/N);
		else
			node.setPageRank(Double.parseDouble(line[1]));
		//If the line has a length of more than 2, then there are links
		if (line.length > 2) {
			for (String link : line[2].split(",")) {
				list.add(link);
			}
		}
		//Add the links to the node
		node.setLinks(list);
		ctx.write(new Text(node.getName()), new NodeAndPR(node));
		//Calculate the probility, current pr/total adj links
		double p = node.getPageRank() / list.size();
		//For all the links in the adjacency list, emit the links with the current node
		for (String link: list) {
			ctx.write(new Text(link), new NodeAndPR(p));
		}
	}
}