package assignment3;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

public class IterateMapper extends Mapper<Object, Text, Text, LinkedEdges> {
//	public ArrayList<DataPoint> centers;
	int N;
	int itr;
	
	@Override
	public void setup(Context ctx) throws IOException {
		N   = ctx.getConfiguration().getInt("N", -10);
		itr = ctx.getConfiguration().getInt("itr", -10);
	}
	
	@Override
	public void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
		String[] line = value.toString().split(":");
		ArrayList<String> list = new ArrayList<>();
		Node node = new Node(line[0]);
		if (itr == 0)
		node.setPageRank(1/N);
		else
			node.setPageRank(Double.parseDouble(line[1]));
 		for (String link: line[3].split(",")) {
			list.add(link);
		}
		LinkedEdges links = new LinkedEdges(list.toArray(new String[0]));
		node.setLinks(links);
	}
}