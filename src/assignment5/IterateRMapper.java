package assignment5;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

//The Iterate mapper class is used to map the adjacency list to the linked node and a node and probability
// The Key is the adjacent link and the value either the entire node or the probability, Pr(current node)/Adjacent nodes of current node
public class IterateRMapper extends Mapper<Object, Text, Text, Text> {
	//Store the total count of the nodes
	private long N;
	//Store the iteration count
	private int itr;
	private int rowmajor;

	@Override
	public void setup(Context ctx) throws IOException {
		//Getting values from the configuration
		N   = ctx.getConfiguration().getLong("N", -10);
		itr = ctx.getConfiguration().getInt("itr", -10);
		rowmajor = ctx.getConfiguration().getInt("rowmajor", 1);
	}

	@Override
	public void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
		//Splitting the current line into parts on :
		String[] line = value.toString().split("\t");
		Long i = Long.parseLong(line[1]);
		Long j = Long.parseLong(line[2]);
		Double val = Double.parseDouble(line[3]);
		if (rowmajor == 1) {
			for (Integer k = 0; k < N; k++)
				ctx.write(new Text(k.toString()), new Text(line[0] + "\t" + i + "\t" + val));
		} else {
			//If column major
			ctx.write(new Text(i.toString()), new Text(line[0] + "\t" + j + "\t" + val));
		}
	}
}
