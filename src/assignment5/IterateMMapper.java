package assignment5;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

//The Iterate M mapper class is used with Multiple Inputs to map the M matrix to the
// iterator reducer.
public class IterateMMapper extends Mapper<Object, Text, Text, Text> {
	//Store the total count of the nodes
	private Long N;
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
		double val = Double.parseDouble(line[3]);
		if (rowmajor ==1) {
			ctx.write(new Text(i.toString()), new Text(line[0] + "\t" + j + "\t" + val));
			//Generating the R vector
			if (itr == 0) {
				ctx.write(new Text(i.toString()), new Text("R" + "\t0\t" + 1.0/N));
			}
		} else {
			ctx.write(new Text(j.toString()), new Text(line[0] + "\t" + i + "\t" + val));
			if (itr == 0) {
				ctx.write(new Text(j.toString()), new Text("R" + "\t0\t" + 1.0/N));
			}
		}
	}
}
