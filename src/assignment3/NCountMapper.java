package assignment3;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

public class NCountMapper extends Mapper<Object, Text, Text, Text> {
//	public ArrayList<DataPoint> centers;
	private int N;
	private int itr;

	@Override
	public void setup(Context ctx) throws IOException {
		N   = ctx.getConfiguration().getInt("N", -10);
		itr = ctx.getConfiguration().getInt("itr", -10);
	}
	
	@Override
	public void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
		String[] line = value.toString().split(":");
		ArrayList<String> list = new ArrayList<>();
		if (line.length > 2) {
			for (String link : line[2].split(",")) {
				list.add(link);
			}
		}
		for (String link: list) {
			ctx.write(new Text(link), new Text(line[0]));
		}
	}
}