package assignment5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

//IterateReducer class is the iterate reducer that calculates the new page rank value and writes the result to a new file
//This class get the node with the probability from the iterate mapper and computes the new values.
public class IterateReducer extends Reducer<Text, Text, Text, NullWritable> {
	//Store the total count of the nodes
	private Long N;
	//Store the iteration count
	private int itr;
	private double alpha;
	//R vector
	private HashMap<Long,Double> rVector;


	public void setup(Context ctx) throws IOException{
		//Getting values from the configuration
		N   = ctx.getConfiguration().getLong("N", -10);
		itr = ctx.getConfiguration().getInt("itr", -10);
		alpha = ctx.getConfiguration().getDouble("alpha", 0.15);
		if (itr > 0) {
			//Setting up read from distributed cache
			rVector = new HashMap<>();
			Configuration conf = ctx.getConfiguration();
			Path rvector = new Path(ctx.getCacheFiles()[0]);
			FileSystem fs = FileSystem.get(rvector.toUri(), conf);
			FileStatus[] status = fs.listStatus(rvector);
			for (int i = 0; i < status.length; i++) {
				try {
					BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
					String node = null;
					while ((node = br.readLine()) != null) {
						String[] line = node.split("\t");
						rVector.put(Long.parseLong(line[1]), Double.parseDouble(line[3]));
					}
				} catch (IOException ex) {
					System.err.println("Exception while reading adj list file: " + ex.getMessage());
				}
			}
		}
	}

	public void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
		Double result = 0.0;
		HashMap<Long, List<Double>> tuples = new HashMap<>();

		for (Text val : values) {
			String[] mat = val.toString().split("\t");
			long index = Long.parseLong(mat[1]);

			if (itr != 0) {
				List<Double> vals = tuples.get(index);
				if (vals == null)
					vals = new ArrayList<>();
				vals.add(Double.parseDouble(mat[2]));
				Double rval = rVector.get(index);
				if (rval != null)
					vals.add(rval);
				tuples.put(index, vals);
			} else {
				List<Double> vals = tuples.get(index);
				if (vals == null)
					vals = new ArrayList<>();
				vals.add(Double.parseDouble(mat[2]));
				tuples.put(index, vals);
			}
		}


		for (Map.Entry e : tuples.entrySet()) {
			List<Double> vals = (ArrayList<Double>) e.getValue();
			if (vals.size() > 1) {
				result += vals.get(0) * vals.get(1);
			}
		}
		if (result != 0.0) {
			Double newVal = alpha/N + (1 - alpha) * result;
			ctx.write(new Text("R\t" + key.toString() + "\t0\t" + newVal), NullWritable.get());
		}
	}
}
