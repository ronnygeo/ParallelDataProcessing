package assignment3;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.aggregate.DoubleValueSum;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class IterateReducer extends Reducer<Text, NodeAndPR, Node, NullWritable> {
	private int itr;
	private double alpha;
	private double dangling;
	private double dangling_temp = 0.0;
	private int N;
	private double total_diff = 0.0;
	private double pr_sum = 0.0;

	public void setup(Context ctx) {
//		dangling = Double.longBitsToDouble(ctx.getConfiguration().getLong("dangling", 0));
		dangling = (double) ctx.getConfiguration().getLong("dangling", 0) / Math.pow(10, 10);

		System.out.println("Dangling: " + dangling);
		itr = ctx.getConfiguration().getInt("itr", -10);
		N = ctx.getConfiguration().getInt("N", 0);
		alpha = ctx.getConfiguration().getDouble("alpha", 0.15);
		if (itr == -10) {
			throw new Error("Didn't propagate itr");
		}
	}

	public static enum IterCounter {DANGLING_COUNTER, CONVERGENCE, TOTAL_SUM}
	
	public void reduce(Text key, Iterable<NodeAndPR> vals, Context ctx) throws IOException, InterruptedException {
		double sum = 0;
//		System.out.println(key);
		Node n = new Node(key.toString());
		ArrayList<String> links = new ArrayList<>();
		double pr, previous_pr = 0;
		for (NodeAndPR val: vals) {
			//If val is a node save it to n else add the probability to the sum
			if (val.isNode()) {
//				n = val.getNode();
				links = val.getNode().getLinks();
				previous_pr = val.getNode().getPageRank();
//				System.out.println(n);
			} else {
				sum += val.getPr();
			}
		}
//		System.out.println(n);
//		if (n == null) {
//		}

		//If there is no node in the adjacency matrix, create a new one with an empty list
		n.setLinks(links);

		//Calculating the new page rank value
		pr = alpha / N + (1 - alpha) * ((dangling/N) + sum);
//		if (itr == 2) {
//			System.out.println(pr);
//		}

		//Incrementing counter for dangling node
		if (n.getLinks().size() <= 0) {
			dangling_temp += pr;
		}
		//Convergence measurement of page rank for nodes
		if (itr > 0) {
			total_diff += (pr - previous_pr);
		}

		//Setting new page rank to the node
		n.setPageRank(pr);
		pr_sum += pr;

//		System.out.println(n);
		ctx.write(n, NullWritable.get());
	}

	public void cleanup(Context ctx) {
		long converted_dn = (long) (dangling_temp * Math.pow(10, 10));
		ctx.getCounter(IterCounter.DANGLING_COUNTER).increment(converted_dn);

		long converted_sum = (long) (pr_sum * Math.pow(10, 10));
		ctx.getCounter(IterCounter.TOTAL_SUM).increment(converted_sum);

		long converted_diff = (long) (total_diff * Math.pow(10, 10));
		ctx.getCounter(IterCounter.CONVERGENCE).increment(converted_diff);

//		double current_diff = Double.longBitsToDouble(ctx.getCounter(IterCounter.CONVERGENCE).getValue());
//		ctx.getCounter(IterCounter.CONVERGENCE).setValue(Double.doubleToLongBits(current_diff + total_diff));
//
//		double converted_dn = Double.longBitsToDouble(ctx.getCounter(IterCounter.DANGLING_COUNTER).getValue());
//		ctx.getCounter(IterCounter.DANGLING_COUNTER).setValue(Double.doubleToLongBits(converted_dn + dangling_temp));
//
//		double converted_sum = Double.longBitsToDouble(ctx.getCounter(IterCounter.TOTAL_SUM).getValue());
//		ctx.getCounter(IterCounter.TOTAL_SUM).setValue(Double.doubleToLongBits(converted_sum + pr_sum));
	}
}
