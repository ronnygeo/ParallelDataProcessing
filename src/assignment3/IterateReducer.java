package assignment3;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

//IterateReducer class is the iterate reducer that calculates the new page rank value and writes the result to a new file
//This class get the node with the probability from the iterate mapper and computes the new values.
public class IterateReducer extends Reducer<Text, NodeAndPR, Node, NullWritable> {
	private int itr;
	private double alpha;
	//Stores the old dangling value
	private double dangling;
	//Stores the value for the current iteration
	private double dangling_temp = 0.0;
	private int N;
	private double total_diff = 0.0;
	private double pr_sum = 0.0;

	public void setup(Context ctx) {
		//Getting the dangling value from the context
		// dangling = Double.longBitsToDouble(ctx.getConfiguration().getLong("dangling", 0));
		dangling = (double) ctx.getConfiguration().getLong("dangling", 0) / Math.pow(10, 10);
		itr = ctx.getConfiguration().getInt("itr", -10);
		N = ctx.getConfiguration().getInt("N", 0);
		//Getting the alpha from context
		alpha = ctx.getConfiguration().getDouble("alpha", 0.15);
		if (itr == -10) {
			throw new Error("Didn't propagate itr");
		}
	}

	//Stores the Counters for the current reducer. There are 3 counters in this reducer.
	//DANGLING_COUNTER: Stores the dangling values for the current iteration
	//TOTAL_SUM: Stores the total sum for the current reducer.
	//CONVERGENCE: Stores the difference in the page rank values for a node
	public static enum IterCounter {DANGLING_COUNTER, CONVERGENCE, TOTAL_SUM}
	
	public void reduce(Text key, Iterable<NodeAndPR> vals, Context ctx) throws IOException, InterruptedException {
		double sum = 0;
		//initialize a node
		Node n = new Node(key.toString());
		ArrayList<String> links = new ArrayList<>();
		double pr, previous_pr = 0;
		for (NodeAndPR val: vals) {
			//If val is a node save it to n else add the probability to the sum
			if (val.isNode()) {
				//Get the links from the node in the value
				links = val.getNode().getLinks();
				//Get the page rank from the node in the value
				previous_pr = val.getNode().getPageRank();
			} else {
				//Add the value to the sum if its the probability
				sum += val.getPr();
			}
		}

		//If there is no node in the adjacency matrix, create a new one with an empty list
		n.setLinks(links);

		//Calculating the new page rank value
		pr = alpha / N + (1 - alpha) * ((dangling/N) + sum);

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

		ctx.write(n, NullWritable.get());
	}

	public void cleanup(Context ctx) {
		//Write the total dangling value to the counter
		long converted_dn = (long) (dangling_temp * Math.pow(10, 10));
		ctx.getCounter(IterCounter.DANGLING_COUNTER).increment(converted_dn);

		//Write the total sum to the counter
		long converted_sum = (long) (pr_sum * Math.pow(10, 10));
		ctx.getCounter(IterCounter.TOTAL_SUM).increment(converted_sum);

		//Write the total difference in node values to the counter
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
