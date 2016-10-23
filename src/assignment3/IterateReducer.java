package assignment3;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Logger;

public class IterateReducer extends Reducer<Text, NodeAndPR, Node, NullWritable> {
	private int itr;
	private double alpha;
	private double dangling;
	private int N;
	private double diff;

	public void setup(Context ctx) {
		dangling = Double.longBitsToDouble(ctx.getConfiguration().getLong("dangling", 0));
		diff = ctx.getConfiguration().getDouble("diff", 0);
		itr = ctx.getConfiguration().getInt("itr", -10);
		N = ctx.getConfiguration().getInt("N", 0);
		alpha = ctx.getConfiguration().getDouble("alpha", 0.15);
		if (itr == -10) {
			throw new Error("Didn't propagate itr");
		}
	}

	public static enum IterCounter {DANGLING_COUNTER, CONVERGENCE}
	
	public void reduce(Text key, Iterable<NodeAndPR> vals, Context ctx) throws IOException, InterruptedException {
		double sum = 0;
		Node n = null;
		double pr;
		for (NodeAndPR val: vals) {
//			System.out.println(val);
			//If val is a node save it to n else add the probability to the sum
			if (val.isNode()) {
				n = val.getNode();
			} else {
				sum += val.getPr();
			}
		}
		//If there is no node in the adjacency matrix, create a new one with an empty list
		if (n == null) {
			n = new Node(key.toString());
			ArrayList<String> temp = new ArrayList<>();
			n.setLinks(temp);
		}
		//Calculating the new page rank value
		pr = alpha / N + (1 - alpha) * ((dangling/N) + sum);

		//Incrementing counter for dangling node
		if (n.getLinks().size() <= 0) ctx.getCounter(IterCounter.DANGLING_COUNTER).increment(Double.doubleToLongBits(pr));

		//Convergence measurement of page rank for nodes
		if (itr > 0) {
			diff = n.getPageRank() - pr;
			ctx.getCounter(IterCounter.CONVERGENCE).increment(Double.doubleToLongBits(diff));
		}

		//Setting new page rank to the node
		n.setPageRank(pr);
		ctx.write(n, NullWritable.get());
	}

	public void cleanup() {
	}
}
