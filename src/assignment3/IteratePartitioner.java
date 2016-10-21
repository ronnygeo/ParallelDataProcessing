package assignment3;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.IOException;
import java.util.ArrayList;

public class IteratePartitioner extends Partitioner<Node,Node> implements Configurable {
	ArrayList<Node> centers = null;
	
	@Override
	public void setConf(Configuration conf) {
		int N   = conf.getInt("N", -10);
		int itr = conf.getInt("itr", -10);
		
		if (itr == -1) {
			return;
		}

	}

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public int getPartition(Node key, Node value, int numPartitions) {

		if (centers == null) { // iteration -1
			return 1 + (Math.abs(value.toString().hashCode()) % (numPartitions - 1));
		}
		
		for (int ii = 0; ii < centers.size(); ++ii) {
			if (key.equals(centers.get(ii))) {
				return ii + 1;
			}
		}
		
		return -27;
	}
}
