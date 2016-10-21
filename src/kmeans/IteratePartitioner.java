package kmeans;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

public class IteratePartitioner extends Partitioner<DataPoint, DataPoint> implements Configurable {
	ArrayList<DataPoint> centers = null;
	
	@Override
	public void setConf(Configuration conf) {
		int K   = conf.getInt("K", -10);
		int itr = conf.getInt("itr", -10);
		
		if (itr == -1) {
			return;
		}
		
		try {
			centers = DataPoint.readCenters(conf, K, itr);
		} catch (IOException e) {
			e.printStackTrace();
			throw new Error("Failed to read centers");
		}
	}

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public int getPartition(DataPoint key, DataPoint value, int numPartitions) {
		if (value.center) {
			return 0;
		}
		
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
