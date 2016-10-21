package kmeans;


import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.mapreduce.Mapper;

public class IterateMapper extends Mapper<DataPoint, DataPoint, DataPoint, DataPoint> {
	public ArrayList<DataPoint> centers;
	int K;
	int itr;
	
	@Override
	public void setup(Context ctx) throws IOException {
		K   = ctx.getConfiguration().getInt("K", -10);
		itr = ctx.getConfiguration().getInt("itr", -10);
		centers = DataPoint.readCenters(ctx.getConfiguration(), K, itr);
	}
	
	@Override
	public void map(DataPoint cc0, DataPoint dp, Context ctx) throws IOException, InterruptedException {
        // FIXME
        // emit(nearest cluster center, data point)

	}
	public static void getCenter() {
		float total_x = 0;
		float count_x = 0;
		float total_y = 0;
		float count_y = 0;
	}
}


