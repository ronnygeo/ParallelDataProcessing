package kmeans;


import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InputMapper extends Mapper<Object, Text, DataPoint, DataPoint> {
	ArrayList<DataPoint> centers = new ArrayList<DataPoint>();
	int K;
	int jj = 0;
	 
	public void setup(Context ctx) {
    	K = ctx.getConfiguration().getInt("K", -10);
    	if (K == -10) {
    		throw new Error("Didn't propagate K");
    	}
	}
	
	public void cleanup(Context ctx) throws IOException, InterruptedException {
		for (DataPoint cc : centers) {
			ctx.write(cc, cc);
		}
	}

    public void map(Object _k, Text line, Context ctx) throws InterruptedException, IOException {
        String[] cols = line.toString().split("\t");

        DataPoint pt = new DataPoint();
        pt.x = Double.parseDouble(cols[0]);
        pt.y = Double.parseDouble(cols[1]);
        pt.label = cols[2];
        
        if (centers.size() < K) {
        	DataPoint ctr = pt.clone();
        	ctr.center = true;
        	
        	centers.add(ctr);
        	ctx.write(ctr, pt);
        }
        else {
        	ctx.write(centers.get(jj++ % K), pt);
        }
    }
}
 
