package kmeans;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class IterateReducer extends Reducer<DataPoint, DataPoint, DataPoint, DataPoint> {
	int itr;
	
	public void setup(Context ctx) {
		itr = ctx.getConfiguration().getInt("itr", -10);
		if (itr == -10) {
			throw new Error("Didn't propagate itr");
		}
	}
	
	public void reduce(DataPoint cc, Iterable<DataPoint> vals, Context ctx) throws IOException, InterruptedException {
        // FIXME
        //  foreach val:
        //     emit(val)
        //  
        //  emit(new center)
		for (DataPoint p: vals) {
			ctx.write(cc, p);
		}
	}
}
