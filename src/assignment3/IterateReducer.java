package assignment3;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IterateReducer extends Reducer<Text, Text, Text, Text> {
	int itr;
	
	public void setup(Context ctx) {
		itr = ctx.getConfiguration().getInt("itr", -10);
		if (itr == -10) {
			throw new Error("Didn't propagate itr");
		}
	}
	
	public void reduce(Text cc, Iterable<Text> vals, Context ctx) throws IOException, InterruptedException {
        // FIXME
        //  foreach val:
        //     emit(val)
        //  
        //  emit(new center)
		for (Text p: vals) {
			ctx.write(cc, p);
		}
	}
}
