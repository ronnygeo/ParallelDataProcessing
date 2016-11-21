package assignment5;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ronnygeo on 11/19/16.
 */

public class MatrixSumReducer extends Reducer<Text, Text, Text, NullWritable> {
    private long N;
    private int itr;
    private double alpha;

    public void setup(Context ctx) {
        //Getting values from the configuration
        N   = ctx.getConfiguration().getLong("N", -10);
        itr = ctx.getConfiguration().getInt("itr", -10);
        alpha = ctx.getConfiguration().getDouble("alpha", 0.15);
    }


    public void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
        HashMap<Long,Double> sum = new HashMap<>();
        for (Text l: values) {
            String[] line = l.toString().split("\t");
            Long j = Long.parseLong(line[1]);
            if (sum.containsKey(j)) {
                sum.put(j, Double.parseDouble(line[2]) + sum.get(j));
            } else {
                sum.put(j, Double.parseDouble(line[2]));
            }
        }

        for (Map.Entry e: sum.entrySet()) {
            Double newVal = alpha/N + (1 - alpha) * (Double) e.getValue();
            String row = "R\t" + key.toString() + "\t" + e.getKey() + "\t" + newVal;
            ctx.write(new Text(row), NullWritable.get());
        }
    }
}
