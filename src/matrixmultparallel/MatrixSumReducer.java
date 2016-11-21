package matrixmultparallel;

import org.apache.hadoop.io.DoubleWritable;
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

    public void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
        HashMap<Long,Double> sum = new HashMap<>();
        for (Text l: values) {
            System.out.println(key.toString() + " " + l);
            String[] line = l.toString().split(",");
            Long j = Long.parseLong(line[1]);
            if (sum.containsKey(j)) {
                sum.put(j, Double.parseDouble(line[2]) + sum.get(j));
            } else {
                sum.put(j, Double.parseDouble(line[2]));
            }
        }

        for (Map.Entry e: sum.entrySet()) {
            String row = key.toString() + "," + e.getKey() + "," + e.getValue();
            ctx.write(new Text(row), NullWritable.get());
        }
    }
}
