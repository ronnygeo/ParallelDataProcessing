package assignment5;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ronnygeo on 10/17/16.
 */
//InputReducer class is used to write the adjacency list from the Input Mapper
//    to the file.
public class IterateColumnReducer extends Reducer<Text, Text, Text, NullWritable> {
    HashMap<Long, Double> tempA;
    HashMap<Long, Double> tempB;

    public void setup(Context ctx) {
    }

    public void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
        tempA = new HashMap<>();
        tempB = new HashMap<>();

        for (Text val : values) {
            String[] mat = val.toString().split("\t");
            Long index = Long.parseLong(mat[1]);
            if (mat[0].equals("M")) {
                tempA.put(index, Double.parseDouble(mat[2]));
            } else {
                tempB.put(index, Double.parseDouble(mat[2]));
            }
        }

        for (Map.Entry e: tempA.entrySet()) {
            for (Map.Entry eb: tempB.entrySet()) {
                ctx.write(new Text(e.getKey() + "\t0\t" + ((Double) e.getValue() * (Double) eb.getValue())), NullWritable.get());
            }
        }
    }

    public void cleanup(Context ctx) {

    }
}
