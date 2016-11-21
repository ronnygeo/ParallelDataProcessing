package matrixmulttest;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Created by ronnygeo on 10/17/16.
 */
//InputReducer class is used to write the adjacency list from the Input Mapper
//    to the file.
public class ListReducer extends Reducer<Text, Text, Text, NullWritable> {
    private List<Double> listA;
    private List<Double> listB;

    public void setup(Context ctx) {
    }

    public void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
        Double result = 0.0;
        HashMap<Long, List<Double>> tuples = new HashMap<>();

        System.out.println(key.toString());
        for (Text val : values) {
            System.out.println(val.toString());
            String[] mat = val.toString().split(",");
            long index = Long.parseLong(mat[1]);
            List<Double> vals = tuples.get(index);
            if (vals == null)
                vals = new ArrayList<>();
            vals.add(Double.parseDouble(mat[2]));
            tuples.put(index, vals);
        }

        for (Map.Entry e: tuples.entrySet()) {
            List<Double> vals = (ArrayList<Double>) e.getValue();
            if (vals.size() > 1) {
                System.out.println(vals.get(0) +" "+ vals.get(1));
                result += vals.get(0) * vals.get(1);
            }
        }
        if (result != 0.0)
        ctx.write(new Text("R\t"+key.toString().split(",")[0]+"\t0\t"+result), NullWritable.get());
    }

    public void cleanup(Context ctx) {

    }
}
