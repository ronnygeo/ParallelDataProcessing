package matrixmult;


import org.apache.hadoop.conf.Configuration;
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
        listA = new ArrayList<>(Collections.nCopies(5, 0.0));
        listB = new ArrayList<>(Collections.nCopies(5, 0.0));

        for (Text val : values) {
//            System.out.println(val.toString());
            String[] mat = val.toString().split(",");
            if (mat[0].equals("A")) {
                listA.set(Integer.parseInt(mat[1]), Double.parseDouble(mat[2]));
            } else if (mat[0].equals("B")) {
                listB.set(Integer.parseInt(mat[1]), Double.parseDouble(mat[2]));
            }
        }
        for (int i =0; i<5; i++) {
            result += listA.get(i) * listB.get(i);
        }
        if (result != 0.0)
        ctx.write(new Text(key.toString()+","+result), NullWritable.get());
    }

    public void cleanup(Context ctx) {

    }
}
