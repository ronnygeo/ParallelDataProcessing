package InClass.ex4;
//package cs6240;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class QuantReducer
extends Reducer<NullWritable,Text,Text,NullWritable> {
    NullWritable nw = NullWritable.get();

    public void reduce(NullWritable _k, Iterable<Text> lines, 
            Context context) throws IOException, InterruptedException {
        ArrayList<String> xs = new ArrayList<String>();

        int qs = Integer.parseInt(context.getConfiguration().get("num-quants"));

        for (Text line: lines) {
            xs.add(line.toString());
        }

        Collections.sort(xs);

        int count = xs.size();

        for (int i=1; i<=qs; i++) {
            context.write(new Text(xs.get(i*count/(qs+1))), NullWritable.get());
        }
//        context.write(new Text(xs.get(count-1)), NullWritable.get());
    }
}


