package assignment3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by ronnygeo on 10/22/16.
 */
public class OutputMapper extends Mapper<Object,Text,DoubleWritable,Text> {
    @Override
    public void map(Object key, Text value, Context ctx) throws InterruptedException, IOException{
        String[] line = value.toString().split(":");
        Text name = new Text(line[0]);
        DoubleWritable pageRank = new DoubleWritable(Double.parseDouble(line[1]));
        ctx.write(pageRank, name);
    }
}
