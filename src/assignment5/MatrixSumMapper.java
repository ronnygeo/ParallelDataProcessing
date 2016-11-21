package assignment5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ronnygeo on 11/19/16.
 */
public class MatrixSumMapper extends Mapper<Object,Text,Text,Text> {
    public void map (Object key, Text line, Context ctx) throws InterruptedException, IOException {
        String[] mat = line.toString().split("\t");
        String i = mat[0];
        ctx.write(new Text(i), line);
    }
}
