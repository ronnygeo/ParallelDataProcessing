package InClass.ex4;
//package cs6240;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.ArrayList;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

public class SortPartitioner 
extends Partitioner<Text,NullWritable>
implements Configurable {
    Configuration conf;
    private ArrayList<String> lines;

    public void setup() {
        BufferedReader rdr;
        try {
            String  thisLine = null;
            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream ss = fs.open(new Path("samples/part-r-00000"));
            rdr = new BufferedReader(new InputStreamReader(ss));
            lines = new ArrayList<>();
            while ((thisLine = rdr.readLine()) != null) {
                lines.add(thisLine);
            }
            rdr.close();
        }
        catch (Exception ee) {
            throw new Error(ee.toString());
        }
    }

    public int getPartition(Text key, NullWritable value, int np) {

        if (key.toString().compareTo(lines.get(0)) <= 0) {
            return 0;
        }
         else if (key.toString().compareTo(lines.get(1)) <= 0) {
            return 1;
        }
        else if (key.toString().compareTo(lines.get(2)) <= 0) {
            return 2;
        }
        else if (key.toString().compareTo(lines.get(3)) <= 0) {
            return 3;
        }
        else if (key.toString().compareTo(lines.get(4)) <= 0) {
            return 4;
        }
        else if (key.toString().compareTo(lines.get(5)) <= 0) {
            return 5;
        }
        else if (key.toString().compareTo(lines.get(6)) <= 0) {
            return 6;
        }
        else if (key.toString().compareTo(lines.get(7)) <= 0) {
            return 7;
        }
        else if (key.toString().compareTo(lines.get(8)) <= 0) {
            return 8;
        }
        else {
            return 9;
        }
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
        setup();
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
