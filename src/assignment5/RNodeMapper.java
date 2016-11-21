package assignment5;

/**
 * Created by ronnygeo on 11/20/16.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.regex.Pattern;

import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

import java.io.*;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RNodeMapper extends Mapper<Object, Text, Text, NullWritable> {
    public ArrayList<String> nodes;
    private int N;
    private int itr;
    private static Pattern rowPattern;
    static {
        rowPattern = Pattern.compile("(\\d+,\\d+)");
    }

    @Override
    public void setup(Context ctx) throws IOException {
        nodes = new ArrayList<>();
        N   = ctx.getConfiguration().getInt("N", -10);
        itr = ctx.getConfiguration().getInt("itr", -10);
        Configuration conf = ctx.getConfiguration();
//		FileSystem fs = FileSystem.getLocal(conf);

        Path adjmapping = new Path(ctx.getCacheFiles()[0]);
        FileSystem fs = FileSystem.get(adjmapping.toUri(), conf);
        FileStatus[] status = fs.listStatus(adjmapping);
        for (int i=0;i<status.length;i++){
            try{
                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                String node = null;
                while((node = br.readLine()) != null) {
                    nodes.add(node);
                }
            } catch(IOException ex) {
                System.err.println("Exception while reading adj list file: " + ex.getMessage());
            }
        }
    }

    @Override
    public void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {

    }

    @Override
    public void cleanup(Context ctx) {
//			for (String node: nodes)
//				System.out.println(node);
    }
}