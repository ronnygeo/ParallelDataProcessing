//package cs6240;
package InClass.ex3.TwoJobs;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TwoJobs {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path temp = new Path("./temp");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "word count");
        job.setJarByClass(TwoJobs.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, temp);
        job.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = new Job(conf2 , "Sort job");
        FileInputFormat.addInputPath(job2, temp);
        job2.setMapperClass(SortMapper.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setPartitionerClass(SortRangePartitioner.class);
        job2.setReducerClass(SortReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setNumReduceTasks(4);
        FileOutputFormat.setOutputPath(job2,
                new Path(otherArgs[otherArgs.length - 1]));

        //FileSystem fs = FileSystem.get(conf2);
        //fs.delete(temp, true);
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}

class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
    private final static Pattern     nw1 = Pattern.compile("[^'a-zA-Z]");
    private final static Pattern     nw2 = Pattern.compile("(^'+|'+$)");
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());

        while (itr.hasMoreTokens()) {
            Matcher mm1 = nw1.matcher(itr.nextToken());
            Matcher mm2 = nw2.matcher(mm1.replaceAll("")); 
            String ww = mm2.replaceAll("").toLowerCase();

            if (!ww.equals("")) {
                word.set(ww);
                context.write(word, one);
            }
        }
    }
}
 
class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}

class SortMapper extends Mapper<Object, Text, IntWritable, Text>{
    private final static Pattern     nw1 = Pattern.compile("[^'a-zA-Z]");
    private final static Pattern     nw2 = Pattern.compile("(^'+|'+$)");
    private final static IntWritable count = new IntWritable();
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] temp = value.toString().split("\\s");
        word.set(temp[0]);
        count.set(Integer.parseInt(temp[1]));
        context.write(count, word);
        }
}

class SortReducer extends Reducer<IntWritable,Text,Text,IntWritable> {
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text word : values) {
            context.write(word, key);
        }
    }
}

//Sort Partitioner
class SortRangePartitioner extends Partitioner<IntWritable, Text> {
    public SortRangePartitioner() {
    }

    public int getPartition(IntWritable key, Text value, int numReducers){
        if (key.get() < 2) {
            return 0;
        } else if (key.get() < 10) {
            return 1;
        } else if (key.get() < 100) {
            return 2;
        } else {
            return 3;
        }
    }
}
