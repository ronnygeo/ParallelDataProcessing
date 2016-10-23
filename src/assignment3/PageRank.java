package assignment3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.aggregate.DoubleValueSum;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Created by ronnygeo on 10/17/16.
 */
public class PageRank {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setLong("N", 0);
        conf.setDouble("alpha", 0.15);
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Path input, output;
        if (otherArgs.length > 0) {
            input = new Path(otherArgs[0]);
            output = new Path(otherArgs[1]);
        }
        else {
            input = new Path("data.tsv.bz2");
            output = new Path("out");
        }

        readAndIterate(conf, input);

//        System.out.println("N value: " + conf.getLong("N", 0));

        int ii = 0;
        while (ii < 5) {
            iterate(conf, ii++);
        }
        writeOutput(conf, new Path(ii-1+"-iter-output"), output);
    }

    public static void readAndIterate(Configuration conf, Path input) throws Exception {
        conf.setInt("itr", -1);

        Job job = Job.getInstance(conf, "Graph creator job");
//        job.setJarByClass(PageRank.class);
        job.setMapperClass(InputMapper.class);
        job.setReducerClass(ListReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LinkedEdges.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, new Path("adjacency_list"));

        boolean ok = job.waitForCompletion(true);
        if (!ok) {
            throw new Exception("Job failed");
        }
        long NCount = job.getCounters().findCounter(ListReducer.ReduceCounters.N).getValue();
//        System.out.println(NCount);
        conf.setLong("N", NCount);
    }

    public static void writeOutput(Configuration conf, Path input, Path output) throws Exception {
        conf.setInt("K", 100);
        Job job = Job.getInstance(conf, "Write top 100 output");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(OutputMapper.class);
        job.setReducerClass(TopKReducer.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        job.setNumReduceTasks(1);

        boolean ok = job.waitForCompletion(true);
        if (!ok) {
            throw new Exception("Job failed");
        }
    }


    public static void iterate(Configuration conf, int ii) throws Exception {
        conf.setInt("itr", ii);
        Job job = Job.getInstance(conf, "Page rank iteration");
//        job.setNumReduceTasks(5);
//        job.setJarByClass(PageRank.class);
        job.setMapperClass(IterateMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NodeAndPR.class);
//        job.setPartitionerClass(IteratePartitioner.class);
        job.setReducerClass(IterateReducer.class);
        job.setOutputKeyClass(Node.class);
        job.setOutputValueClass(NullWritable.class);

        if (ii == 0)
            FileInputFormat.addInputPath(job, new Path("adjacency_list"));
        else
            FileInputFormat.addInputPath(job, new Path(ii-1+"-iter-output"));
            FileOutputFormat.setOutputPath(job, new Path(ii+"-iter-output"));


        boolean ok = job.waitForCompletion(true);

        if (!ok) {
            throw new Exception("Job failed");
        }

        //    Use a counter to count up how many
        long dangling = job.getCounters().findCounter(IterateReducer.IterCounter.DANGLING_COUNTER).getValue();
        System.out.println("Dangling: " + Double.longBitsToDouble(dangling));
        conf.setLong("dangling", dangling);

        long diff = job.getCounters().findCounter(IterateReducer.IterCounter.CONVERGENCE).getValue();
        System.out.println("Diff: " + Double.longBitsToDouble(diff));
        conf.setDouble("diff", Double.longBitsToDouble(diff));
    }

}
