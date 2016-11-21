package assignment5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Created by ronnygeo on 10/17/16.
 */
//MatrixMult class is the main class that runs the page rank job.
public class PageRank {
    private static Long N;
    static boolean rowMajor;
    private static int itrcount;

    public static enum HADOOP_COUNTER {
        N_COUNT
    };
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        rowMajor = true;
        itrcount = 10;
        long start_time, end_time;
        conf.setLong("N", 0);
        conf.setDouble("alpha", 0.15);
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Path input, output;
        if (otherArgs.length > 0) {
            input = new Path(otherArgs[0]);
            output = new Path(otherArgs[1]);
            if (otherArgs.length > 2)
            rowMajor = otherArgs[2].equals("true");
        }
        else {
            input = new Path("wikipedia-simple-html.bz2");
            output = new Path("out");
        }
        start_time = System.currentTimeMillis();
        //Read the data from the input and create the adj list data file
        readAndIterate(conf, input);
        end_time = System.currentTimeMillis();
        System.out.println("Preprocessing Running Time: " + (end_time - start_time) + "ms");
        start_time = System.currentTimeMillis();
        //calculate the count of N from the list.
        //Need to calculate N this way to get accurate count and improve the total value
        mapNodesToIndices(conf, new Path("adjjob/adjlist"));
        end_time = System.currentTimeMillis();

        //Start iterable algorithm
        int ii = 0;
        start_time = System.currentTimeMillis();
        while (ii < itrcount) {
            iterate(conf, ii++);
        }
        end_time = System.currentTimeMillis();
        System.out.println("Iteration Jobs Running Time: " + (end_time - start_time) + "ms");

        start_time = System.currentTimeMillis();
        //Write the output to final output file
        writeOutput(conf, new Path(ii-1+"-iter-output"), output);
        end_time = System.currentTimeMillis();
        System.out.println("Top K Job Running Time: " + (end_time - start_time) + "ms");
    }

    public static void readAndIterate(Configuration conf, Path input) throws Exception {
        conf.setInt("itr", -1);
        conf.setLong("dangling", 0);
        Job job = Job.getInstance(conf, "Graph creator job");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(InputMapper.class);
        job.setReducerClass(InputReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NodeAndOutlinks.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, new Path("adjjob"));
        MultipleOutputs.addNamedOutput(job, "adjmapping", TextOutputFormat.class, Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "dangling", TextOutputFormat.class, Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "adjlist", TextOutputFormat.class, Text.class, NullWritable.class);


        boolean ok = job.waitForCompletion(true);
        long NCount = job.getCounters().findCounter(HADOOP_COUNTER.N_COUNT).getValue();
        System.out.println("Total N count: " + NCount);
        N = NCount;
        conf.setLong("N", NCount);

        if (!ok) {
            throw new Exception("Job failed");
        }
    }

    //A job to count all the N in the graph
    public static void mapNodesToIndices(Configuration conf, Path input) throws Exception {
        Job job = Job.getInstance(conf, "N Counter job");
        job.setJarByClass(PageRank.class);
        Path nodeListPath = new Path("adjjob/adj/");
        job.addCacheFile(nodeListPath.toUri());
        Path danglingNodePath = new Path("adjjob/dangling/");
        job.addCacheFile(danglingNodePath.toUri());
        job.setMapperClass(MapNodesMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, new Path("adjmapped"));

        boolean ok = job.waitForCompletion(true);
        if (!ok) {
            throw new Exception("Job failed");
        }
    }

    public static void writeOutput(Configuration conf, Path input, Path output) throws Exception {
        conf.setInt("K", 100);
        Job job = Job.getInstance(conf, "Write top 100 output");
        job.setJarByClass(PageRank.class);
        Path nodeListPath = new Path("adjjob/adj/");
        job.addCacheFile(nodeListPath.toUri());
        job.setMapperClass(TopKMapper.class);
        job.setReducerClass(TopKReducer.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setSortComparatorClass(SortComparator.class);
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
        conf.setLong("N", N);
        conf.setInt("rowmajor", rowMajor? 1:0);
        Job job = Job.getInstance(conf, "Page rank iteration");
        job.setJarByClass(PageRank.class);
        MultipleInputs.addInputPath(job, new Path("adjmapped"),
                TextInputFormat.class, IterateMMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        if (rowMajor) {
            job.setReducerClass(IterateReducer.class);
            if (ii != 0) {
                Path nodeListPath = new Path(ii - 1 + "-iter-output");
                job.addCacheFile(nodeListPath.toUri());
            }
            FileOutputFormat.setOutputPath(job, new Path(ii+"-iter-output"));
        }else {
            if (ii != 0)
            MultipleInputs.addInputPath(job, new Path(ii - 1 + "-iter-output"),
                    TextInputFormat.class, IterateMMapper.class);
            job.setReducerClass(IterateColumnReducer.class);
            FileOutputFormat.setOutputPath(job, new Path(ii + "-iter-output-temp"));
        }
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        boolean ok = job.waitForCompletion(true);
        if (!ok) {
            throw new Exception("Job failed");
        }

        if (!rowMajor) {
            job = Job.getInstance(conf, "Matrix column summer job");
            job.setJarByClass(PageRank.class);
            job.setMapperClass(MatrixSumMapper.class);
            job.setReducerClass(MatrixSumReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            FileInputFormat.addInputPath(job, new Path(ii+"-iter-output-temp"));
            FileOutputFormat.setOutputPath(job, new Path(ii+"-iter-output"));
            ok = job.waitForCompletion(true);
            if (!ok) {
                throw new Exception("Job failed");
            }
        }
    }

}
