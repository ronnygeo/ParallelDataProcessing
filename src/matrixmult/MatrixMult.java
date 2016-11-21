package matrixmult;


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
import org.apache.hadoop.yarn.util.SystemClock;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;

/**
 * Created by ronnygeo on 10/17/16.
 */
//MatrixMult class is the main class that runs the page rank job.
public class MatrixMult {
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
            input = new Path("inputFiles/matA");
            output = new Path("out");
        }
//        start_time = System.currentTimeMillis();
//        //Read the data from the input and create the adj list data file
        readAndIterate(conf, input);
//        end_time = System.currentTimeMillis();
//        System.out.println("Preprocessing Running Time: " + (end_time - start_time) + "ms");
//
//        start_time = System.currentTimeMillis();
//        //calculate the count of N from the list.
//        //Need to calculate N this way to get accurate count and improve the total value
//        getNCount(conf, new Path("adjacency_list"));
//        end_time = System.currentTimeMillis();
//        System.out.println("NCount Job Running Time: " + (end_time - start_time) + "ms");
//
//        //Start iterable algorithm
//        int ii = 0;
//        start_time = System.currentTimeMillis();
//        while (ii < 10) {
//            iterate(conf, ii++);
//        }
//        end_time = System.currentTimeMillis();
//        System.out.println("Iteration Jobs Running Time: " + (end_time - start_time) + "ms");
//
//        start_time = System.currentTimeMillis();
//        //Write the output to final output file
//        writeOutput(conf, new Path(ii-1+"-iter-output"), output);
//        end_time = System.currentTimeMillis();
//        System.out.println("Top K Job Running Time: " + (end_time - start_time) + "ms");
    }
//
    public static void readAndIterate(Configuration conf, Path input) throws Exception {
        conf.setInt("itr", -1);
        conf.setLong("dangling", 0);
        Job job = Job.getInstance(conf, "Graph creator job");
        job.setJarByClass(MatrixMult.class);
        job.setMapperClass(InputMapper.class);
        job.setReducerClass(ListReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, new Path("outMat"));

        boolean ok = job.waitForCompletion(true);
        if (!ok) {
            throw new Exception("Job failed");
        }
    }
//
//    //A job to count all the N in the graph
//    public static void getNCount(Configuration conf, Path input) throws Exception {
//        Job job = Job.getInstance(conf, "N Counter job");
//        job.setJarByClass(MatrixMult.class);
//        job.setMapperClass(MapNodesMapper.class);
//        job.setReducerClass(MapNodesReducer.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(LinkedEdges.class);
//        FileInputFormat.addInputPath(job, input);
//        FileOutputFormat.setOutputPath(job, new Path("temp"));
//        job.setNumReduceTasks(1);
//
//        boolean ok = job.waitForCompletion(true);
//        if (!ok) {
//            throw new Exception("Job failed");
//        }
//
//        long NCount = job.getCounters().findCounter(MapNodesReducer.ReduceCounters.N).getValue();
//        System.out.println(NCount);
//        conf.setLong("N", NCount);
//    }
//
//    public static void writeOutput(Configuration conf, Path input, Path output) throws Exception {
//        conf.setInt("K", 100);
//        Job job = Job.getInstance(conf, "Write top 100 output");
//        job.setJarByClass(MatrixMult.class);
//        job.setMapperClass(TopKMapper.class);
//        job.setReducerClass(TopKReducer.class);
//        job.setMapOutputKeyClass(DoubleWritable.class);
//        job.setMapOutputValueClass(Text.class);
//        job.setSortComparatorClass(SortComparator.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(DoubleWritable.class);
//        FileInputFormat.addInputPath(job, input);
//        FileOutputFormat.setOutputPath(job, output);
//        job.setNumReduceTasks(1);
//
//        boolean ok = job.waitForCompletion(true);
//        if (!ok) {
//            throw new Exception("Job failed");
//        }
//    }
//
//
//    public static void iterate(Configuration conf, int ii) throws Exception {
//        conf.setInt("itr", ii);
//        Job job = Job.getInstance(conf, "Page rank iteration");
//        job.setJarByClass(MatrixMult.class);
//        job.setMapperClass(IterateMMapper.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(NodeAndPR.class);
//        job.setReducerClass(IterateReducer.class);
//        job.setOutputKeyClass(Node.class);
//        job.setOutputValueClass(NullWritable.class);
//
//        if (ii == 0)
//            FileInputFormat.addInputPath(job, new Path("adjacency_list"));
//        else
//            FileInputFormat.addInputPath(job, new Path(ii-1+"-iter-output"));
//            FileOutputFormat.setOutputPath(job, new Path(ii+"-iter-output"));
//
//
//        boolean ok = job.waitForCompletion(true);
//
//        if (!ok) {
//            throw new Exception("Job failed");
//        }
//
//        //    Use a counter to count up how many
//        long dangling = job.getCounters().findCounter(IterateReducer.IterCounter.DANGLING_COUNTER).getValue();
////        System.out.println("Dangling: " + Double.longBitsToDouble(dangling));
//        System.out.println("Dangling: " + (double) dangling/Math.pow(10, 10));
//        conf.setLong("dangling", dangling);
//
////        System.out.println(Double.longBitsToDouble(job.getCounters().findCounter(IterateReducer.IterCounter.TOTAL_SUM).getValue()));
//        System.out.println("Sum: " + (double) job.getCounters().findCounter(IterateReducer.IterCounter.TOTAL_SUM).getValue() / Math.pow(10, 10));
//
//
//        long diff = job.getCounters().findCounter(IterateReducer.IterCounter.CONVERGENCE).getValue();
//        System.out.println("Diff: " + (double) diff / Math.pow(10, 10));
//    }

}
