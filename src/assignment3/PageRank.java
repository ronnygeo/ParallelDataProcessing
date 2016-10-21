package assignment3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
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
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Path input;
        if (otherArgs.length > 0) {
            input = new Path(otherArgs[0]);
        }
        else {
            input = new Path("data.tsv.bz2");
        }

        readAndIterate(conf, input);

//        System.out.println("N value: " + conf.getLong("N", 0));

        int ii = 0;
        boolean done = false;
        while (!done) {
            done = iterate(conf, ii++);
        }

//        writeOutput(conf, ii);
    }

    public static void readAndIterate(Configuration conf, Path input) throws Exception {
        conf.setInt("itr", -1);

        Job job = Job.getInstance(conf, "read input");
        job.setJarByClass(PageRank.class);
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

    public static boolean iterate(Configuration conf, int ii) throws Exception {
        Job job = Job.getInstance(conf, "k-Means iteration");
//        job.setNumReduceTasks(5);
        job.setMapperClass(IterateMapper.class);
//        job.setPartitionerClass(IteratePartitioner.class);
        job.setReducerClass(IterateReducer.class);

        // FIXME: Set up job.
        FileInputFormat.addInputPath(job, new Path(ii-1+"-iter-output"));
        FileOutputFormat.setOutputPath(job, new Path(ii+"-iter-output"));


        boolean ok = job.waitForCompletion(true);
        if (!ok) {
            throw new Exception("Job failed");
        }

        // FIXME:
        //    Use a counter to count up how many
        //    points are assigned to a different cluster
        //    in this iteration.

        int numChanges = 5;
        return numChanges < 10;
    }

    public static void writeOutput(Configuration conf, int ii) throws Exception {
        conf.setInt("itr", ii);

        Job job = Job.getInstance(conf, "write output");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(Mapper.class);
        job.setReducerClass(Reducer.class);
        job.setPartitionerClass(IteratePartitioner.class);
//        job.setOutputKeyClass(DataPoint.class);
//        job.setOutputValueClass(DataPoint.class);
//        job.setNumReduceTasks(K + 1);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path("last-iter-output"));
        FileOutputFormat.setOutputPath(job, new Path("output"));

        boolean ok = job.waitForCompletion(true);
        if (!ok) {
            throw new Exception("Job failed");
        }
    }
}
