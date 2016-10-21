package kmeans;


import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;

public class KMeans {
	static final int K = 5;
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setInt("K", K);
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Path input;
		if (otherArgs.length > 0) {
			input = new Path(otherArgs[0]);
		}
		else {
			input = new Path("data.tsv.bz2");
		}

		readAndIterate(conf, input);

		int ii = 0;
		boolean done = false;
		while (!done) {
			done = iterate(conf, ii++);
		}
		
		writeOutput(conf, ii);
	}
	
	public static void readAndIterate(Configuration conf, Path input) throws Exception {
		conf.setInt("itr", -1);
		
		Job job = Job.getInstance(conf, "read input");
		job.setJarByClass(KMeans.class);
		job.setMapperClass(InputMapper.class);
		job.setReducerClass(IterateReducer.class);
		job.setPartitionerClass(IteratePartitioner.class);
		job.setOutputKeyClass(DataPoint.class);
		job.setOutputValueClass(DataPoint.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setNumReduceTasks(K + 1);

		FileInputFormat.addInputPath(job, new Path("data.tsv.bz2"));
		FileOutputFormat.setOutputPath(job, new Path("first-iter-input"));

		boolean ok = job.waitForCompletion(true);
		if (!ok) {
			throw new Exception("Job failed");
		}
	}

	public static boolean iterate(Configuration conf, int ii) throws Exception {
		Job job = Job.getInstance(conf, "k-Means iteration");
        // The plan requires K + 1 reducers.
		job.setNumReduceTasks(K + 1);
		
        // FIXME: Set up job.
		
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
		job.setJarByClass(KMeans.class);
		job.setMapperClass(Mapper.class);
		job.setReducerClass(Reducer.class);
		job.setPartitionerClass(IteratePartitioner.class);
		job.setOutputKeyClass(DataPoint.class);
		job.setOutputValueClass(DataPoint.class);
		job.setNumReduceTasks(K + 1);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, new Path("last-iter-output"));
		FileOutputFormat.setOutputPath(job, new Path("output"));

		boolean ok = job.waitForCompletion(true);
		if (!ok) {
			throw new Exception("Job failed");
		}
	}
}
