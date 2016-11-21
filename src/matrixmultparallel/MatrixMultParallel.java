package matrixmultparallel;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Created by ronnygeo on 10/17/16.
 */
//MatrixMult class is the main class that runs the page rank job.
public class MatrixMultParallel {
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
            input = new Path("inputFiles/matB");
            output = new Path("out");
        }
//        //Read the data from the input and create the adj list data file
        readAndIterate(conf, input);
    }
//
    public static void readAndIterate(Configuration conf, Path input) throws Exception {
        conf.setInt("itr", -1);
        conf.setLong("dangling", 0);
        Job job = Job.getInstance(conf, "Graph creator job");
        job.setJarByClass(MatrixMultParallel.class);
        job.setMapperClass(InputMapper.class);
        job.setReducerClass(ListReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, new Path("outMatTemp"));

        boolean ok = job.waitForCompletion(true);
        if (!ok) {
            throw new Exception("Job failed");
        }
        job = Job.getInstance(conf, "Matrix summer job");
        job.setJarByClass(MatrixMultParallel.class);
        job.setMapperClass(MatrixSumMapper.class);
        job.setReducerClass(MatrixSumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path("outMatTemp"));
        FileOutputFormat.setOutputPath(job, new Path("outMatParallel"));
        ok = job.waitForCompletion(true);
        if (!ok) {
            throw new Exception("Job failed");
        }
    }


}
