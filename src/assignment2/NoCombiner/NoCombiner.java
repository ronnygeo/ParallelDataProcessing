/**
 Write three MapReduce programs that calculate the mean minimum temperature and the mean maximum temperature,
 by sta on, for a single year of data. Reducer Output Format (lines do not have to be sorted by Sta onID):
 Sta onId0, MeanMinTemp0, MeanMaxTemp0
 */
package assignment2.NoCombiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;


public class NoCombiner {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, MinMaxTemp>{
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      //Creating a list of words
      List<String> words = Arrays.asList(value.toString().split(","));
      //Checking if the line has TMAX, then this line has the maximum temperature
      if (words.indexOf("TMAX") != -1) {
          word.set(words.get(0));
          context.write(word, new MinMaxTemp(99999, Integer.parseInt(words.get(3))));
        }
      //Checking if the line has TMIN, then this line has the minimum temperature
      else if (words.indexOf("TMIN") != -1) {
          word.set(words.get(0));
          context.write(word, new MinMaxTemp(Integer.parseInt(words.get(3)), -99999));
        }
    }
  }

  public static class IntSumReducer 
       extends Reducer<Text,MinMaxTemp,Text,NullWritable> {

    private static final Text result = new Text();
    public void reduce(Text key, Iterable<MinMaxTemp> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int minSum = 0;
      int maxSum = 0;
      int minCount = 0;
      int maxCount = 0;
      int minMean = 99999;
      int maxMean = -99999;

      for (MinMaxTemp val : values) {
        if (val.getMax() != -99999) {
          maxSum += val.getMax();
          maxCount++;
        } else {
          minSum += val.getMin();
          minCount++;
        }
      }

      if (minCount > 0)
      minMean = minSum/minCount;

      if (maxCount > 0)
        maxMean = maxSum/maxCount;

      result.set(key.toString() + "," + (minMean == 99999? "NULL": minMean) + "," + (maxMean == -99999? "NULL": maxMean));
      context.write(result, NullWritable.get());
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "No Combiner");
    job.setJarByClass(assignment2.NoCombiner.NoCombiner.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setMapOutputValueClass(MinMaxTemp.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

