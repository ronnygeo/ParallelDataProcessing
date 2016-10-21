package InClass.ex2; /**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//package cs6240;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DivisibleLengths {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, IntWritable, Text>{
    
    private final static Pattern     nw1 = Pattern.compile("[^'a-zA-Z]");
    private final static Pattern     nw2 = Pattern.compile("(^'+|'+$)");
    private IntWritable divisor = new IntWritable();
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        Matcher mm1 = nw1.matcher(itr.nextToken());
        Matcher mm2 = nw2.matcher(mm1.replaceAll("")); 
        String ww = mm2.replaceAll("").toLowerCase();

        if (!ww.equals("")) {
          double length = ww.length();
          for (int i = 2; i <= length; i++) {
            Boolean prime_flag = true;
          for (int j = 2; j <= i/2; j++) {
            if (i % j == 0) {
              prime_flag = false;
              break;
            }
          }
            if (prime_flag && length % i == 0) {
              word.set(ww);
              divisor = new IntWritable(i);
              context.write(divisor, word);
            }

          }
        }
      }

    }
  }
  
  public static class IntSumReducer 
       extends Reducer<IntWritable, Text, IntWritable, Text> {
    private Text longest = new Text();

    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      for (Text val : values) {
        if (val.getLength() > longest.getLength()) {
          longest = val;
        }
      }
      context.write(key, longest);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count");
    job.setJarByClass(DivisibleLengths.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
