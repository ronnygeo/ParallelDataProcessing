package assignment2.InMapCombiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

/**
 * Created by ronnygeo on 10/5/16.
 */

public class InMapCombiner {

    public static class LineMapper
            extends Mapper<Object,Text,Text,MinMaxTemp> {
        private static String word;
        //Hash to store the keys and values in map
        private Map<String, int[]> h;

        public void setup(Context context) {
            h = new HashMap<>();
        }

        public void map(Object key, Text value, Context context) {
            //Create a list of words
            List<String> words = Arrays.asList(value.toString().split(","));
            //Checking if the line has TMAX, then this line has the maximum temperature
            if (words.indexOf("TMAX") != -1) {
                int tmax = Integer.parseInt(words.get(3));
                //Getting the station ID
                word = words.get(0);
                //Checking if the hash contains the station ID already
                if (h.containsKey(word)) {
                    int[] val = h.get(word);
                    //Adding the maximum temperature
                    val[2] += tmax;
                    //Incrementing the count
                    val[3]++;
                    //Adding the data to the hash
                    h.put(word, val);
                } else {
                    //Array to store the minSum, minCount, maxSum, maxCount
                    int[] v = {0,0, tmax, 1};
                    h.put(word, v);
                }
            //Checking if the line has TMIN, then this line has the minimum temperature
            } else if (words.indexOf("TMIN") != -1) {
                //Getting the minimum temperature
                int tmin = Integer.parseInt(words.get(3));
                word = words.get(0);
                if (h.containsKey(word)) {
                    int[] val = h.get(word);
                    val[0] += tmin;
                    val[1]++;
                    h.put(word, val);
                } else {
                    int[] v = {tmin, 1, 0, 0};
                    h.put(word, v);
                }
            }
        }

        //After the mapper, emit all the values from the hash
        public void cleanup(Context context) throws IOException, InterruptedException {
            //Iterating through each element in the hash
            for (Map.Entry<String, int[]> e: h.entrySet()) {
                int meanMin = 99999;
                int meanMax = -99999;
                if (e.getValue()[1] != 0)
                    meanMin = e.getValue()[0]/e.getValue()[1];
                if (e.getValue()[3] != 0)
                    meanMax = e.getValue()[2]/e.getValue()[3];
                context.write(new Text(e.getKey()), new MinMaxTemp(meanMin, meanMax));
            }
        }
    }

    public static class TempReducer
    extends Reducer<Text,MinMaxTemp,Text,NullWritable> {
        private static final Text result = new Text();

        public void reduce(Text key, Iterable<MinMaxTemp> values, Context context) throws IOException, InterruptedException {
            //Getting the min and max mean values.
            List<Integer> mlist = getMeans(values);
            //Creating the result Text
            result.set(key.toString() + "," + ((mlist.get(0) == 99999)? "NULL": mlist.get(0)) + "," + (mlist.get(1) == -99999? "NULL": mlist.get(1)));
            context.write(result, NullWritable.get());
        }

        //Helper method to caculate the mean values.
        private List getMeans(Iterable<MinMaxTemp> values) {
            List<Integer> meanList = new ArrayList<>();
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
                    if (val.ifMinMax()) {
                        minSum += val.getMin();
                        minCount++;
                    }
                } else {
                    minSum += val.getMin();
                    minCount++;
                }
            }

            //Checking if the count is not 0, then calculate mean
            if (minCount > 0)
                minMean = minSum/minCount;
            if (maxCount > 0)
                maxMean = maxSum/maxCount;


            meanList.add(minMean);
            meanList.add(maxMean);
            return meanList;
        }
    }


    public static void main(String args[]) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Temperature with Combiner");
        job.setJarByClass(InMapCombiner.class);
        job.setMapperClass(LineMapper.class);
        job.setMapOutputValueClass(MinMaxTemp.class);
        job.setReducerClass(TempReducer.class);
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
