package assignment2.StationByYear;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

/**
 * Created by ronnygeo on 10/5/16.
 */
public class StationByYearList {

    public static class TokenizerMapper
            extends Mapper<Object, Text, StationAndYear, MinMaxTemp> {
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //Creating a list of words
            List<String> words = Arrays.asList(value.toString().split(","));
            //Checking if the line has TMAX, then this line has the maximum temperature
            if (words.indexOf("TMAX") != -1) {
                word.set(words.get(0));
                context.write(new StationAndYear(words.get(0), Integer.parseInt(words.get(1).substring(0, 4))), new MinMaxTemp(99999, Integer.parseInt(words.get(3))));
            }
            //Checking if the line has TMIN, then this line has the minimum temperature
             else if (words.indexOf("TMIN") != -1) {
                word.set(words.get(0));
                context.write(new StationAndYear(words.get(0), Integer.parseInt(words.get(1).substring(0, 4))), new MinMaxTemp(Integer.parseInt(words.get(3)), -99999));
            }
        }
    }

    public static class StationReducer
            extends Reducer<StationAndYear,MinMaxTemp,Text,NullWritable> {

        private static final Text result = new Text();
        private Map<String, ArrayList> h;

        public void setup(Context context) {
            h = new HashMap<String, ArrayList>();
        }


        public void reduce(StationAndYear key, Iterable<MinMaxTemp> values,
                           Context context
        ) throws IOException, InterruptedException {
            String stationId = key.getStationId().toString();
            int year = key.getYear().get();

            //Checking if the hash has the stationID already stored
            if (h.containsKey(stationId)) {
                //Fetching the values from the Hash with key.
                ArrayList<int[]> v = h.get(stationId);
                //Adding the new data to the existing list
                v.add(generateDataYear(year, values));
                //Updating the hash with the new values
                h.put(key.getStationId().toString(), v);
//                    }
            } else {
                //Create a new List to store the Array of year data
                ArrayList<int[]> v = new ArrayList<>();
                v.add(generateDataYear(year, values));
                h.put(key.getStationId().toString(), v);
            }
        }

        //Cleanup method to write the final data to the file.
        public void cleanup(Context context) throws IOException, InterruptedException {
            //Iterates through the Hash and get each record
            for (Map.Entry<String, ArrayList> y : h.entrySet()) {
                ArrayList vlist = y.getValue();
                //String to store the final output line
                String res;
                res = "";
                res += y.getKey() + ", [";
                int c = 0;
                //Get data for each year from the list
                for (Object a : vlist) {
                    c++;
                    int[] ylist = (int[]) a;
                    if (c > 1) res += ",";
                    res += "(" + ylist[0] + ", " + (ylist[1] == 99999 ? "NULL" : ylist[1]) + ", " + (ylist[2] == -99999 ? "NULL" : ylist[2]) + ")";
                }
                res += "]";
                result.set(res);
                context.write(result, NullWritable.get());
            }
        }

        //Creates an array with the year, minMean and maxMean
        private int[] generateDataYear(int year, Iterable<MinMaxTemp> values) {
            int[] ylist = new int[3];
            List<Integer> mlist = getMeans(values);
            ylist[0] = year;
            ylist[1] = mlist.get(0);
            ylist[2] = mlist.get(1);
            return ylist;
        }
    }

        //The partition class sends the records a station to a single reducer.
        public static class StationPartitioner extends Partitioner<StationAndYear, MinMaxTemp> {
            public StationPartitioner() {}

            public int getPartition(StationAndYear key, MinMaxTemp value, int numReducers) {
                return Math.abs(key.getStationId().hashCode() % numReducers);
            }
        }

        //The Group Comparator groups the input data to the reducer by comparing the keys
        public static class GroupComparator extends WritableComparator{

            public GroupComparator() {
                super(StationAndYear.class, true);
            }

            @Override
            public int compare(WritableComparable w1, WritableComparable w2) {
                StationAndYear ip1 = (StationAndYear) w1;
                StationAndYear ip2 = (StationAndYear) w2;
                return ip1.compareTo(ip2);
            }
        }

        //A Key Comparator class that compares the keys between two records and helps in sorting them after the map stage.
        public static class KeyComparator extends WritableComparator {
            protected KeyComparator() {
                super(StationAndYear.class, true);
            }

            @Override
            public int compare(WritableComparable w1, WritableComparable w2) {
                StationAndYear s1 = (StationAndYear) w1;
                StationAndYear s2 = (StationAndYear) w2;
                return s1.compareTo(s2);
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
            job.setJarByClass(StationByYearList.class);
            job.setMapperClass(TokenizerMapper.class);
//            job.setSortComparatorClass(KeyComparator.class);
            job.setGroupingComparatorClass(GroupComparator.class);
            job.setPartitionerClass(StationPartitioner.class);
            job.setMapOutputKeyClass(StationAndYear.class);
            job.setMapOutputValueClass(MinMaxTemp.class);
            job.setReducerClass(StationReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
//            job.setNumReduceTasks(4);
            for (int i = 0; i < otherArgs.length - 1; ++i) {
                FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
            }
            FileOutputFormat.setOutputPath(job,
                    new Path(otherArgs[otherArgs.length - 1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }

        //Helper method to calculate the mean values from the input values.
        private static List getMeans(Iterable<MinMaxTemp> values) {
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



