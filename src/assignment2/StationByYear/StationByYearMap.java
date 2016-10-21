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
//Reducer Output Format (lines do not have to be sorted by Sta onID):
//        StationIda, [(1880, MeanMina0, MeanMaxa0), (1881, MeanMina1, MeanMaxa1) ... (1889 ...)]

public class StationByYearMap {
        public static class TokenizerMapper
                extends Mapper<Object, Text, StationAndYear, MinMaxTemp> {
            private Text word = new Text();

            public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException {
                List<String> words = Arrays.asList(value.toString().split(","));
                if (words.indexOf("TMAX") != -1) {
                    word.set(words.get(0));
                    context.write(new StationAndYear(words.get(0), Integer.parseInt(words.get(1).substring(0, 4))), new MinMaxTemp(99999, Integer.parseInt(words.get(3))));
                } else if (words.indexOf("TMIN") != -1) {
                    word.set(words.get(0));
                    context.write(new StationAndYear(words.get(0), Integer.parseInt(words.get(1).substring(0, 4))), new MinMaxTemp(Integer.parseInt(words.get(3)), -99999));
                }
            }
        }

        public static class IntSumReducer
                extends Reducer<StationAndYear,MinMaxTemp,Text,NullWritable> {

            private static final Text result = new Text();
            private Map<String, TreeMap> h;

            public void setup(Context context) {
                h = new HashMap<String, TreeMap>();
            }


            public void reduce(StationAndYear key, Iterable<MinMaxTemp> values,
                               Context context
            ) throws IOException, InterruptedException {
                System.out.println(key);
                if (h.containsKey(key.getStationId().toString())) {
                    TreeMap<Integer, int[]> current = h.get(key.getStationId().toString());
                        current.putAll(makeValue(key, values));

                        h.put(key.getStationId().toString(), current);
//                    }
                }else {
                    TreeMap<Integer, int[]> ymap = makeValue(key, values);
                    h.put(key.getStationId().toString(), ymap);
                }
            }

            public void cleanup(Context context) throws IOException, InterruptedException {
                for (Map.Entry<String, TreeMap> y: h.entrySet()) {
                    TreeMap<Integer, int[]> vmap = y.getValue();
                    String res;
                    res = "";
                    res += y.getKey() + ", [";
                    int c = 0;
                    for (Map.Entry<Integer, int[]> e: vmap.entrySet()) {
                        c++;
                        if (c > 1) res += ",";
                        res += "(" + e.getKey() + ", " + (e.getValue()[0] == 99999? "NULL": e.getValue()[0]) + ", " + (e.getValue()[1] == -99999? "NULL": e.getValue()[1]) + ")";
                    }
                    res += "]";
                    result.set(res);
                    context.write(result, NullWritable.get());
                }
            }

            private TreeMap makeValue(StationAndYear key, Iterable<MinMaxTemp> values) {
                TreeMap<Integer, int[]> ymap = new TreeMap<>();
                List<Integer> mlist = getValues(values);
                int[] ml = {mlist.get(0), mlist.get(1)};
                ymap.put(key.getYear().get(), ml);
                return ymap;
            }

            private List getValues(Iterable<MinMaxTemp> values) {
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

                if (minCount > 0)
                    minMean = minSum/minCount;

                if (maxCount > 0)
                    maxMean = maxSum/maxCount;

                meanList.add(minMean);
                meanList.add(maxMean);
                return meanList;
            }
        }

        public static class StationPartitioner extends Partitioner<StationAndYear, MinMaxTemp> {
            public StationPartitioner() {}

            public int getPartition(StationAndYear key, MinMaxTemp value, int numReducers) {
                return Math.abs(key.getStationId().hashCode() % numReducers);
            }
        }

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

        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length < 2) {
                System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
                System.exit(2);
            }
            Job job = new Job(conf, "No Combiner");
            job.setJarByClass(StationByYearMap.class);
            job.setMapperClass(TokenizerMapper.class);
//            job.setSortComparatorClass(KeyComparator.class);
            job.setGroupingComparatorClass(GroupComparator.class);
            job.setPartitionerClass(StationPartitioner.class);
            job.setMapOutputKeyClass(StationAndYear.class);
            job.setMapOutputValueClass(MinMaxTemp.class);
            job.setReducerClass(IntSumReducer.class);
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
}



