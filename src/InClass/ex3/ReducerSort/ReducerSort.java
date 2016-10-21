//package cs6240;
package InClass.ex3.ReducerSort;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.file.tfile.TFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.*;

public class ReducerSort {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "word count");
        job.setJarByClass(ReducerSort.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        // FileSystem fs = FileSystem.get(getConf());
        // fs.delete(new Path("temp"), true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
    private final static Pattern     nw1 = Pattern.compile("[^'a-zA-Z]");
    private final static Pattern     nw2 = Pattern.compile("(^'+|'+$)");
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());

        while (itr.hasMoreTokens()) {
            Matcher mm1 = nw1.matcher(itr.nextToken());
            Matcher mm2 = nw2.matcher(mm1.replaceAll("")); 
            String ww = mm2.replaceAll("").toLowerCase();

            if (!ww.equals("")) {
                word.set(ww);
                context.write(word, one);
            }
        }
    }
}
 
class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable count = new IntWritable();
    private Text word = new Text();
    private HashMap<String, Integer> counts = new HashMap<>();
    public void setup(Context ctx) {
        //HashMap<String, Integer> counts = new HashMap<>();
    }

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
    counts.put(key.toString(), sum);
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        Map<String, Integer> sorted_map = sortByValue(counts);
        for (Map.Entry e: sorted_map.entrySet()) {
            word.set((String) e.getKey());
            count.set((Integer) e.getValue());
            context.write(word, count);
        }
    }

    public static <K, V extends Comparable<? super V>> Map<K, V>
    sortByValue( Map<K, V> map )
    {
        List<Map.Entry<K, V>> list =
                new LinkedList<>( map.entrySet() );
        Collections.sort( list, new Comparator<Map.Entry<K, V>>()
        {
            public int compare( Map.Entry<K, V> o1, Map.Entry<K, V> o2 )
            {
                return (o1.getValue()).compareTo( o2.getValue() );
            }
        } );

        Map<K, V> result = new LinkedHashMap<K, V>();
        for (Map.Entry<K, V> entry : list)
        {
            result.put( entry.getKey(), entry.getValue() );
        }
        return result;
    }

}





