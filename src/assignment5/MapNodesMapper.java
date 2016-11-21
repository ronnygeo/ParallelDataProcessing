package assignment5;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

//MapNodesMapper uses Distributed Cache to read list of unique nodes and assign
// index to nodes and create the transition matrix, M'. This class also checks for
// dangling nodes and sets the value to 1/N if a dangling node is found.
public class MapNodesMapper extends Mapper<Object, Text, Text, NullWritable> {
	private HashMap<String, Long> nodes = new HashMap<>();
	private List<String> danglingnodes;
	private int N;
	private int itr;
	private static Pattern rowPattern;
	static {
		rowPattern = Pattern.compile("(\\d+,\\d+)");
	}

	@Override
	public void setup(Context ctx) throws IOException {
		danglingnodes = new ArrayList<>();
		N   = ctx.getConfiguration().getInt("N", -10);
		itr = ctx.getConfiguration().getInt("itr", -10);
		long ind = 0;
		//Setting up read from distributed cache
		Configuration conf = ctx.getConfiguration();
		Path adjmapping = new Path(ctx.getCacheFiles()[0]);
		FileSystem fs = FileSystem.get(adjmapping.toUri(), conf);
		FileStatus[] status = fs.listStatus(adjmapping);
		for (int i=0;i<status.length;i++){
				try{
					BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
					String node = null;
					while((node = br.readLine()) != null) {
						nodes.put(node, ind);
						ind++;
					}
				} catch(IOException ex) {
					System.err.println("Exception while reading adj list file: " + ex.getMessage());
				}
			}

		Path dangling = new Path(ctx.getCacheFiles()[1]);

		fs = FileSystem.get(dangling.toUri(), conf);
		status = fs.listStatus(dangling);
		for (int i=0;i<status.length;i++){
			try{
				BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
				String node = null;
				while((node = br.readLine()) != null) {
					danglingnodes.add(node);
					System.out.println(node);
				}
			} catch(IOException ex) {
				System.err.println("Exception while reading adj list file: " + ex.getMessage());
			}
		}

	}

		@Override
		public void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
			String[] line = value.toString().split("\t");
			StringBuilder row = new StringBuilder();
			Long i = nodes.get(line[1]);
			Long j = nodes.get(line[2]);
			if (i != null && j != null) {
				row.append(line[0] + "\t");
				row.append(i + "\t");
				row.append(j + "\t");
				if (Double.parseDouble(line[3]) == -99999) {
					Double t = 1.0 / N;
					line[3] = t.toString();
				}
				row.append(line[3]);
				ctx.write(new Text(row.toString()), NullWritable.get());
				if (danglingnodes.contains(line[2])) {
					ctx.write(new Text(line[0]+"\t"+i+"\t"+j+"\t"+1/N), NullWritable.get());
				}

			}
		}

		@Override
		public void cleanup(Context ctx) {
//			for (String node: nodes)
//				System.out.println(node);
		}
	}