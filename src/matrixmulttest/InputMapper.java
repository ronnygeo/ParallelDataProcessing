package matrixmulttest;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// The input mapper class goes through the input file and parses it into the adjacency matrix
public class InputMapper extends Mapper<Object, Text, Text, Text> {
	private static Pattern rowPattern;
	private String cell;
	private static long n;

	static {
		// Keep only html pages not containing tilde (~).
		rowPattern = Pattern.compile("(\\d+,\\d+)");
		n = 5;
	}

	public void setup(Context ctx) throws IOException, InterruptedException {
		//Initializing a Map to store the adjacency list

	}

	public void cleanup(Context ctx) throws IOException, InterruptedException {
		//Write each node and its adjacent edge as value

	}

	public void map(Object _k, Text line, Context ctx) throws InterruptedException, IOException {
		try {
			//Get the links and node from the current line
			String[] mat = line.toString().split(":");
			Long i = Long.parseLong(mat[1]);
			int count = 5;
			Matcher rowValue = rowPattern.matcher(mat[2]);
				while (rowValue.find()) {
					cell = rowValue.group(1);
					Long j = Long.parseLong(cell.split(",")[0]);
					double val = Double.parseDouble(cell.split(",")[1]);
						if (mat[0].equals("A")) {
							ctx.write(new Text(i.toString()), new Text(mat[0] + "," + j + "," + val));
							ctx.write(new Text(i.toString()), new Text("B" + "," + j + "," + 4));
					}
//					else {
//							for (Integer k =0; k < 3; k++)
//							ctx.write(new Text(k.toString()), new Text("B" + "," + i + "," + val));
//						}
				}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
 
