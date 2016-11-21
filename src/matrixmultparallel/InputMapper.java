package matrixmultparallel;


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
			String i = mat[1];
			Matcher rowValue = rowPattern.matcher(mat[2]);
				while (rowValue.find()) {
					cell = rowValue.group(1);
					String j = cell.split(",")[0];
					String val = cell.split(",")[1];
						if (mat[0].equals("A")) {
							ctx.write(new Text(j), new Text(mat[0] + "," + i + "," + val));
							ctx.write(new Text(j), new Text("B" + "," + 0 + "," + 4));
						}

//						else {
//							ctx.write(new Text(i), new Text(mat[0] + "," + j + "," + val));
//						}
				}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
 
