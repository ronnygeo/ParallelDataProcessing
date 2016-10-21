package kmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.WritableComparable;

class DataPoint implements WritableComparable<DataPoint> {
	public double x; 
	public double y;
	public String label;
	public boolean center;
	
	public DataPoint () {
		center = false;
		label  = "";
	}
	
	public DataPoint clone() {
		DataPoint dp = new DataPoint();
		dp.x = x;
		dp.y = y;
		dp.label = label;
		dp.center = center;
		return dp;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(x);
		out.writeDouble(y);
		out.writeUTF(label);
		out.writeBoolean(center);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		x = in.readDouble();
		y = in.readDouble();
		label = in.readUTF();
		center = in.readBoolean();
	}
	
	public String toString() {
		if (center) {
			label = "center";
		}
		
		return String.format("%.02f %.02f %s", x, y, label);
	}

	@Override
	public int compareTo(DataPoint arg0) {
		this.toString().compareTo(arg0.toString());
		return 0;
	}
	
	@Override
	public int hashCode() {
		return this.toString().hashCode();
	}

	public boolean equals(DataPoint yy) {
		return this.toString().equals(yy.toString());
	}
	
	@Override
	public boolean equals(Object oo) {
		return this.equals((DataPoint) oo);
	}
	
	public static ArrayList<DataPoint> readCenters(Configuration conf, int K, int ii) throws IOException {
		ArrayList<DataPoint> centers = new ArrayList<DataPoint>();

		Path centersFile = new Path("data" + ii + "/part-r-00000");
		SequenceFile.Reader rdr = new SequenceFile.Reader(conf, SequenceFile.Reader.file(centersFile));

		while (centers.size() < K) {
			DataPoint _k = new DataPoint();
			DataPoint ctr = new DataPoint();
			
			if (!rdr.next(_k, ctr)) {
				break;
			}

			if (!ctr.center) {
				rdr.close();
				throw new Error("That center isn't properly marked.");
			}
			
			centers.add(ctr);
		}
		
		rdr.close();

		return centers;
	}

}
