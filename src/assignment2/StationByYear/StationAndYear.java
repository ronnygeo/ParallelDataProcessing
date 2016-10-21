package assignment2.StationByYear;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ronnygeo on 10/5/16.
 */
/* An Object to store the StationId and year to be used as the key inside the program.
* This Class implements the Writable and WritableComparable interfaces. */
public class StationAndYear
        implements Writable, WritableComparable<StationAndYear> {
    private Text stationId = new Text();
    private IntWritable year = new IntWritable();

    public StationAndYear() {}

    public Text getStationId() {
        return stationId;
    }

    public IntWritable getYear() {
        return year;
    }

    public StationAndYear(String station, int year) {
        this.stationId = new Text(station);
        this.year = new IntWritable(year);
    }

    public void readFields(DataInput in) throws IOException {
        stationId = new Text(in.readUTF());
        year = new IntWritable(in.readInt());
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(stationId.toString());
        out.writeInt(year.get());
    }


    @Override
    public int compareTo(StationAndYear other) {
        int c = this.stationId.compareTo(other.getStationId());
        if (c == 0) {
            c = this.year.compareTo(other.getYear());
        }
        return c;
    }

    public int compareYear(StationAndYear other) {
        return this.year.compareTo(other.getYear());
    }
}
