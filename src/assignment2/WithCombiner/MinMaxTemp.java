package assignment2.WithCombiner;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ronnygeo on 10/5/16.
 */
/* An Object to store the Minimum and Maximum temperature recorded at a particular station.
This object is used as the value inside the program.
* This Class implements the Writable. */
public class MinMaxTemp implements Writable {
    int max;
    int min;

    public MinMaxTemp() {
        max = -99999;
        min = 99999;
    }

    public MinMaxTemp(int m, int ma) {
        max = ma;
        min = m;
    }

    public void set(int m, int ma) {
        max = ma;
        min = m;
    }

    public int getMax() {
        return max;
    }

    public int getMin() {
        return min;
    }

    public Boolean ifMinMax() {
        return ((max != -99999) && (min != 99999));
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(min);
        out.writeInt(max);
    }

    public void readFields(DataInput in) throws IOException {
        min = in.readInt();
        max = in.readInt();
    }

    @Override
    public String toString() {
        return min+" "+max;
    }
}
