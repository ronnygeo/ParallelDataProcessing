package assignment3;

import java.io.*;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by ronnygeo on 10/23/16.
 */
//SortComparator class is used to sort a WritableComparator before
    //passing it to the Reducer. We use it to sort the DoubleWritable value from output mapper
public class SortComparator extends WritableComparator {

        protected SortComparator() {
            super(DoubleWritable.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            DoubleWritable k1 = (DoubleWritable) w1;
            DoubleWritable k2 = (DoubleWritable) w2;

            //Sort in descending order
            return -1 * k1.compareTo(k2);
        }
}
