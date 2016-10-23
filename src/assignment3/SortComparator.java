package assignment3;

import java.io.*;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by ronnygeo on 10/23/16.
 */
public class SortComparator extends WritableComparator {

        protected SortComparator() {
            super(DoubleWritable.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            DoubleWritable k1 = (DoubleWritable) w1;
            DoubleWritable k2 = (DoubleWritable) w2;

            return -1 * k1.compareTo(k2);
        }
}
