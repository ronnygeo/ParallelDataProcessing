package assignment3;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by ronnygeo on 10/18/16.
 */
public class LinkedEdges extends ArrayWritable {
        public LinkedEdges(Class<? extends Writable> itemClass) {
            super(itemClass);
        }

        public LinkedEdges(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }

        @Override
        public String toString() {
            String value = "";
            Text[] values = (Text[]) get();
            for (int i = 0; i < values.length; i++) {
                value += values[i];
                if (i != values.length - 1) {
                    value += ",";
                }
            }
            return value;
        }
    }
