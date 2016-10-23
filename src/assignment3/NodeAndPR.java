package assignment3;

import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ronnygeo on 10/22/16.
 */
public class NodeAndPR implements Writable {
    Node node = new Node();
    DoubleWritable pr = new DoubleWritable(0);

    public NodeAndPR(Node n) {
        node = n;
    }

    public NodeAndPR(double p) {
        pr = new DoubleWritable(p);
    }

    public NodeAndPR(Node n, double p) {
        node = n;
        pr = new DoubleWritable(p);
    }

    public NodeAndPR() {}

    public Node getNode() {
        return node;
    }

    public double getPr() {
        return pr.get();
    }

    public boolean isNode() {
        if (node.getName().equals(" ")) {
            return false;
        } else {
//            System.out.println(node);
            return true;
        }
    }

   public String toString() {
       StringBuilder str = new StringBuilder();
       str.append(node + " " + pr);
       return str.toString();
   }

   @Override
   public void readFields(DataInput in) throws IOException {
       if (in.readBoolean()) {
           node.readFields(in);
       } else {
           node = new Node();
       }

       if (in.readBoolean()) {
           pr.readFields(in);
       } else {
           new DoubleWritable(0);
       }
   }

   @Override
    public void write(DataOutput out) throws IOException {
        if (node == null) {
            out.writeBoolean(false);
//            NullWritable.get().write(out);
        } else {
            out.writeBoolean(true);
            node.write(out);
        }

        if (pr == null) {
//            NullWritable.get().write(out);
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            pr.write(out);
        }
    }
}
