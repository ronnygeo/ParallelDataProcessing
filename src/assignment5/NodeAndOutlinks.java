package assignment5;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ronnygeo on 11/15/16.
 */
public class NodeAndOutlinks implements Writable {
    String node;
    Double outlinks;

    public NodeAndOutlinks() {}

    public NodeAndOutlinks(String node, double outLinks) {
        this.node = node;
        this.outlinks = outLinks;
    }

    public NodeAndOutlinks(double c) {
        outlinks = c;
        node = "NULLNODE";
    }

    public double getOutlinks() {
        return outlinks;
    }

    public String getName() {
        return node;
    }
    public void setOutlink(Double out) {
        outlinks = out;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        node = in.readUTF();
        outlinks = in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(node);
        out.writeDouble(outlinks);
    }

    public String toString() {
        return node+"\t"+outlinks;
    }

    public boolean isNode() {
        return !node.equals("NULLNODE");
    }
}
