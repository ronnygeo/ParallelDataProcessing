package assignment3;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ronnygeo on 10/18/16.
 */

public class Node implements Writable, WritableComparable<Node> {
    Text name;
    DoubleWritable pageRank;
    LinkedEdges links;

    public void Node() {}

    public double getPageRank() {
        return pageRank.get();
    }

    public void setPageRank(double pageRank) {
        this.pageRank = new DoubleWritable(pageRank);
    }

    public LinkedEdges getLinks() {
        return links;
    }

    public void setLinks(LinkedEdges links) {
        this.links = links;
    }


    public Node() {}

    public Node(String name) {
        this.name = new Text(name);
    }

    public String getName() {return name.toString();}

    public void setName(String name) {
        this.name = new Text(name);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        name.readFields(in);
        pageRank.readFields(in);
        if (in.readBoolean()) {
            links.readFields(in);
        } else {
            links = null;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        name.write(out);
        pageRank.write(out);
        if (links != null) {
            out.writeBoolean(true);
            links.write(out);
        } else {
            out.writeBoolean(false);
        }

    }

    @Override
    public String toString() {
        StringBuilder value = new StringBuilder();
        value.append(name);
        value.append(":");
        value.append(pageRank);
        value.append(":");
        value.append(links.toString());
        return value.toString();
    }

    public int compareTo(Node n) {
        return getName().compareTo(n.getName());
    }

}
