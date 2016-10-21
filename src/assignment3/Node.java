package assignment3;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ronnygeo on 10/18/16.
 */
public class Node implements WritableComparable<Node> {
    private String name;

    public double getPageRank() {
        return pageRank;
    }

    public void setPageRank(double pageRank) {
        this.pageRank = pageRank;
    }

    public LinkedEdges getLinks() {
        return links;
    }

    public void setLinks(LinkedEdges links) {
        this.links = links;
    }

    private double pageRank;
    private LinkedEdges links;

    public Node(String name) {
        this.name = name;
    }

    public String getName() {return name;}

    public void setName(String name) {
        this.name = name;
    }

    public void readFields(DataInput in) throws IOException {
        name = in.readUTF();
        pageRank = in.readDouble();
        links.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeDouble(pageRank);
        links.write(out);
    }

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
        return this.name.compareTo(n.getName());
    }
}
