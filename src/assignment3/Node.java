package assignment3;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by ronnygeo on 10/18/16.
 */

public class Node implements Writable, WritableComparable<Node> {
    String name = "";
    double pageRank = 0;
    ArrayList<String> links = new ArrayList<>();

    public Node() {}

    public Node(String name) {
        this.name = name;
    }

    public double getPageRank() {
        return pageRank;
    }

    public void setPageRank(double pageRank) {
        this.pageRank = pageRank;
    }

    public ArrayList<String> getLinks() {
        return links;
    }

    public void setLinks(ArrayList<String> links) {
        this.links = links;
    }

    public String getName() {return name;}

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        name = in.readUTF();
        pageRank = in.readDouble();
        int len = in.readInt();
        links = new ArrayList<>();
        while (len > 0) {
            String link = in.readUTF();
            links.add(link);
            len--;
        }
//        if (in.readBoolean()) {
//            links.readFields(in);
//        } else {
//            links = null;
//        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeDouble(pageRank);
        out.writeInt(links.size());
        for (int i=0; i < links.size(); i++) {
            out.writeUTF(links.get(i));
        }
//        name.write(out);
//        pageRank.write(out);
//        if (links != null && links.size() > 0) {
//            out.writeBoolean(true);
//            links.write(out);
//        } else {
//            out.writeBoolean(false);
//        }
    }

    @Override
    public String toString() {
        StringBuilder value = new StringBuilder();
        value.append(name);
        value.append(":");
        value.append(pageRank);
        value.append(":");
        value.append(printLinks());
//        value.append(links.toString());
        return value.toString();
    }

    public String printLinks() {
        String value = "";
            for (int i = 0; i < links.size(); i++) {
                value += links.get(i);
                if (i != links.size() - 1) {
                    value += ",";
                }
            }
        return value;
    }

    public int compareTo(Node n) {
        return getName().compareTo(n.getName());
    }

}
