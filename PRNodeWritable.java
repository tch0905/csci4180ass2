import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PRNodeWritable implements Writable{
    private int nodeId;
    private Double p;
    private List<Integer> adjList;
    private boolean isNode; //true: a node; false: not a node

    public PRNodeWritable(){
    }

    public PRNodeWritable(int nodeId, Double p, boolean isNode){
        this.nodeId = nodeId;
        this.p = p;
        this.isNode = isNode;
        this.adjList = new ArrayList<Integer>();
    }

    public void set(PRNodeWritable node){
        this.nodeId = node.getNodeId();
        this.p = node.getP();
        this.isNode = node.isNode();
        this.adjList = new ArrayList<Integer>();
        for(Integer val: node.adjList){
            add(val);
        }
    }

    public void add(int m){
        this.adjList.add(m);
    }



    public void setP(Double p){
        this.p = p;
    }

    public int getNodeId() {
        return nodeId;
    }

    public Double getP() {
        return p;
    }

    public List<Integer> getAdjList() {
        return adjList;
    }

    public List<Integer> setAdjList(List<Integer> adjList) {
        this.adjList = adjList;
        return adjList;
    }

    public boolean isNode() {
        return isNode;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.nodeId);
        dataOutput.writeDouble(this.p);
        dataOutput.writeBoolean(this.isNode);
        dataOutput.writeInt(this.adjList.size());
        for(Integer val: adjList){
            dataOutput.writeInt(val);
        }
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.nodeId = dataInput.readInt();
        this.p = dataInput.readDouble();
        this.isNode = dataInput.readBoolean();
        int adjListSize = dataInput.readInt();
        this.adjList = new ArrayList<Integer>();
        for(int i=0; i<adjListSize; i++){
            int val = dataInput.readInt();
            add(val);
        }
    }

    public String toString(){
        StringBuilder s = new StringBuilder();
        s.append(nodeId).append(" ").append(p).append(" ").append(isNode).append(" ");
        if(adjList != null){
            for(Integer val : adjList){
                s.append(val.toString()).append(" ");
            }
        }
        return s.toString();
    }
}