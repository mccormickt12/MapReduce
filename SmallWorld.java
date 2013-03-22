/*
 *
 * CS61C Spring 2013 Project 2: Small World
 *
 * Partner 1 Name: Nathaniel Holland
 * Partner 1 Login: cs61c-hc
 *
 * Partner 2 Name: Tom McCormick
 * Partner 2 Login: cs61c-ec
 *
 * REMINDERS: 
 *
 * 1) YOU MUST COMPLETE THIS PROJECT WITH A PARTNER.
 * 
 * 2) DO NOT SHARE CODE WITH ANYONE EXCEPT YOUR PARTNER.
 * EVEN FOR DEBUGGING. THIS MEANS YOU.
 *
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.Math;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class SmallWorld {
    // Maximum depth for any breadth-first search
    public static final int MAX_ITERATIONS = 20;

    // Example writable type
    public static class EValue implements Writable {

        public int exampleInt; //example integer field
        public long[] exampleLongArray; //example array of longs

        public EValue(int exampleInt, long[] exampleLongArray) {
            this.exampleInt = exampleInt;
            this.exampleLongArray = exampleLongArray;
        }

        public EValue() {
            // does nothing
        }

        // Serializes object - needed for Writable
        public void write(DataOutput out) throws IOException {
            out.writeInt(exampleInt);

            // Example of serializing an array:
            
            // It's a good idea to store the length explicitly
            int length = 0;

            if (exampleLongArray != null){
                length = exampleLongArray.length;
            }

            // always write the length, since we need to know
            // even when it's zero
            out.writeInt(length);

            // now write each long in the array
            for (int i = 0; i < length; i++){
                out.writeLong(exampleLongArray[i]);
            }
        }

        // Deserializes object - needed for Writable
        public void readFields(DataInput in) throws IOException {
            // example reading an int from the serialized object
            exampleInt = in.readInt();

            // example reading length from the serialized object
            int length = in.readInt();

            // Example of rebuilding the array from the serialized object
            exampleLongArray = new long[length];
            
            for(int i = 0; i < length; i++){
                exampleLongArray[i] = in.readLong();
            }

        }

        public String toString() {
            // We highly recommend implementing this for easy testing and
            // debugging. This version just returns an empty string.
            return new String();
        }

    }

    /** Our datatype to store a node. Implements Writable but not
    * Comparable because we never use it as a key. 
    * Stores the neighbors as an arraylist of Longs.
    * Stores distance from source as an int, not a long.
    */
    public static class Node implements Writable {
        
        /** The ID of the node. Every node in the graph will
        * have it's own ID. */
        private long id;

        /** An int which keeps track of how far from the source this
        * Node is in it's given search. */
        private int distance_from_source;

        /** An ArrayList which keeps track of all the
        * Neighbors of this node as an ArrayList
        * of longs which holds their IDs. */
        private ArrayList<Long> neighbors;
        
        /** An int which keeps track of the status.
        * Should only be 0, 1, or 2.
        * 0 means we haven't visted the node yet.
        * 1 means we are on that node and need to explode to all
        * its neighbors.
        * 2 means we have already seen the node and should no longer
        * count or explode it. */
        private int status;
        
        /** a long which keeps track of where this node search
        * started at. Defaults to MAX_VALUE which means
        * that this node is the "generic node"*/
        private long start;

        /** Contsructor for a node which takes in a Long
        * i which is used as the ID. */
        public Node(long i) {
            id = i;
            neighbors = null;
            status = 0; // Default status is 0, on_node = 1, done = 2
            distance_from_source = Integer.MAX_VALUE;
            start = Long.MAX_VALUE;
        }

        /** Constructor for a node which takes in another 
        * Node N and then basically clones the Node N. */
        public Node(Node n) {
            id = n.getID();
            neighbors = n.getNeighbors();
            status = n.getStatus();
            distance_from_source = n.getDistance();
            start = n.getStart();
        }

        /** Null constructor, you should not use this. */
        public Node() {}

        /** Method to add a neighbor's ID to your neighbor list. */
        public void addNeighbor(long neighbor) {
            if (neighbors == null) {
                neighbors = new ArrayList<Long>();
            }
            neighbors.add(neighbor);
        }

        /** Method to return where the search on this particular
        * node started from. */
        public long getStart() {
            return start;
        }

        /** Method to set the starting node for the search
        * for this particular node */
        public void setStart(long s) {
            start = s;
        }

        /** A method that allows you to explicately set
        * the neighbors of a node by passing in
        * an arraylist of Longs. */
        public void setNeighbors(ArrayList<Long> n) {
            neighbors = n;
        }

        /** A method that will return the ID's of the 
        * neighbors of the nodes as an ArrayList of Longs. */
        public ArrayList<Long> getNeighbors() {
            return neighbors;
        }

        /** A method that returns the current status of the node. */
        public int getStatus() {
            return status;
        }

        /** A method to set the status of a Node to the int C. */
        public void setStatus(int c) {
            status = c;
        }

        /** A method which will increment the status from 0 to 1
        * and 1 to 2 but will not increment past 2. */
        public void incrementStatus() {
            if (status == 0 || status == 1) {
                status ++;
            }
        }

        /** A method which returns the ID of this node. */
        public long getID() {
            return id;
        }

        /** A method which increments the distance from the start.
        * Handles the case when distance = MAX_VALUE by
        * simply setting the distance_from_source to 1. */
        public void incrementDistance() {
            if (distance_from_source != Integer.MAX_VALUE) {
                distance_from_source ++;
            } else {
                distance_from_source = 1;
            }
        }

        /** A method which will return the distance from the source. */
        public int getDistance() {
            return distance_from_source;
        }

        /** A method to set the distance fromt he source to a given int D. */
        public void setDistance(int d) {
            distance_from_source = d;
        }

        /** The write method for this Writable. Farily straight forward.
        * see Hadoop's documentation on Writable for more information. */
        public void write (DataOutput out) throws IOException {
            out.writeLong(id);
            out.writeLong(start);
            out.writeInt(distance_from_source);
            out.writeInt(status);
            int length = 0;
            if (neighbors != null) {
                length = neighbors.size();
            }
            out.writeInt(length);
            for (int i = 0; i < length; i++) {
                out.writeLong(neighbors.get(i));
            }
        } 

        /** The readFeilds method for Writeable. Again see Hadoop's
        * documentation for more information. */
        public void readFields(DataInput in) throws IOException {
            id = in.readLong();
            start = in.readLong();
            distance_from_source = in.readInt();
            status = in.readInt();
            int length = in.readInt();
            neighbors = new ArrayList<Long>();
            for (int i = 0; i < length; i++) {
                neighbors.add(in.readLong());
            }
        }

        /** To string method for the node. Print's it out in the format:
        * "id: # status: # neighbors: [#, #] start: # distance: #"
        */
        public String toString() {
            String s = new String();
            s = s + "id: " + id
                + " status: " + status
                + " neighbors: ";
                if (neighbors != null) {
                    s = s + Arrays.toString(neighbors.toArray());
                } else {
                    s = s + "[null]";
                }
                s = s + " start: " + start
                + " distance: " + distance_from_source;
            return s;

        }
            
    }

    /* The first mapper. Part of the graph loading process, currently just an 
     * identity function. Modify as you wish. */
    public static class LoaderMap extends Mapper<LongWritable, LongWritable, 
        LongWritable, LongWritable> {

        @Override
        public void map(LongWritable key, LongWritable value, Context context)
                throws IOException, InterruptedException {

            // example of getting value passed from main
            //int inputValue = Integer.parseInt(context.getConfiguration().get("inputValue"));
            context.write(key, value);
            context.write(value, new LongWritable(Long.MAX_VALUE));
        }
    }


    /* The first reducer. This is also currently an identity function (although it
     * does break the input Iterable back into individual values). Modify it
     * as you wish. In this reducer, you'll also find an example of loading
     * and using the denom field.  
     */
    public static class LoaderReduce extends Reducer<LongWritable, LongWritable, LongWritable, Node> {

        public long denom;

        public void reduce(LongWritable key, Iterable<LongWritable> values, 
            Context context) throws IOException, InterruptedException {
            // We can grab the denom field from context: 
            denom = Long.parseLong(context.getConfiguration().get("denom"));

            // You can print it out by uncommenting the following line:
            // System.out.println(denom);

            // Example of iterating through an Iterable
            //for (LongWritable value : values){            
            //    context.write(key, value);
            //}
            Random r = new Random();
            int i = r.nextInt((int) denom);
            Node n = new Node(key.get());
           
            for (LongWritable value : values) {
                if (value.get() != Long.MAX_VALUE) {
                    n.addNeighbor(value.get());
                }
            }
            context.write(key, n);
            if (i==0) {
                Node on = new Node(n);
                //nt.setStarting(key);
                on.setDistance(0);
                on.incrementStatus();
                on.setStart(key.get());
                //System.out.println(on);
                context.write(key, on);
            }
        }

    }


    // ------- Add your additional Mappers and Reducers Here ------- //



    /** A mapper which takes in a key which is the ID of the node and 
    * then the node itself as a value. This mapper takes in the node and
    * if it's status is 1 it explodes all the neighbors, and if the status
    * is not 1 just passes it through the mapper and whites it again. */
    public static class BFSMap extends Mapper<LongWritable, Node, LongWritable, Node> {

        @Override	
        public void map(LongWritable key, Node value, Context context)
            throws IOException, InterruptedException {
	       
            if (value.getStatus() == 1 && value.getStart() != Long.MAX_VALUE) {
                ArrayList<Long> neighbors = value.getNeighbors();
                    for (Long l : neighbors) {
                        Node n = new Node(l);
                        n.setStatus(1);
                        n.setDistance(value.getDistance() + 1); 
                        n.setStart(value.getStart());
                        context.write(new LongWritable(l), n);
                    }
                value.incrementStatus();
            }
            context.write(key, value);
        }
    }
    
    /** The reducer for the Breadth First Search algorythm. This
    * basically takes in all the nodes with the same ID and then 
    * compiles the exploded nodes back int one node taking the max
    * of status and the min of distance. Also makes sure to keep track
    * of which starting node each node is searching from. */
    public static class BFSReduce extends Reducer<LongWritable,
					  Node, LongWritable, Node> {
        public void reduce(LongWritable key, Iterable<Node> values,
            Context context) throws IOException, InterruptedException {
 
            Node endNode = new Node(key.get());
            HashMap<Long, Node> endNodes = new HashMap<Long, Node>();
            ArrayList<Node> nodeArray = new ArrayList<Node>();
            ArrayList<Long> tempN = new ArrayList<Long>();
            for (Node n : values) {
                if (n.getNeighbors() != null && n.getNeighbors().size() > 0) {
                    tempN = n.getNeighbors();
                }
                if (!endNodes.containsKey(n.getStart())) {
                    Node on = new Node(key.get());
                    on.setStart(n.getStart());
                    on.setNeighbors(n.getNeighbors());
                    endNodes.put(n.getStart(), on);
                }
                Node temp = endNodes.get(n.getStart());
                if (n.getNeighbors() != null) {
                    temp.setNeighbors(n.getNeighbors());
                }
                if (n.getStatus() > temp.getStatus()) {
                    temp.setStatus(n.getStatus());
                }
                if (n.getDistance() < temp.getDistance()) {
                    temp.setDistance(n.getDistance());
                }
                endNodes.put(n.getStart(), temp);
            }
            Collection<Node> endNodeCollection = endNodes.values();
            for (Node n : endNodeCollection) {
                n.setNeighbors(tempN);
                context.write(key, n);
            }
        }
    }	

    /** The mapper which starts constructing the histogram. It takes
    * in the key, Node pair outputted by BFS and then puts out two
    * LongWritables which are the distance and then 1 which will make
    * this easy to sum. */
    public static class histogramMap extends Mapper<LongWritable, Node, LongWritable, LongWritable> {
	
        @Override
	    public void map(LongWritable key, Node value, Context context)
	       throws IOException, InterruptedException {
            
            if (value.getDistance() != Integer.MAX_VALUE) {
                context.write(new LongWritable(value.getDistance()),
                    new LongWritable(1));
            }
        }
    }


    /** A recurder which takes in a bunch of distances and then sums how often
    * they occur. */
    public static class histogramReduce extends Reducer<LongWritable,
        LongWritable, LongWritable, LongWritable> {
        
        public void reduce(LongWritable key, Iterable<LongWritable> values,
            Context context) throws IOException, InterruptedException {
            
            long sum = 0L;
            for (LongWritable value : values) {
                sum += value.get();
            }   
            context.write(key, new LongWritable(sum));
        } 
    }


    /** This was written by the instructors and then edited by me to make
    * all the types match. */
    public static void main(String[] rawArgs) throws Exception {
        GenericOptionsParser parser = new GenericOptionsParser(rawArgs);
        Configuration conf = parser.getConfiguration();
        String[] args = parser.getRemainingArgs();

        // Pass in denom command line arg:
        conf.set("denom", args[2]);
        // Set the input and outputs in the conf file
        conf.set("input", args[0]);
        conf.set("output", args[1]);

        // Sample of passing value from main into Mappers/Reducers using
        // conf. You might want to use something like this in the BFS phase:
        // See LoaderMap for an example of how to access this value
        conf.set("inputValue", (new Integer(5)).toString());

        // Setting up mapreduce job to load in graph
        Job job = new Job(conf, "load graph");

        job.setJarByClass(SmallWorld.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Node.class);

        job.setMapperClass(LoaderMap.class);
        job.setReducerClass(LoaderReduce.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // Input from command-line argument, output to predictable place
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("bfs-0-out"));

        // Actually starts job, and waits for it to finish
        job.waitForCompletion(true);

        // Repeats your BFS mapreduce
        int i = 0;
        while (i < MAX_ITERATIONS) {
            job = new Job(conf, "bfs" + i);
            job.setJarByClass(SmallWorld.class);

            // Feel free to modify these four lines as necessary:
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Node.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Node.class);

            // You'll want to modify the following based on what you call
            // your mapper and reducer classes for the BFS phase.
            job.setMapperClass(BFSMap.class); // currently the default Mapper
            job.setReducerClass(BFSReduce.class); // currently the default Reducer

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            // Notice how each mapreduce job gets gets its own output dir
            FileInputFormat.addInputPath(job, new Path("bfs-" + i + "-out"));
            FileOutputFormat.setOutputPath(job, new Path("bfs-"+ (i+1) +"-out"));

            job.waitForCompletion(true);
            i++;
        }

        // Mapreduce config for histogram computation
        job = new Job(conf, "hist");
        job.setJarByClass(SmallWorld.class);

        // Feel free to modify these two lines as necessary:
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        // DO NOT MODIFY THE FOLLOWING TWO LINES OF CODE:
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        // You'll want to modify the following based on what you call your
        // mapper and reducer classes for the Histogram Phase
        job.setMapperClass(histogramMap.class); // currently the default Mapper
        job.setReducerClass(histogramReduce.class); // currently the default Reducer

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // By declaring i above outside of loop conditions, can use it
        // here to get last bfs output to be input to histogram
        FileInputFormat.addInputPath(job, new Path("bfs-"+ i +"-out"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
