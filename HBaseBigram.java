import java.io.*;
import java.util.*;
import java.lang.*;
import java.util.regex.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class HBaseBigram {
    
    private static Configuration conf = HBaseConfiguration.create();
    private static String sourceTable = new String("raw_text");
    private static String targetTable = new String("bigram_result");
    
    public static class Map extends TableMapper<Text, IntWritable> {
        public static final byte[] CF = "data".getBytes();
        public static final byte[] ATTR1 = "content".getBytes();
        
        private LinkedList wordQueue;
        private java.util.Map<String, Integer> wordMap = new HashMap<String, Integer>();
        
        protected void setup(Context context) {
			wordQueue = new LinkedList();
		}
        
        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            
            String emit_key;
            Pattern p = Pattern.compile("[a-zA-Z]+");
            Matcher m = p.matcher((Bytes.toString(value.getValue(CF, ATTR1))).toLowerCase());
            
            // If no element in queue, then add a word
			if (wordQueue.size() == 0) {
                if (m.find()) {
                    String word = m.group();
                    wordQueue.add(word);
                }
            }
            
            // The queue now contain 1 element at this point
			while (m.find()) {
                String word = m.group();
                
                wordQueue.add(word);
                emit_key = wordQueue.get(0) + " " + wordQueue.get(1);
                
                if (!wordMap.containsKey(emit_key)) {
                    wordMap.put(emit_key, 0);
                }
                wordMap.put(emit_key, wordMap.get(emit_key)+1);
				wordQueue.removeFirst();
			}
            
        }
        
        // to emit things
		protected void cleanup(Context context)	throws IOException, InterruptedException {
			for (java.util.Map.Entry<String, Integer> e : wordMap.entrySet()) {
				context.write(new Text(e.getKey()), new IntWritable(wordMap.get(e.getKey())));
			}
		}
    }
    
    public static class Reduce extends TableReducer<Text, IntWritable, ImmutableBytesWritable>  {
        public static final byte[] CF = "result".getBytes();
        public static final byte[] COUNT = "count".getBytes();
        
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    		int sum = 0;
            
			for (IntWritable val : values) {
				sum += val.get();
			}
            
    		Put put = new Put(Bytes.toBytes(key.toString()));
    		put.add(CF, COUNT, Bytes.toBytes(Integer.toString(sum)));
            
    		context.write(null, put);
        }
    }
    
	public static void main(String[] args) throws Exception {
        Job job = new Job(conf, "HBaseBigram");
        job.setJarByClass(HBaseBigram.class);    // class that contains mapper
        
        Scan scan = new Scan();
        scan.setCaching(50000);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
        // set other scan attrs
        
        TableMapReduceUtil.initTableMapperJob(
                                              sourceTable,        // input table
                                              scan,               // Scan instance to control CF and attribute selection
                                              Map.class,     // mapper class
                                              Text.class,         // mapper output key
                                              IntWritable.class,  // mapper output value
                                              job);
        TableMapReduceUtil.initTableReducerJob(
                                               targetTable,        // output table
                                               Reduce.class,    // reducer class
                                               job);
        job.setNumReduceTasks(1);   // at least one, adjust as required
        
        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
        
	}
    
}
