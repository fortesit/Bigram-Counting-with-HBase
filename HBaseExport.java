import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class HBaseExport {
    
	public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        HTable table = new HTable(config, "bigram_result");
        int theta = Integer.parseInt(args[0]);
        
        Scan s = new Scan();
        ResultScanner rs = table.getScanner(s);
        for (Result r:rs) {
            for (KeyValue kv : r.raw()) {
                if (Integer.parseInt(Bytes.toString(kv.getValue())) >= theta) {
                    System.out.print(new String(kv.getRow()) + " ");
                    System.out.println(new String(kv.getValue()));
                }
            }
        }
        
        rs.close();
	}
}
