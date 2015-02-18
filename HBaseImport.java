import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class HBaseImport {
    
    private static String family = new String("data");
    private static String qualifier = new String("content");
    private static int rowid = 1;
    
    public static void importFile(File folder, HTable table) throws IOException, FileNotFoundException {
        File[] listOfFiles = folder.listFiles();
        BufferedReader br;
        String line;
        
        for (File file : listOfFiles) {
            if (file.isFile()) {
                br = new BufferedReader(new FileReader(file));
                while ((line = br.readLine()) != null) {
                    Put p = new Put(Bytes.toBytes(new String(String.valueOf(rowid))));
                    p.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(line));
                    table.put(p);
                    rowid++;
                }
                br.close();
            }
            if (file.isDirectory()) {
                importFile(file, table);
            }
        }
    }
    
	public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        HTable table = new HTable(config, "raw_text");
        
        File folder = new File(args[0]);
        
        if(!folder.isDirectory()) {
            System.out.println("Cannot read directory.\n");
            return;
        }
        
        importFile(folder, table);
	}
}
