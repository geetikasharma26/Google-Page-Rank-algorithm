import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.*;

public class FinalRankingReducer extends MapReduceBase implements Reducer<DoubleWritable, Text, Text, Text> {

private String bucketName;
    
    public void configure(JobConf job){
    	
    	bucketName = job.get("bucketName");
    }
	
	public void reduce(DoubleWritable key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	
		while(values.hasNext())
		{    
		  String s = key.toString();
		  
		  Double d = Double.parseDouble(s);
		  
		  Configuration con1 = new Configuration();
			BufferedReader br = null;
			FileSystem fs = null;
			int numPages = 0;
			Path path = new Path(bucketName + "/temp/PageRank.n.out/part-00000");

			try {
				fs = path.getFileSystem(con1);
				br = new BufferedReader(new InputStreamReader(fs.open(path)));
				String line = br.readLine();
				if (line != null && !line.isEmpty()) {                  
				

	                      numPages = Integer.parseInt(line.split("\t")[1]);

	            br.close();            
				}
	               

				   }catch(Exception e){

					   e.printStackTrace();
	                }
		  
		  if (d >= (5.0/numPages)){
		 output.collect(values.next(), new Text(s));
		 }
		}
		
	}
}
