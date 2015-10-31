
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Scanner;
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

public class TempInputIterReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
   //public static int count = 0;
	
private String bucketName;
    
    public void configure(JobConf job){
    	
    	bucketName = job.get("bucketName");
    }
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {


		
		
		//new coode to open the file
	 	Configuration con1 = new Configuration();
		BufferedReader br = null;
		FileSystem fs = null;
		int numPages = 0;
		//Path path = new Path("s3://aws-mayank-bucket/results/PageRank.n.out");
		Path path = new Path(bucketName + "/temp/PageRank.n.out/part-00000");
		try {
			fs = path.getFileSystem(con1);
			br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line = br.readLine();
			if (line != null && !line.isEmpty()) {                  
			
                       System.out.println(line);
                      numPages = Integer.parseInt(line.split("\t")[1]);
//
//                      System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+numPages);
                      // line=br.readLine();
            br.close();            
			}
               

			   }catch(Exception e){
				   System.out.println("Exception thrown");
				   e.printStackTrace();
                }
		

		
		String pagerank = String.valueOf(1.0/numPages) + "\t" ;
        boolean first = true;    
         
     while(values.hasNext()){
           
            
            String next = values.next().toString();

            if(!next.equalsIgnoreCase("@")){
            	first = false;
              pagerank += next+"\t";
            }

        }
         
        output.collect(key, new Text(pagerank)); 
       
    
        
        
    }
}