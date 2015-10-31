import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.*;

public class OutLinkReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

    	String page = "";
        boolean first = true;    
         
     while(values.hasNext()){
           
            
            String next = values.next().toString();
            

            if(!next.equalsIgnoreCase("@")){
            	first = false;
              page += next+"\t";
            }

        }
         
        output.collect(key, new Text(page)); 
   
        System.out.println();
        
        
    }
}