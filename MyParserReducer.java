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

public class MyParserReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

		
    	String pagerank = "";
        boolean first = true;
        boolean isNotRed = false;
    	// Code to remove duplicates from the outlink graph
		Set<String> set = new HashSet<String>();
		String keyStr = key.toString();
        
        while(values.hasNext()){
            if(!first) pagerank += "~";
            
            String next = values.next().toString();
            
            first = false;
         
           
            //check if the value contains hash
            if (next.equalsIgnoreCase("@")){
            	isNotRed = true;
            }
            if(!set.contains(next) && !keyStr.equalsIgnoreCase(next))
            {
            pagerank += next;
            
            set.add(next);
            
            }
        }
         if(isNotRed){
        output.collect(key, new Text(pagerank)); 
        }
     
        System.out.println();

        
        
    }
}