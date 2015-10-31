import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class RankCalculationReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    private static final float damping = 0.85F;
    private String bucketName;
    
    public void configure(JobConf job){
    	
    	bucketName = job.get("bucketName");
    }
    
    
    
    
    
    
    public void reduce(Text page, Iterator<Text> values, OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
        boolean isExistingWikiPage = false;
        String[] split;
        float sumShareOtherPageRanks = 0;
        String links = "";
        String pageWithRank;
        
        
      //new coode to open the file
        
	 	Configuration con1 = new Configuration();
		BufferedReader br = null;
		FileSystem fs = null;
		int numPages = 0;
		//String a = bucketName;
		Path path = new Path(bucketName + "/temp/PageRank.n.out/part-00000");
		//Path path = new Path("/home/vishalreddy07/Desktop/wiki/results/PageRank.n.out");
		try {
			fs = path.getFileSystem(con1);
			br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line = br.readLine();
			if (line != null && !line.isEmpty()) {                  
			
                       System.out.println(line);
                      numPages = Integer.parseInt(line.split("\t")[1]);
//
                      System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+numPages);
                      // line=br.readLine();
            br.close();            
			}
               

			   }catch(Exception e){
				   System.out.println("Am in cat");
				   e.printStackTrace();
                }
        
        
        // For each otherPage: 
        // - check control characters
        // - calculate pageRank share <rank> / count(<links>)
        // - add the share to sumShareOtherPageRanks

        while(values.hasNext()){
            pageWithRank = values.next().toString();
            
            if(pageWithRank.equals("!")) {
                isExistingWikiPage = true;
                continue;
            }
            
            if(pageWithRank.startsWith("|")){
                links = "\t"+pageWithRank.substring(1);
                continue;
            }

            split = pageWithRank.split("\\t");
            
            float pageRank = Float.valueOf(split[1]);
            int countOutLinks = Integer.valueOf(split[2]);
            
            sumShareOtherPageRanks += (pageRank/countOutLinks);
        }

        if(!isExistingWikiPage) return;
//        float newRank = damping * sumShareOtherPageRanks + (1-damping)/Global.NCount;
        float newRank = damping * sumShareOtherPageRanks + (1-damping)/numPages;

        out.collect(page, new Text(newRank + links));
    }
}