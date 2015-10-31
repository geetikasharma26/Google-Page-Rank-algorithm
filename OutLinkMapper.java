import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class OutLinkMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
{
	    
	 public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
  	String[] s = value.toString().split("\t");
		Text page = new Text(s[0]);
		String list = s[1];
	
	
		String[] links = list.split("~");
	     if(links.length == 1|| true)
	     {
	    	 output.collect(page,new Text("@"));
	     }
//		
		for(String l : links){
			if(!l.equalsIgnoreCase("@"))
			{
				output.collect(new Text(l),page);
			}
		}
		
	    }
		}
	