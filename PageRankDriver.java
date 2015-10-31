import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.io.File;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.InputFormat;


import org.apache.hadoop.mapred.TextOutputFormat;



public class PageRankDriver {

	 public static void main(String[] args) throws Exception {
		 PageRankDriver pageRanking = new PageRankDriver();
		 NumberFormat iter = new DecimalFormat("00");
	     
	        //In and Out dirs in HDFS
	        pageRanking.runXmlParsing(args[0], args[1]+"/temp/job1");
	        pageRanking.outlinkGraph(args[1]+"/temp/job1" , args[1]+"/temp/PageRank.outlink.out", args[1]);
		    pageRanking.countN(args[1]+"/temp/job1/part-00000", args[1] + "/temp/PageRank.n.out", args[1]);
		    pageRanking.tempInputIter(args[1]+"/temp/job1/part-00000", args[1]+ "/temp/iter00File", args[1]);
		    

	        int runs;
	        for(runs = 0; runs < 8; runs++)
	        {
	        	pageRanking.runRankCalculation(args[1] + "/temp/iter" + iter.format(runs) + "File/part-00000", args[1] + "/temp/iter" + iter.format(runs + 1) + "File", runs + 1, args[1]);
	        	
	        }
	        pageRanking.runRankOrdering(args[1] + "/temp/iter01File/part-00000", args[1] + "/temp/PageRank.iter1.out", "1", args[1]);
	        pageRanking.runRankOrdering(args[1] + "/temp/iter08File/part-00000", args[1] + "/temp/PageRank.iter8.out", "8", args[1]);
	           
	    }
	 
	    public void runXmlParsing(String inputPath, String outputPath) throws IOException {
	        JobConf myConfig = new JobConf(PageRankDriver.class);
	 
	        FileInputFormat.setInputPaths(myConfig, new Path(inputPath));
	        // Mahout class to Parse XML + config
	       // myConfig.setInputFormat((Class<? extends InputFormat>) XmlInputFormat.class);
	        myConfig.setInputFormat(XmlInputFormat.class);
	        myConfig.set(XmlInputFormat.START_TAG_KEY, "<page>");
	        myConfig.set(XmlInputFormat.END_TAG_KEY, "</page>");
	        // Our class to parse links from content.
	        myConfig.setMapperClass(MyParserMapper.class);
	 
	        FileOutputFormat.setOutputPath(myConfig, new Path(outputPath));
	        myConfig.setOutputFormat(TextOutputFormat.class);
	        myConfig.setOutputKeyClass(Text.class);
	        myConfig.setOutputValueClass(Text.class);
	        // Our class to create initial output
	        myConfig.setReducerClass(MyParserReducer.class);
	        myConfig.setNumReduceTasks(1);
	 
	        JobClient.runJob(myConfig);
	    }
	    
	    public void outlinkGraph(String inputPath, String outputPath, String bucketName) throws IOException {
	        JobConf myConfig = new JobConf(PageRankDriver.class);
	 
	        FileInputFormat.setInputPaths(myConfig, new Path(inputPath));
	        myConfig.setInputFormat(TextInputFormat.class);
	        // Our class to parse links from content.
	        myConfig.setMapperClass(OutLinkMapper.class);
	 
	        FileOutputFormat.setOutputPath(myConfig, new Path(outputPath));
	        myConfig.setOutputFormat(TextOutputFormat.class);
	        myConfig.setOutputKeyClass(Text.class);
	        myConfig.setOutputValueClass(Text.class);
	        // Our class to create initial output
	        myConfig.setReducerClass(OutLinkReducer.class);
	        myConfig.setNumReduceTasks(1);
	        myConfig.set("bucketName", bucketName);
	 
	        JobClient.runJob(myConfig);
	        
	        FileSystem fs = null;
			// for outlinks
			Path src;
			Path dst;
			String bName = bucketName;
			try {
				src = new Path(bName + "/temp/PageRank.outlink.out");
				dst = new Path(bName + "/results/PageRank.outlink.out");
				fs = src.getFileSystem(myConfig);
				FileUtil.copyMerge(fs, src, fs, dst, false, myConfig, null);
			} catch (IOException e) {
				e.printStackTrace();
			}
	    }
	    
	    private void countN(String inputPath, String outputPath, String bucketName) throws IOException
	    {
	    	JobConf conf = new JobConf(PageRankDriver.class);
	        conf.setJobName("N Count");
	        conf.setOutputKeyClass(Text.class);
	        conf.setOutputValueClass(IntWritable.class);
	        
	        conf.setInputFormat(TextInputFormat.class);
	        conf.setOutputFormat(TextOutputFormat.class);
	        
	        
	        FileInputFormat.setInputPaths(conf, new Path(inputPath));
	        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
	        //FileOutputFormat.setOutputPath(conf, outPath);
	        conf.setMapperClass(CountMapper.class);
	        conf.setReducerClass(CountReducer.class);
	        conf.setNumReduceTasks(1);
	        conf.set("bucketName", bucketName);
	        JobClient.runJob(conf);

	        FileSystem fs = null;
			// for outlinks
			Path src;
			Path dst;
			String bName = bucketName;
			try {
				src = new Path(bName + "/temp/PageRank.n.out");
				dst = new Path(bName + "/results/PageRank.n.out");
				fs = src.getFileSystem(conf);
				FileUtil.copyMerge(fs, src, fs, dst, false, conf, null);
			} catch (IOException e) {
				e.printStackTrace();
			}
	        

	    }
	    
	 
	 
	 private void tempInputIter(String inputPath, String outputPath, String bucketName) throws IOException
	    {
	    	JobConf conf = new JobConf(PageRankDriver.class);
	        conf.setJobName("Input Iter");
	        conf.setOutputKeyClass(Text.class);
	        conf.setOutputValueClass(Text.class);
	        
	        conf.setInputFormat(TextInputFormat.class);
	        conf.setOutputFormat(TextOutputFormat.class);
	        
	        
	        FileInputFormat.setInputPaths(conf, new Path(inputPath));
	        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
	        //FileOutputFormat.setOutputPath(conf, outPath);
	        conf.setMapperClass(TempInputIterMapper.class);
	        conf.setReducerClass(TempInputIterReducer.class);
	        conf.setNumReduceTasks(1);
	        conf.set("bucketName", bucketName);
	        JobClient.runJob(conf);
	        


	    }
	    
	 private void runRankCalculation(String inputPath, String outputPath, int runs, String bucketName) throws IOException {
	        JobConf conf = new JobConf(PageRankDriver.class);

	        conf.setOutputKeyClass(Text.class);
	        conf.setOutputValueClass(Text.class);
	        
	        conf.setInputFormat(TextInputFormat.class);
	        conf.setOutputFormat(TextOutputFormat.class);
	        
	        FileInputFormat.setInputPaths(conf, new Path(inputPath));
	        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
	        
	        conf.setMapperClass(RankCalculationMapper.class);
	        conf.setReducerClass(RankCalculationReducer.class);
	        conf.setNumReduceTasks(1);
	        conf.set("bucketName", bucketName);
	        JobClient.runJob(conf);
	       
	    }
	
	 private void runRankOrdering(String inputPath, String outputPath, String num, String bucket) throws IOException {
	       
	    	JobConf conf = new JobConf(PageRankDriver.class);
	    	 
	        conf.setOutputKeyClass(DoubleWritable.class);
	        conf.setOutputValueClass(Text.class);
	        conf.setInputFormat(TextInputFormat.class);
	        conf.setOutputFormat(TextOutputFormat.class);
	 
	        FileInputFormat.setInputPaths(conf, new Path(inputPath));
	        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
	 
	        conf.setMapperClass(FinalRankingMapper.class);
	        conf.setReducerClass(FinalRankingReducer.class);
	 	   //use job if conf gives error
			conf.setOutputKeyComparatorClass(DoubleComparator.class);
			conf.setNumReduceTasks(1);
			conf.set("bucketName", bucket);
			JobClient.runJob(conf);
	        
	        FileSystem fs = null;
			// for outlinks
			Path src;
			Path dst;
			String bucketName = bucket;
			try {
				src = new Path(outputPath);
				dst = new Path(bucketName + "/results/PageRank.iter" + num + ".out");
				fs = src.getFileSystem(conf);
				FileUtil.copyMerge(fs, src, fs, dst, false, conf, null);
			} catch (IOException e) {
				e.printStackTrace();
			}
	    }
	 
	      public static class DoubleComparator extends DoubleWritable.Comparator { 
	    	@Override 
	    	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) { 
	    		return -1 * super.compare(b1, s1, l1, b2, s2, l2); 
	    		}
	    }
	 
} 