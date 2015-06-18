package com.bd.da.map.profile;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

//import com.ba.da.map.profile.util.DateUtil;

public class Profile_frequence {
  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
  {
    private Text word = new Text();
    private Text keyWord = new Text();
   	private int confStatus = -1;

   	
   	public void configure(JobConf job) {
		confStatus = -1;		
		try {
			String file = job.get("map.input.file").toLowerCase();
			if(!FilterUtils.checkPath(file, "20140901~20141131", "")) {
				confStatus = -1;
				return;
			}
		
							
		} catch(Exception e) {
			this.confStatus = -3;
			return;
		}
		
		this.confStatus = 0;
	}
    
    
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException
    {
      if (confStatus != 0) {
			return;
	  }	
    	
      String[] words = value.toString().split("\t",-1);
      String[] tags_info = words[3].split(" ",-1);
      for(String tag_info : tags_info)
      {
    	  String tag = tag_info.split(":",2)[0];
    	  keyWord.set(tag);
    	  word.set(tag);
    	  reporter.incrCounter("MAP", "INFO_CATA_num",1);
		  output.collect(this.keyWord, this.word);
      }
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>
{
	  double e = 0d;
	  double baseNum =0d;
  public void configure(JobConf job) {
	
		//configure the taxonomy dictionary
		 e = Double.parseDouble(job.get("number.e","2.7182818"));    
		 baseNum =Double.parseDouble(job.get("number.baseNum","24571744")); 
		 
  }
				
  public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
    throws IOException
  {
    int count = 0 ;
	while (values.hasNext())
    {
      values.next();
      count++;
    }	
	double result = Math.log((baseNum/count)+(e-1));
    output.collect(key, new Text(""+result));
    
  }
}
  
  
  public static void main(String[] args)
    throws Exception
  {
    JobConf conf = new JobConf(args[0]);
	conf.setJarByClass(Profile_frequence.class);
    conf.setJobName("Profile_frequence");
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.setMapperClass(Map.class);
    conf.setReducerClass(Reduce.class);
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    conf.set("mapred.job.priority", "VERY_HIGH");
    conf.setNumReduceTasks(500);
    
    String input_path = conf.get("Hadoop.inputPath");
	String output_path = conf.get("Hadoop.outputPath");
	Path outPath = new Path(output_path);
	FileSystem fs = outPath.getFileSystem(conf);
	if(fs.exists(outPath)){
		System.out.println("The path of " + output_path + " exists and removes...");
		fs.delete(outPath, true);
	}
	FileInputFormat.setInputPaths(conf, input_path);
    FileOutputFormat.setOutputPath(conf, new Path(output_path));
    
    int n = JobClient.runJobReturnExitCode(conf);
    System.out.println(n);
    if (n < 0) {
      System.out.println("error");
    } else if (n > 0) {
      System.out.println("succeed!");
    } else {
      System.out.println("wait");
    }
  }
}







  
