package com.bd.lbs.di;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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

public class UserYearTradePreferRatio {

	public static class MyMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		
		String filePath=null;
//		Map<String, Integer> spTag = new HashMap<String, Integer>();
//		Map<String, Integer> otherTag = new HashMap<String, Integer>();
		Map<String, String> tagMap = new HashMap<String, String>();
		String[] outTags = {"金融","交通设施","购物","美食","景点", "休闲娱乐","酒店","其他"};
        public void configure(JobConf job) { 
        	this.tagMap.put("金融", "金融");
        	this.tagMap.put("交通设施", "交通设施");
        	this.tagMap.put("购物", "购物");
        	this.tagMap.put("美食", "美食");
        	this.tagMap.put("旅游景点", "景点");
        	this.tagMap.put("休闲娱乐", "休闲娱乐");
        	this.tagMap.put("宾馆", "酒店");
        	this.tagMap.put("生活服务", "其他");
        	this.tagMap.put("医疗", "其他");
        	this.tagMap.put("汽车服务", "其他");
        	this.tagMap.put("丽人", "其他");
        	this.tagMap.put("结婚", "其他");
        	this.tagMap.put("运动健身", "其他");
        	
        	try {
        		filePath = job.get("map.input.file").toLowerCase();
    										
    		} catch(Exception e) {
    			return;
    		}
        }  
		
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {	  
                // String filePath = ((FileSplit)reporter.getInputSplit()).getPath().toString();   
	            String line = value.toString();  
	             
	            // input[0]文件输入  price average数据
	            if (filePath.contains("lukaimin/meishi_hotel/")) {  
	                //String src[] = line.split("\t");// input文件以“\t”切分  
	                String src[] = line.split(" ");// input文件以“ ”切分  
	                String cuid = src[0];
	                String tag1 = null;
	                String msavg = null;
	                String tag2 = null;
	                String bgavg = null;
					try {
		                  tag1 = src[1];
		                  msavg = src[2];
		                  tag2 = src[3];
		                  bgavg = src[4];
					} catch (ArrayIndexOutOfBoundsException e) {
						 e.printStackTrace();
						 reporter.incrCounter("MAP", "avgnull",1);
					}	                 
		            output.collect( new Text(cuid), new Text("src#"+"2014"));  
	            }  
	            // input[1]文件输入: tag偏好数据 
	            else if (filePath.contains("category/20141231/")) {  
	                String rst[] = line.split("\t"); // input文件列切分	                  
	              //System.out.println("QQQQQQQQQQQQQQQQ"+rst);
	                String cuid = rst[0];
	                try{
	                	String tag = null;
	                	String msscore = null;
	                	String score = null;
	                	float rscore = (float) 0.00;
	                	float sum = 0;

	                	Map<String, Float> hashmap = new HashMap<String, Float>();
	                    if ((rst.length-1)%2==0) {
	    	                for(int i =1; i < rst.length-2; i=i+2){    	                		    	                

	    	                	tag = rst[i];
		    	                msscore = rst[i+1];	    	                		
	    	                	
	    	                	score = msscore.split(":")[1];  //2:0.0000219 
	    	                	rscore = Float.parseFloat(score); 
	    	                	if(this.tagMap.containsKey(tag)) {
	    	                		String k = this.tagMap.get(tag);
	    	                		if(!hashmap.containsKey(k))
	    	                			hashmap.put(k, (float) 0.0);
	    	                		sum += rscore;
	    	                		hashmap.put(k, hashmap.get(k) + rscore);
	    	                	}
	    	                }
	    	            }
	                    if(hashmap.size() ==1 && hashmap.containsKey("其他"))
	                    	return;
//	                    Map<String, Integer> outPercent = new HashMap<String, Integer> ();
	                    String outStr = "";
	                    int otherPercent = 100;
	                    for(String outTag : this.outTags){
	                    	float tagScore = hashmap.containsKey(outTag) ? hashmap.get(outTag) : 0;
	                    	int tagPercent = (int) (tagScore/sum * 100);
	                    	if(!"其他".equals(outTag)) {
	                    		outStr += tagPercent + "%\t";
	                    		otherPercent -= tagPercent;
	                    	}
//	                    		outPercent.put(outTag, (int) (hashmap.get(outTag)/sum * 100));
	                    }
	                    outStr += otherPercent + "%";
	                    	                    	                    
                		output.collect(new Text(cuid), new Text(outStr));
	                	
	                }catch (Exception e) {
	                	   e.printStackTrace();
	                	   reporter.incrCounter("MAP", "tagnull",1);
	                  }                	                  
	                
	            } 
												  
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			
                output.collect(key, new Text("\t" + values));
                
		}
	}

	public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf(UserYearTradePreferRatio.class);
		conf.setJobName("UserYearTradePreferRatio");
		conf.set("mapred.job.priority", "VERY_HIGH");
		conf.setNumReduceTasks(0);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(MyMap.class);
		//conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileInputFormat.addInputPath(conf, new Path(args[1]));
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));
		

		// JobClient.runJob(conf);
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
