package com.bd.lbs.di;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.Vector;

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

public class UserPoiTagPreferOrder {

	public static class MyMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		
		String filePath=null;
		Map<String, Integer> msTag = new HashMap<String, Integer>();
		
        public void configure(JobConf job) { 
        	this.msTag.put("奶茶",1);
        	this.msTag.put("蛋糕西点",1);
        	this.msTag.put("酒吧",1);
        	this.msTag.put("茶座",1);
        	this.msTag.put("小吃",1);
        	this.msTag.put("咖啡厅",1);
        	this.msTag.put("烧烤",1);
        	this.msTag.put("甜点饮品",1);
        	this.msTag.put("冰淇淋",1);
        	this.msTag.put("创意菜",1);
        	this.msTag.put("东南亚菜",1);
        	this.msTag.put("韩国菜",1);
        	this.msTag.put("快餐",1);
        	this.msTag.put("农家菜",1);
        	this.msTag.put("日本菜",1);
        	this.msTag.put("西餐厅",1);
        	this.msTag.put("中餐厅",1);
        	this.msTag.put("自助餐",1);
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
		            output.collect( new Text(cuid), new Text("src#"+"\t"+tag1+"\t"+msavg+"\t"+tag2+"\t"+bgavg));  
	            }  
	            // input[1]文件输入: tag偏好数据 
	            else if (filePath.contains("category/20150118/")) {  
	                String rst[] = line.split("\t"); // input文件列切分	                  
	              //System.out.println("QQQQQQQQQQQQQQQQ"+rst);
	                String cuid = rst[0];
	                try{
	                	String mstag = null;
	                	String msscore = null;
	                	String score = null;
	                	float rscore = (float) 0.00;
	                	TreeMap<Float, String> treemap = new TreeMap<Float, String>();
	                    if ((rst.length-1)%2==0) {
	    	                for(int i =1; i < rst.length-2; i=i+2){    	                	
	    	                	mstag = rst[i];
	    	                	msscore = rst[i+1];
	    	                	
	    	                	score = msscore.split(":")[1];  //2:0.0000219 
	    	                	rscore = Float.parseFloat(score); 
	    	                	if(this.msTag.containsKey(mstag)) {
	    	                		treemap.put(rscore, mstag);
	    	                    }
	    	                }
	    	            }
	                    NavigableMap<Float, String> tagmap=treemap.descendingMap();
	                    	                    
	                    System.out.println("tag desc: "+tagmap);	
//	                	if(this.msTag.containsKey(mstag)) {
//	                		TreeMap<Float, String> treemap = new TreeMap<Float, String>();
//	                		treemap.put(rscore, mstag);
//	                		NavigableMap<Float, String> tagmap=treemap.descendingMap();
//
//	                		System.out.println("tag desc: "+tagmap);	                   
//	                    }
                		output.collect(new Text(cuid), new Text("rst#"+"\t"+tagmap));
	                	
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
            Vector<String> vecA = new Vector<String>(); // 存放来自src的值  
            Vector<String> vecB = new Vector<String>(); // 存放来自rst的值  
              
            while (values.hasNext()) {  
                String value = values.next().toString();  
                if (value.startsWith("src#")) {  
                    vecA.add(value.substring(4));  
                } else if (value.startsWith("rst#")) {  
                    vecB.add(value.substring(4));  
                }  
            }  
              
            int sizeA = vecA.size();  
            int sizeB = vecB.size();  
              
            // 遍历两个向量  
            int i, j;  
            for (i = 0; i < sizeA; i ++) {  
                for (j = 0; j < sizeB; j ++) {  
                    output.collect(key, new Text(vecA.get(i) + "\t" +vecB.get(j)));
                }
			                                                      			
		    }
		}
	}

	public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf(UserPoiTagPreferOrder.class);
		conf.setJobName("UserPoiTagPreferOrder");
		conf.set("mapred.job.priority", "VERY_HIGH");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(MyMap.class);
		conf.setReducerClass(Reduce.class);

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
