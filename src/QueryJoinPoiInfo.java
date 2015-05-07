package com.baidu.lbs.ucdt.DataRe;

import java.io.IOException;
import java.util.Iterator;
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

public class QueryJoinPoiInfo {

	public static class MyMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		
		    //private String main_path_feature = null;
		    //private String join_path_feature = null;		
		    String filePath = null;		    
            public void configure(JobConf job) { 
        	try {
        		filePath = job.get("map.input.file").toLowerCase();
        		//main_path_feature = job.get("main.path.feature");
        		//join_path_feature = job.get("join.path.feature");
    										
    		} catch(Exception e) {
    			return;
    		}
        }  
		
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
 
	            String line = value.toString();  
	            // input[0]文件输入udw抽取的query数据
	            // input_path.contains(main_path_feature)
	            if (filePath.contains("action_type=query/")) {  
	                String src[] = line.split("\t"); // input文件以“\t”切分  
	                String event_date = "null";
	                String cuid = "null";
	                String uid = "null";
	                String event_tms = "null";
	                String query_word = "null";
	                String city = "null";
	                String province = "null";
	                String loc_x = "null";
	                String loc_y = "null";
	                String data_type = "null";
	                String sub_type = "null";
					try {
		                  event_date = src[0];  
						  cuid = src[1]; 
		                  uid = src[2];		                  		                  
		                  event_tms = src[3];
		                  query_word = src[4];
		                  city = src[5];
		                  province = src[6];
		                  loc_x = src[7];
		                  loc_y = src[8];
		                  //data_type = src[9];
		                  data_type = src[9].split(",")[0];
		                  sub_type = src[9].split(",")[1];
					} catch (ArrayIndexOutOfBoundsException e) {
						 e.printStackTrace();
						 reporter.incrCounter("MAP", "avgnull",1);
					}	                 
		        
					output.collect(new Text(uid), new Text("src#"+event_date+"\t"+cuid+"\t"+uid+"\t"+event_tms+"\t"+query_word+"\t"+city+"\t"+province+"\t"+loc_x+"\t"+
					                                              loc_y+"\t"+data_type+"\t"+sub_type));  
		            
	            }  
	            // input[1]文件输入: poi_info_new数据 
	            // (input_path.contains(join_path_feature))
	              else if (filePath.contains("/lbs_poi_info_new/")){  
	                String rst[] = line.split("\t"); // input文件列切分	                  
	              //System.out.println("QQQQQQQQQQQQQQQQ"+rst);
                	String uid = null;
                	String name = null;
                	String tag = null;
                	String brand = null;
                	String price = null;
                	String overall_rating = null;	                	
                	String service_rating = null;
                	String effect_rating = null;
                	String enviroment_rating = null;	                	
                	String taste_rating = null;
                	String point_x = null;
                	String point_y = null;	                	
                	String poi_area = null;
                	String poi_city = null;
                	String src_tag = null;
                	
	                try{
	                	uid = rst[0];
	                	name = rst[2];
	                	tag = rst[14];
	                	brand = rst[30];
	                	price = rst[19];
	                	overall_rating = rst[20];
	                	service_rating = rst[21];
	                	effect_rating = rst[22];
	                	enviroment_rating = rst[23];
	                	taste_rating = rst[24];
	                	point_x = rst[9];
	                	point_y = rst[10];
	                	poi_area = null;
	                	poi_city = null;
	                	src_tag = rst[25];
                		                		                		                	
	                }catch (Exception e) {
	                	   e.printStackTrace();
	                	   reporter.incrCounter("MAP", "tagnull",1);
	                  } 	                
            		output.collect(new Text(uid), new Text("rst#"+name+'\t'+tag+'\t'+brand+'\t'+price+'\t'+overall_rating +'\t'+service_rating+'\t'+effect_rating+
                            '\t'+enviroment_rating +'\t'+taste_rating+'\t'+point_x+'\t'+point_y +'\t'+poi_area+'\t'+poi_city+'\t'+src_tag));	                
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
            Vector<String> vecUB = new Vector<String>(); // 存放来自rst的值_unique  
            int count =0;
            while (values.hasNext()) {  
                String value = values.next().toString();  
                if (value.startsWith("src#")) {  
                    vecA.add(value.substring(4));  
                } else if (value.startsWith("rst#")) {  
                    vecB.add(value.substring(4));                      
                }  
                if(count++ >1000){               	
                	break;
                }
                
            }  
                        
            //poi_info_new抽取字段去重
            for (int i=0; i<vecB.size(); i++){ 
            	Object obj = vecB.get(i); 
            	if(!vecUB.contains(obj)){
            	   vecUB.add((String) obj); 
                }
            } 
                       
            // inner join
//            int sizeA = vecA.size();  
//            int sizeB = vecB.size();  
//              
//            // 遍历两个向量  
//            int i, j;  
//            for (i = 0; i < sizeA; i ++) {  
//                for (j = 0; j < sizeB; j ++) {  
//                    output.collect(key, new Text(vecA.get(i) + "\t" +vecB.get(j)));
//                }
//			                                                      			
//		    }
            
            // left outer join            
            for (String A : vecA) {
                if (!vecUB.isEmpty()) {
                    for (String B : vecUB) {
                    	output.collect( new Text(A), new Text(B));
                    	System.out.println("can join:"+"\t" + A + "\t" +B);
                    }
                } else {
                	output.collect(new Text(A), new Text("null"+'\t'+"null"+'\t'+"null"+'\t'+"null"+'\t'+"null"+'\t'+"null"+'\t'+"null"+'\t'+"null"+'\t'+
                			"null"+'\t'+"null"+'\t'+"null"+'\t'+"null"+'\t'+"null"+'\t'+"null"));
                	System.out.println("can't join:"+"\t"+A);
                }
            }
			
		}
	}

	public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf(QueryJoinPoiInfo.class);
		conf.setJobName("QueryJoinPoiInfo");
		conf.set("mapred.job.priority", "VERY_HIGH");
		//conf.set("mapred.child.java.opts","-Xmx8000m");
		//conf.set("mapred.job.queue.name", "lbs-da-upp");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(MyMap.class);
		conf.setReducerClass(Reduce.class);

		conf.setNumReduceTasks(100);
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
