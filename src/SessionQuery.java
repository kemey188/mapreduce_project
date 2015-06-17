package com.bd.da.map.profile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.hadoop.filecache.DistributedCache;
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

import com.bd.da.map.profile.ClickRate.Reduce.Item;
import com.bd.da.map.profile.util.IOUtils;
import com.bd.da.map.profile.util.FilterUtils;
import com.bd.da.map.profile.util.TimeUtil;

public class SessionQuery {
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		ArrayList<String> cuidList = new ArrayList<String>();
		String file = null;
		boolean fg1 = false;
		boolean fg2 = false;
		boolean fg3 = false;
		
		int user_num =-1;
		int before_days = -1;
		int after_days = -1;
		
		public void configure(JobConf job) {
			
			user_num = Integer.parseInt(job.get("user.num"));
			before_days = Integer.parseInt(job.get("day.before"));
			after_days = Integer.parseInt(job.get("day.after"));
			
			Path[] paths = null;
			try {
				paths = DistributedCache.getLocalCacheFiles(job);
			} catch (IOException e) {
				return;
			}
			String content = null;
			try {
				content = IOUtils.reader(paths[0].toString());
			} catch (Exception e) {
				e.printStackTrace();
			}
			if ((content != null) && (!content.trim().equals(""))) {
				String[] lines = content.split("\n");
				for (int index = 0; index < lines.length; index++) {
					String[] strs = lines[index].split("\t");
					cuidList.add(strs[0]);
				}
			}
			try {
				file = job.get("map.input.file").toLowerCase();

				String date = job.get("date");
				String date90 = TimeUtil.getPreNDate(date, this.before_days/*90*/);
				// String date1 = TimeUtil.getPreNDate(date,1);
				String datePre1 = TimeUtil.getPreNDate(date, -1);
				String datePre30 = TimeUtil.getPreNDate(date, -this.after_days/*-30*/);
				if (FilterUtils.checkPath(file, date90 + "~" + date,
						"event_day") && !file.contains("dic")) {
					fg1 = true;
					System.out.println("fg1");
				}
				if (FilterUtils.checkPath(file, datePre1 + "~" + datePre30,
						"event_day")) {
					fg2 = true;
					System.out.println("fg2");
				}
				if (file.contains(date) && file.contains("dic")) {
					fg3 = true;
					System.out.println("fg3");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		static class Item{
			 String word;
			 double score;
		 }
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String[] str = value.toString().split("\t");
			boolean fg = false;
			// for(String temp : cuidList)
			// {
			// if(str[0].equals(temp) )
			// {
			// fg=true;
			// reporter.incrCounter("MAP", "um", 1);
			//
			// }
			//
			// }
			if(user_num == -1){
				reporter.incrCounter("MAP", "user_num=-1", 1);
			}
			for (int i = 0; i < this.user_num; i++) {//num
				if (str[0].equals(cuidList.get(i))) {
					fg = true;
					reporter.incrCounter("MAP", "um", 1);

				}
			}
			if (fg) {
				if (fg1) {
					output.collect(value, new Text("fg1"));
					reporter.incrCounter("MAP", "fg1", 1);
				}
				if (fg2) {
					output.collect(value, new Text("fg2"));
					reporter.incrCounter("MAP", "fg2", 1);
				}
				if (fg3) {
					ArrayList<Item> words = new ArrayList<Item>();
					String [] values =value.toString().split("\t");
					String cuid = values[0].trim();
		    		String tags ="";
		    		if((values.length-1)%2!=0)
		    		{
		    			reporter.incrCounter("MAP", "upp_format_err", 1);
		    			return;
		    		}
		    		for(int i=1;i<values.length;i=i+2)
		    		{
		    			tags += values[i]+"_"+values[i+1].split(":")[1]+"\t";
		    		}
					for (String str1 : tags.split("\t")) {
						if(str1.length()>0){
							Item it = new Item();
							if(str1.split("_").length!=2){
								continue;
							}
							
							it.word = str1.split("_")[0];
							it.score = Double.parseDouble(str1.split("_")[1]);
							words.add(it);						
						}
					}
					// sort the words
					Collections.sort(words, new Comparator<Item>() {
						@Override
						public int compare(Item it0, Item it1) {
							// TODO Auto-generated method stub
							if (it0.score > it1.score) {
								return -1;
							} else if (it0.score < it1.score) {
								return 1;
							}
							return 0;
						}
					});
					StringBuffer sb = new StringBuffer();
					sb.append(cuid+"\t");
					for (Item it : words) {
						sb.append(it.word).append("\t").append("1:").append(it.score).append("\t");
					}
					
					
					output.collect(new Text(sb.toString().substring(0, sb.toString().length()-1)), new Text("fg3"));
					reporter.incrCounter("MAP", "fg3", 1);
				}
			}

		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String result = "";
			while (values.hasNext()) {
				String item = ((Text) values.next()).toString();
				result += item + "\t";
			}
			output.collect(key, new Text(result.trim()));
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(args[0]);
		conf.setJarByClass(SessionQuery.class);
		conf.setJobName("SessionQuery");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		// conf.setOutputFormat(DicMTOF.class);
		conf.set("mapred.job.priority", "VERY_HIGH");
		String dic = conf.get("cuid_dic");
		DistributedCache.addCacheFile(new Path(dic).toUri(), conf);

		String input_path = conf.get("Hadoop.inputPath1");
		String output_path = conf.get("Hadoop.outputPath1");
		Path outPath = new Path(output_path);
		FileSystem fs = outPath.getFileSystem(conf);
		if (fs.exists(outPath)) {
			System.out.println("The path of " + output_path
					+ " exists and removes...");
			fs.delete(outPath, true);
		}
		System.out.println("Input Path:\t" + input_path);
		System.out.println("Output Path:\t" + output_path);
		FileInputFormat.setInputPaths(conf, input_path);
		FileOutputFormat.setOutputPath(conf, new Path(output_path));

		int n = JobClient.runJobReturnExitCode(conf);
		System.out.println(n);
		if (n < 0) {
			System.exit(1);
			;
		}
	}
}
