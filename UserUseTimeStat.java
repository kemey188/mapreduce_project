package com.baidu.lbs.di;

import java.io.IOException;
import java.util.Iterator;

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

public class UserUseTimeStat {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String rows[] = line.split("\t");
			String cuid = null;
			String n90 = null;
			String gap = null;
			try {
				  cuid = rows[0];
				  n90 = rows[3];
				  gap = rows[8];
			} catch (Exception e) {
				reporter.incrCounter("MAP", "Exception_err", 1);
			}
			String isN90_1 = (n90.equals("1")) ? "==1" : ">1";
			output.collect(new Text("ALL\t" + gap), new Text(cuid));
			output.collect(new Text(isN90_1 + "\t" + gap), new Text(cuid));
			reporter.incrCounter("info", "output_n" + isN90_1 +"\t"+ gap, 1);
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			String result = null;
			while (values.hasNext()) {
				values.next();
				sum++;
			}
			result = "" + sum;
			output.collect(key, new Text(result));
		}
	}

	public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf(UserUseTimeStat.class);
		conf.setJobName("UserUseTimeStat");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

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
