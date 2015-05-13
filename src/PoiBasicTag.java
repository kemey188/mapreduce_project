package com.bd.upp.dataprocess.upp;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.bd.upp.associatedtopic.tools.HadoopUtil;
import com.bd.upp.util.HdfsFileUtils;
import com.bd.upp.util.IOUtils;

class PoiTagProcessMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valueText = new Text();

	private int tag_index = -1;

	// The correlation between categories
	public HashMap<String, String> cateMap = new HashMap<String, String>();

	@Override
	public void configure(JobConf job) {
		//
		tag_index = job.getInt("join.tag.index", -1);

		// process cache file
		Path[] paths = null;
		try {
			paths = DistributedCache.getLocalCacheFiles(job);
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		String content = null;
		IOUtils io = new IOUtils();
		for (Path path : paths) {
			try {
				content = io.reader(path.toString());
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}

			if (content == null) {
				continue;
			}

			String[] conts = content.split("\n", -1);
			// System.out.println("content length is :"+conts.length);
			for (int j = 0; j < conts.length; j++) {
				String[] contss = conts[j].split("\t", -1);
				// System.out.println(contss[0]);
				if (contss.length < 2) {
					continue;
				}

				if (conts[j].contains("其他")) {
					continue;
				}
				cateMap.put(contss[1], contss[0]);

			}// for
		}// path for
	}

	public String GetCode(String[] taxnos) {
		String valueOut = "";
		HashSet<String> idSet = new HashSet<String>();
		// tag 处理程序 tag simple 宾馆:5 度假村:5 美食:10 自助餐:10 休闲娱乐:5 餐馆:10
		for (int i = 0; i < taxnos.length; i++) {// 对每一个tag获取编码
			String[] tags = taxnos[i].split(":", -1);
			if (tags.length != 2) {// 不符合 宾馆:5 这样的格式
				continue;
			}

			String word = tags[0].trim();
			Set<String> key = cateMap.keySet();
			for (Iterator<String> it = key.iterator(); it.hasNext();) {
				String s = (String) it.next();
				// System.out.println(cateMap.get(s));
				if (cateMap.get(s).equalsIgnoreCase(word)) {
					if (!idSet.contains(s)) {
						idSet.add(s);
					}
				}

			}
		}
		// 按逗号输出tag编码，不区分先后顺序
		if (idSet.size() == 0) {
			valueOut = "null";
		} else {
			Iterator<String> iterator = idSet.iterator();
			while (iterator.hasNext()) {
				valueOut += ";" + iterator.next();
			}
			valueOut = valueOut.substring(1);
			System.out.print(valueOut);
		}
		return valueOut;
	}

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		String[] values = value.toString().split("\t", -1);
		if (values.length >= 1) {
			// input values
			// uid name address areid x y tag date
			if (tag_index >= values.length) {
				reporter.incrCounter("UserGroup", "TagIndexError", 1);
			}
			String tag = values[tag_index];
			String taxno = null;// 取出数据中tag字段
			if (tag.indexOf("\"") < 0) {
				taxno = tag;
			} else {
				taxno = tag.substring(tag.indexOf("\"") + 1,
						tag.lastIndexOf("\""));
			}

			String[] taxnos = taxno.split(" ", -1);
			String codes = GetCode(taxnos);
			String uid = values[0];
			StringBuilder b_str = new StringBuilder();
			for (int i = 1; i < values.length; i++) {
				b_str.append(values[i] + "\t");
			}
			if (b_str.length() > 0) {
				b_str.delete(b_str.length() - 1, b_str.length());
			}
			keyText.set(uid);
			valueText.set(b_str.toString() + "\t" + codes);
			output.collect(keyText, valueText);
			reporter.incrCounter("UserGroup", "UserfulRows", 1);
		} else {
			reporter.incrCounter("UserGroup", "ErrorRecords", 1);
		}// map function
	}

}

public class PoiBasicTag {
	// para check
	private void auxCheckParamter(JobConf job_conf, String conf_key,
			boolean is_must) {
		if (job_conf.get(conf_key, "").isEmpty()) {
			System.err.println(conf_key + " not set.");
			if (is_must == true) {
				System.exit(1);
			}
		}
		System.out.println(conf_key + ":" + job_conf.get(conf_key, ""));
	}

	public int TagProcess(String[] args) throws IOException {
		JobConf conf = new JobConf(PoiBasicTag.class);
		args = HadoopUtil.cmdJobConf(conf, args);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(PoiTagProcessMapper.class);
		conf.setReducerClass(Reducer.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		auxCheckParamter(conf, "ares.user", true);
		auxCheckParamter(conf, "mapred.job.name", true);
		auxCheckParamter(conf, "mapred.input.dir", true);
		auxCheckParamter(conf, "mapred.output.dir", true);
		auxCheckParamter(conf, "mapred.reduce.tasks", true);

		// auxCheckParamter(conf, "mapred.cache.files",true);

		auxCheckParamter(conf, "mapred.job.reuse.jvm.num.tasks", false);
		auxCheckParamter(conf, "mapred.child.java.opts", false);

		HdfsFileUtils.delMROutputPath(conf);
		int n = JobClient.runJobReturnExitCode(conf);

		if (n == 0) {
			// write done file
			HdfsFileUtils.createDoneFile(conf);
		}
		return n;
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		PoiBasicTag poi = new PoiBasicTag();
		int ret = poi.TagProcess(args);
		if (ret != 0) {
			System.out.println("error");
			System.exit(-1);
		} else if (ret == 0) {
			System.out.println("succeed!");
			System.exit(0);
		}
	}

}
