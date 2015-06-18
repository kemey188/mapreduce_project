package  com.bd.da.map.profile;

import java.io.IOException;

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
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;




public class SplitProfile {
	public static class Map
    extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text>
  {
    private Text word = new Text();
    private Text keyWord = new Text();
    
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException
    {
      String[] words = value.toString().split("\t",2);
      keyWord.set(words[0]);
      String profile_value = words[1];
      
      String[] value_list = profile_value.split("\t", -1);  
      
		int value_list_size = value_list.length;
		if ((value_list_size % 2) != 0)     
		{
			reporter.incrCounter("MAP","ERROR_profile_format_err",1);
			return ;
		}
		for (int label = 0; label < value_list_size; label += 2)
		{
			String profile_id = value_list[label];    
			String profile_score = value_list[label + 1];  
			String[] profile_score_pair = profile_score.split(":", -1);
			if (profile_score_pair.length != 2)
			{
				
				reporter.incrCounter("MAP", "ERROR_profile_format_err", 1);
				reporter.incrCounter("MAP", "ERROR_profile_format_err_" + profile_score_pair.length, 1);
				return ;
			}
		
			double score = Double.parseDouble(profile_score_pair[1]);
//			int score_int = (int)(score*1000000);
			String profile_word = profile_id+"\t"+(int)(score*1000000);
			word.set(profile_word);
			reporter.incrCounter("MAP", "INFO_CATA_num",1);
		    output.collect(this.keyWord, this.word);
		}
    }
  }

  
  public static void main(String[] args)
    throws Exception
  {
    JobConf conf = new JobConf(SplitProfile.class);
    conf.setJobName("SplitProfile");
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.setMapperClass(Map.class);
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    conf.set("mapred.job.priority", "VERY_HIGH");
    conf.setNumReduceTasks(0);
    
    
   
	FileInputFormat.setInputPaths(conf, args[0]);
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
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




  
