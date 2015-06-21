package com.hadoop.school.bigdata.set;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapred.*;

import com.hadoop.school.bigdata.set.FreebaseServerTime.MapClass;

public class FreeBaseServerMain {

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		JobConf jobConf = new JobConf(conf, FreebaseServerTime.class);
		jobConf.setJobName("FreeBaseServer");

		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(LongWritable.class);

		JobConf jobConf1 = new JobConf(false);

		ChainMapper.addMapper(jobConf, MapClass.class, LongWritable.class,
				Text.class, Text.class, LongWritable.class, true, jobConf1);

		jobConf.setMapperClass(ChainMapper.class);

		jobConf.setCombinerClass(LongSumReducer.class);
		jobConf.setReducerClass(LongSumReducer.class);
		Job job = new Job(jobConf);
		FileInputFormat.setInputPaths(jobConf, args[0]);

		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
		JobClient.runJob(jobConf);

	}

}
