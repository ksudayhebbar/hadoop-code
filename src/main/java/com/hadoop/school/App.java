package com.hadoop.school;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.hadoop.school.WordCount.WordCountMapper;
import com.hadoop.school.WordCount.WordCountReducer;
import org.apache.hadoop.io.* ;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		// System.out.println( "Hello World!" );
		Configuration conf = new Configuration();
		//JobConf jobConf = new JobConf(conf);
		Job job = new Job(conf,"Word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
