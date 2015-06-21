package com.hadoop.school;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCount {
	public static class WordCountMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] words = value.toString().split(" ");
			for (String str : words) {
				word.set(str);
				context.write(word, one);
			}
		}

	}

	public static class WordCountReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int total = 0;
			for (IntWritable val : values) {
				total++;
			}
			context.write(key, new IntWritable(total));
		}

	}
}
