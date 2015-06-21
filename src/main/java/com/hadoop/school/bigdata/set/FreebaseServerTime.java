package com.hadoop.school.bigdata.set;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class FreebaseServerTime {
	public static class MapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, LongWritable> {
		private final LongWritable one = new LongWritable(1);

		public void map(LongWritable key, Text value,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] parts = line.split(",");
			String creator = parts[1].trim();
			if (creator.length() >= 2) {
				output.collect(new Text(creator), one);
			}

		}

	}
}
