package com.hadoop.school.bigdata.set;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class FreebaseServerTimeStampMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, LongWritable, Text> {

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<LongWritable, Text> output, Reporter reporter)
			throws IOException {
		String line = value.toString();
		if (validate(line)) {
			output.collect(key, value);
		}

	}

	private boolean validate(String line) {
		String[] parts = line.split(",");
		if (parts.length != 7)
			return false;

		return true;
	}

}
