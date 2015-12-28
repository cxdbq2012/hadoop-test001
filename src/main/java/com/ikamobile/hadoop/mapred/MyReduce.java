package com.ikamobile.hadoop.mapred;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyReduce extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable> {
	Logger log = LoggerFactory.getLogger(MyReduce.class);

	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {

		log.info("MyReduce .... .. ... . . ... . {}",key);
		int sum = 0;
		while(values!=null && values.hasNext()){
			sum += values.next().get();
		}

		output.collect(key,new IntWritable(sum));
	}

}
