package com.ikamobile.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{

	Logger log = LoggerFactory.getLogger(MyMapper.class);
	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {

		log.info("myMapper .... .. ... . . ... . {}",value);

		String[]strs = value.toString().split("\t");
		if(strs!=null && strs.length>1){
			output.collect(new Text(strs[0]),new IntWritable(1));
		} else {
			output.collect(new Text("null"+value.toString()),new IntWritable(1));
		}


		
	}

}
