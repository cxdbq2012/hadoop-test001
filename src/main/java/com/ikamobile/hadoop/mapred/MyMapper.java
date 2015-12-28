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
		String v = value.toString();
		if(value.toString().contains("航班查询处理时间："))
		{

			String[]vs = v.split("航班查询处理时间：");
			String time = vs[1].replaceAll("毫秒","");
			int timeInt = Integer.valueOf(time);

			if(timeInt<30){
				output.collect(new Text("less-30"),new IntWritable(timeInt));
			} else if (timeInt >= 30 && timeInt < 60 ) {
				output.collect(new Text("less-60"),new IntWritable(timeInt));
			} else if(timeInt >= 60 && timeInt < 120 ) {
				output.collect(new Text("less-120"),new IntWritable(timeInt));
			} else if(timeInt >= 120 && timeInt < 240 ) {
				output.collect(new Text("less-240"),new IntWritable(timeInt));
			} else if(timeInt >= 240 && timeInt < 480 ) {
				output.collect(new Text("less-480"),new IntWritable(timeInt));
			} else if(timeInt >= 480 && timeInt < 960 ) {
				output.collect(new Text("less-960"),new IntWritable(timeInt));
			} else if(timeInt >= 960 && timeInt < 2000 ) {
				output.collect(new Text("less-2000"),new IntWritable(timeInt));
			} else if(timeInt >= 2000 ) {
				output.collect(new Text("grate-2000"),new IntWritable(timeInt));
			}

		}





		
	}

}
