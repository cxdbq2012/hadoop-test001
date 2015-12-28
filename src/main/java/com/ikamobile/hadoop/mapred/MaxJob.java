package com.ikamobile.hadoop.mapred;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class MaxJob {

	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(MaxJob.class);
		conf.setJobName("Max job...123");
		FileInputFormat.addInputPath(conf,new Path("/user/root/sme/"));
		SimpleDateFormat sdf = new SimpleDateFormat("HH_mm_ss");
		FileOutputFormat.setOutputPath(conf,new Path("/user/root/smeJob-"+sdf.format(new Date())));
		
		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(MyReduce.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		JobClient.runJob(conf);
	}
}
