package com.mvad.sessionLog;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import parquet.hadoop.thrift.ParquetThriftInputFormat;

import com.mediav.data.log.CookieEvent;

/**
 * SessionLogParquet.java 1.0 Aug 24, 2015
 *Copyright 2009-2011 Mediav Inc. All rights reserved.
 */

/**
 * @author chenqy@mvad.com
 * @version 1.0 Aug 24, 2015
 */
public class SessionLogParquet extends Configured implements Tool{

  /**
   * @param args
   */
  public static void main(String[] args) {
    if (args.length == 2) {
      int ret = ToolRunner.run(new SessionLogParquet(), args);
      System.exit(ret);
    } else {
      System.out.println("wrong args, the args.length != 2");
      System.exit(-1);
    }

  }
 public static class mapper extends Mapper<LongWritable, CookieEvent, IntWritable, LongWritable>{

   
  @Override
  public void map(LongWritable key, CookieEvent value, org.apache.hadoop.mapreduce.Mapper.Context context)
      throws IOException, InterruptedException {
     if (value != null) {
       if (value.isSetEventType()) {
        context.write(new IntWritable(value.getEventType()),new LongWritable(1));
      }
      
    }
  }
 }
  
 public static class reducer extends Reducer<IntWritable, LongWritable, Text, LongWritable>
 {
   enum log_type_count {
     S, T, C, V, OTHER
   }

   @Override
   public void reduce(IntWritable key, Iterable<LongWritable> value, Context context) throws IOException, InterruptedException {
     long outValue = 0;
     for (LongWritable count : value) {
       outValue += count.get();
     }
     String outputKey = null;
     switch ((char)key.get()) {
       case 's':
         context.getCounter(log_type_count.S).increment(outValue);
         outputKey = "s";
         break;
       case 't':
         context.getCounter(log_type_count.T).increment(outValue);
         outputKey = "t";
         break;
       case 'c':
         context.getCounter(log_type_count.C).increment(outValue);
         outputKey = "c";
         break;
       case 'v':
         context.getCounter(log_type_count.V).increment(outValue);
         outputKey = "v";
         break;
       default:
         context.getCounter(log_type_count.OTHER).increment(outValue);
         outputKey = "other";
     }
     context.write(new Text(outputKey), new LongWritable(outValue));
   }
 }
 public static class combine extends Reducer<IntWritable, LongWritable, IntWritable, LongWritable>
 {
   @Override
   public void reduce(IntWritable key, Iterable<LongWritable> value, Context context) throws IOException, InterruptedException {
     long outValue = 0;
     for (LongWritable count : value) {
       outValue += count.get();
     }
     context.write(key, new LongWritable(outValue));
   }
   
 }
/* (non-Javadoc)
 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
 */
@Override
public int run(String[] arg0) throws Exception {
  Configuration conf = new Configuration();
  Job job = Job.getInstance(conf, "[etl][Count sessionlog parquet eventType][" + (new SimpleDateFormat("HH:mm").format(new Date())) + "]");
  Path inputParquet = new Path(arg0[0]);
  Path output = new Path(arg0[1]);

  job.setJarByClass(SessionLogParquet.class);
  job.setMapOutputKeyClass(IntWritable.class);
  job.setMapOutputValueClass(LongWritable.class);
  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(LongWritable.class);

  job.setInputFormatClass(ParquetThriftInputFormat.class);
  ParquetThriftInputFormat.addInputPath(job, inputParquet);
  ParquetThriftInputFormat.setReadSupportClass(job, CookieEvent.class);
  ParquetThriftInputFormat.setThriftClass(job.getConfiguration(), CookieEvent.class);

  job.setMapperClass(mapper.class);
  job.setCombinerClass(combine.class);
  job.setReducerClass(reducer.class);
  job.setNumReduceTasks(15);

  job.setOutputFormatClass(TextOutputFormat.class);
  FileOutputFormat.setOutputPath(job, output);

  boolean success = job.waitForCompletion(true);
  return success ? 0 : 1;
}
  
}
