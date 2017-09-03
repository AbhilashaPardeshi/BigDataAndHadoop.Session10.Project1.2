package project12.task1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Task1Driver 
{

	public enum MyCounters
	{
		INVALIDRECORDS,
		VALIDRECORDS
	}

	
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
		if (args == null || args.length != 2) 
		{
			System.err.println("Driver : Incorrect parameters passed");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		Job job = new Job(conf,"Project1.2.1");
		
		job.setJarByClass(Task1Driver.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));

		Path out = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, out);
		
		out.getFileSystem(conf).delete(out);
		
		job.setMapperClass(Task1Mapper.class);
		job.setReducerClass(Task1Reducer.class);
		job.setCombinerClass(Task1Reducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.waitForCompletion(true);
		
		Counters counters = job.getCounters();
	
		Counter validRecCounter = counters.findCounter(MyCounters.VALIDRECORDS);
		System.out.println("Driver : Number of valid records is "+validRecCounter.getValue());
		
		Counter invalidRecCounter = counters.findCounter(MyCounters.INVALIDRECORDS);
		System.out.println("Driver : Number of invalid records is "+invalidRecCounter.getValue());
		
	}
}