package project12.task2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task2Reducer extends Reducer<Text, LongWritable, Text, LongWritable> 
{

	@Override
	public void reduce(Text className,Iterable<LongWritable> counts,Context context) throws IOException, InterruptedException
	{
		long total = 0; 
		for (LongWritable count : counts) 
		{
			total = total + count.get();
		}

		System.out.println("ReducerClass : Inserting ["+className.toString()+" , "+total);
		
		context.write(className, new LongWritable(total));
	}

}