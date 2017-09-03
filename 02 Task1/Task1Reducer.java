package project12.task1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task1Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> 
{

	@Override
	public void reduce(Text className,Iterable<DoubleWritable> fares,Context context) throws IOException, InterruptedException
	{
		long count  = 0;
		double totalFare = 0; 
		for (DoubleWritable fare : fares) 
		{
			totalFare = totalFare + fare.get();
			count++;
		}

		double averageFare = totalFare/count; 
		
		System.out.println("ReducerClass : Inserting ["+className.toString()+" , "+averageFare);
		
		context.write(className, new DoubleWritable(averageFare));
	}

}