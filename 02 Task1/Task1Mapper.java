package project12.task1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import project12.task1.Task1Driver.MyCounters;



public class Task1Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable>
{
	private Text className;
	private DoubleWritable fare; 
	
	
	@Override
	public void map(LongWritable key,Text titanicRecord,Context context) throws IOException, InterruptedException
	{
		String strValue = titanicRecord.toString();
		System.out.println("MapperClass : Current record is [ "+strValue+" ]");
		
		String[] split = strValue.split(",");
				
		if( split.length<10 || split[2].equals("") || split[9].equals("") )
		{
			System.out.println("MapperClass : Invalid record found");
			context.getCounter(MyCounters.INVALIDRECORDS).increment(1);
		}
		else
		{
			context.getCounter(MyCounters.VALIDRECORDS).increment(1);
			
			String strClassName = split[2];
			className = new Text(strClassName); 
		
			String strFare  = split[9];
			fare  =  new DoubleWritable(Double.parseDouble(strFare));
			
			System.out.println("MapperClass : Inserting ["+strClassName+" , "+strFare+"] in the context");
			
			context.write(className, fare);
		}
	}

	
}