package project12.task2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import project12.task2.Task2Driver.MyCounters;



public class Task2Mapper extends Mapper<LongWritable, Text, Text, LongWritable>
{
	private Text className;
	private static final LongWritable ONE  =  new LongWritable(1); 
	
	
	@Override
	public void map(LongWritable key,Text titanicRecord,Context context) throws IOException, InterruptedException
	{
		String strValue = titanicRecord.toString();
		System.out.println("MapperClass : Current record is [ "+strValue+" ]");
		
		String[] split = strValue.split(",");
				
		if( split.length<12 || split[1].equals("") || split[2].equals("")  || split[11].equals("") )
		{
			System.out.println("MapperClass : Invalid record found");
			context.getCounter(MyCounters.INVALIDRECORDS).increment(1);
		}
		else
		{
			context.getCounter(MyCounters.VALIDRECORDS).increment(1);
			
			String strSurvived = split[1];
			String strEmbarked = split[11];
			
			if (strSurvived.trim().equals("0") && strEmbarked.trim().toLowerCase().startsWith("s")) 
			{
				context.getCounter(MyCounters.REQUIREDRECORDS).increment(1);
				
				String strClassName = split[2];
				className = new Text(strClassName); 

				System.out.println("MapperClass : Inserting ["+strClassName+" , 1] in the context");
				
				context.write(className, ONE);

				
			}
		}
	}

	
}