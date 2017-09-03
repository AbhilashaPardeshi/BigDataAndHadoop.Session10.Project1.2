package project12.task3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import project12.task3.Task3Driver.MyCounters;



public class Task3Mapper extends Mapper<LongWritable, Text, Text, LongWritable>
{
	private Text keyValue;
	private static final LongWritable ONE  =  new LongWritable(1); 
	
	
	@Override
	public void map(LongWritable key,Text titanicRecord,Context context) throws IOException, InterruptedException
	{
		String strValue = titanicRecord.toString();
		System.out.println("MapperClass : Current record is [ "+strValue+" ]");
		
		String[] split = strValue.split(",");
				
		if( split.length<5 || split[1].equals("") || split[2].equals("")  || split[4].equals("") )
		{
			System.out.println("MapperClass : Invalid record found");
			context.getCounter(MyCounters.INVALIDRECORDS).increment(1);
		}
		else
		{
			context.getCounter(MyCounters.VALIDRECORDS).increment(1);
			
			String strSurvived = split[1];
			String strSex = split[4];
			
			
			if (strSurvived.trim().equals("1") ) 
			{
				context.getCounter(MyCounters.REQUIREDRECORDS).increment(1);
				
				String strClassName = split[2];
				keyValue = new Text(strClassName+"-"+strSex); 

				System.out.println("MapperClass : Inserting ["+keyValue.toString()+" , 1] in the context");
				
				context.write(keyValue, ONE);

				
			}
		}
	}

	
}