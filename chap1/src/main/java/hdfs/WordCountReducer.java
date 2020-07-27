package hdfs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
//this,1	,1
// is,1 ,1
//  a,1 ,1
//book,1 ,
//pen,1
public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	private IntWritable result=new IntWritable();
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		int sum=0;
		for(IntWritable v:values) {sum+=v.get();}
		result.set(sum);
		context.write(key, result);
		//this,2
		//is,2
		//a,2
		//book,1
		//pen,1
		
	}
}
