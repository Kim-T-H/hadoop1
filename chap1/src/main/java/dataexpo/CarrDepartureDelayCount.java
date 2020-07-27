package dataexpo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//항공사코드별 출발지연 건수 산출하기
public class CarrDepartureDelayCount {

	public static void main(String[] args) throws Exception, IOException {
		Configuration conf= new Configuration();
		String in="hdfs://localhost:9000/user/hadoop/dataexpo/1988.csv";
		String out="hdfs://localhost:9000/user/hadoop/dataexpo/1988out3";
		Job job=new Job(conf,"CarrDepartureDelayCount");
		FileInputFormat.addInputPath(job,new Path(in)); 
		FileOutputFormat.setOutputPath(job, new Path(out));
		job.setJarByClass(CarrDepartureDelayCount.class);
		job.setMapperClass(CarrDepartureDelayCountMapper.class); 
		job.setReducerClass(DelayCountReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.waitForCompletion(true);


	}

}
