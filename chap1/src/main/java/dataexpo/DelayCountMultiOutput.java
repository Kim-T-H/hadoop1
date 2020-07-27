package dataexpo;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DelayCountMultiOutput {
//출발지연 도착지연 한번에
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		String in="hdfs://localhost:9000/user/hadoop/dataexpo/1988.csv";
		String out="1988out";
				Job job = new Job(new Configuration(),"DelayCountMultiOutput");
		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));
		job.setJarByClass(DelayCountMultiOutput.class);
		job.setMapperClass(DelayCountMapperMultiOutput.class);
		job.setReducerClass(DelayCountReducerMultiOutput.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//결과물 파일을 여러개의 파일로 출력하도록 설정하기
		MultipleOutputs.addNamedOutput(job, "departure", TextOutputFormat.class, Text.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "arrival", TextOutputFormat.class, Text.class, IntWritable.class);
		job.waitForCompletion(true);
			for(DelayCounters d : DelayCounters.values()) {
				long tot=job.getCounters().findCounter(d).getValue();
				System.out.println(d+":"+tot);
			}
		}
	
	private static class DelayCountReducerMultiOutput extends Reducer<Text,IntWritable, Text, IntWritable>{
		private MultipleOutputs<Text,IntWritable > mos;
		private Text outkey=new Text();
		private IntWritable result=new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			String[] columns=key.toString().split(",");
			outkey.set(columns[1]+","+columns[2]);
			int sum=0;
			for(IntWritable v :values) sum+=v.get();
			result.set(sum);
			if(columns[0].equals("D")) {
				mos.write("departure", outkey, result);
			}else if(columns[0].equals("A")) {
				mos.write("arrival", outkey, result);
			}
		}
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
		mos.close();
		}

		@Override
		protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			mos=new MultipleOutputs<Text, IntWritable>(context);
		}
	}
		private static class DelayCountMapperMultiOutput extends Mapper<LongWritable,Text,Text,IntWritable>{
			private final static IntWritable ONE=new IntWritable(1);
			private Text outkey= new Text();
			@Override
			protected void map(LongWritable key, Text value,
					Mapper<LongWritable, Text, Text, IntWritable>.Context context)
					throws IOException, InterruptedException {
				Airline al=new Airline(value);
				if(al.isDepartureDelayAvailable()) {	//출발대상
					if(al.getDepartureDelayTime()>0) {
						outkey.set("D,"+al.getYear()+","+al.getMonth());
						context.write(outkey, ONE);
					}
				}
				if(al.isArriveDelayAvailable()) {
					if(al.getArriveDelayTime()>0) {
						outkey.set("A,"+al.getYear()+","+al.getMonth());
						context.write(outkey, ONE);
					}
				}
			}
			
			
		}
		
	}

	



