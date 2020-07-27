package dataexpo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//전체 비행 건수 출력
public class DelayCount extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		String[] arg= { "-D","workType=arrival",			//-D 는 툴 사용의미 worktype는 파라미터 설정
				"hdfs://localhost:9000/user/hadoop/dataexpo/1988.csv",
				"arrival-1987"};
		int res=ToolRunner.run(new Configuration(), new DelayCount(),arg);
		}

	@Override
	public int run(String[] args) throws Exception {
		String[] otherargs=  new GenericOptionsParser(getConf(), args).getRemainingArgs();
		if(otherargs.length != 2) {
			//PrintStream  System.err: 표준오류객체, 콘솔출력
			System.err.println("Usage: DelayCount <in>,<out>");
			System.exit(2);	//프로세스 종료. 프로그램 강제 종료
		}
		Job job= new Job(getConf(),"DelayCount");
		FileInputFormat.addInputPath(job,new Path(otherargs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherargs[1]));
		job.setJarByClass(DelayCount.class);
		job.setMapperClass(DelayCountMapperWithCounter.class);
		job.setReducerClass(DelayCountReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.waitForCompletion(true);
		for(DelayCounters d : DelayCounters.values()) {
			long tot=job.getCounters().findCounter(d).getValue();
			System.out.println(d+":"+tot);
		}
		return 0;
	}
	
	
	}

