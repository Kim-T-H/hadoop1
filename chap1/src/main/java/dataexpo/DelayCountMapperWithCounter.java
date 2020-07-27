package dataexpo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DelayCountMapperWithCounter extends Mapper<LongWritable ,Text, Text, IntWritable>{
	private String workType;
	private final static IntWritable one=new IntWritable(1);
	private Text outkey= new Text();
	
	@Override
	public void setup(Context context) {
		workType= context.getConfiguration().get("workType");
	}
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
		Airline al = new Airline(value);
		context.getCounter(DelayCounters.total_count).increment(1);
		if(workType.equals("departure")) {
			if(al.isDepartureDelayAvailable()) {
				if(al.getDepartureDelayTime()>0) {	//출발지연비행기
					outkey.set(al.getYear()+","+al.getMonth());	//키설정
					context.write(outkey, one);	//리듀서로 전송할 데이터
					context.getCounter(DelayCounters.delay_departure).increment(1);
					
				}else if(al.getDepartureDelayTime()==0) {	//정시출발비행기
					context.getCounter(DelayCounters.scheduled_departure).increment(1); //자원에 정보생성
				}else if(al.getDepartureDelayTime()<0) {
					context.getCounter(DelayCounters.early_departure).increment(1);
				}
				
			}else {
				context.getCounter(DelayCounters.not_available_departure).increment(1);
			}
		}else if(workType.equals("arrival")) {
			if(al.isArriveDelayAvailable()) {
				if(al.getArriveDelayTime()>0) {
					outkey.set(al.getYear()+","+al.getMonth());
					context.write(outkey, one);
					context.getCounter(DelayCounters.delay_arrival).increment(1);
				}else if(al.getArriveDelayTime()==0) {
					context.getCounter(DelayCounters.scheduled_arrival).increment(1);
				}else if(al.getArriveDelayTime()<0) {
					context.getCounter(DelayCounters.early_arrival).increment(1);
				}
			}else {
				context.getCounter(DelayCounters.not_available_arrival).increment(1);
			}
		}
	}

}
