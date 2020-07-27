package hdfs;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
//Mapper 클래스 : 맵의 기능을 처리
//					원본데이터를 읽어서 매핑작업 =>결과=>리듀서의 입력값
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		private final static IntWritable one= new IntWritable(1);//1
		private Text word = new Text();
		@Override
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
			//value : this a book
			//StringTokenizer : 문자열을 토큰화 해주는 클래스
			//토큰:	기본적 공백기준으로 분리된 문자열, 의미를 가지고있는 최소단위의 문자열
			StringTokenizer itr=new StringTokenizer(value.toString());// value.toString()   this is book 이 들어감
			while(itr.hasMoreTokens()) {
				//word : this, is , a, book
				word.set(itr.nextToken());	//nextToken() 에  
				//context: 맵퍼의 산출물=> 리듀서의 입력값
				context.write(word, one);	// this,1	,1
												// is,1 ,1
												//  a,1 ,1
												//book,1 ,
												//pen,1
			}
		}
	
}
