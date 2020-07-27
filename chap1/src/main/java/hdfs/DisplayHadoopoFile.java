package hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DisplayHadoopoFile {

	public static void main(String[] args) {
		try {
			String filepath="hdfs://localhost:9000/user/hadoop/in";
			Path pt=new Path(filepath);
			Configuration conf= new Configuration();
			conf.set("fs.defaultFS", filepath); 
			FileSystem fs=FileSystem.get(conf);
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));	//파일 시스템에 오픈
			//fs.open(pt) : hadoop 서버에 연결하여 파일 읽기
			String line=null;
			while((line =br.readLine()) !=null) {
				System.out.println(line);
				
			}
			br.close();
		}catch(IOException e) {
			e.printStackTrace();
		}
	}

}
