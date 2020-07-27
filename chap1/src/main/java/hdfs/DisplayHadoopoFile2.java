package hdfs;


import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DisplayHadoopoFile2 {

	public static void main(String[] args) throws IOException {
		Configuration conf=new Configuration();
		conf.addResource(new Path("/usr/local/hadoop-2.9.2/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop-2.9.2/etc/hadoop/hdfs-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop-2.9.2/etc/hadoop/mapred-site.xml"));
		FileSystem fs=FileSystem.get(conf);
		Path pt=new Path("consoledata"); 
		//br:하둡 서버의 consoledata 파일 읽기위한 스트림
		FSDataInputStream br= fs.open(pt);
		while(true) {
			try {
				System.out.println(br.readUTF());
			}catch(Exception e) {
				break;
			}
		}
		br.close();
	}

}
