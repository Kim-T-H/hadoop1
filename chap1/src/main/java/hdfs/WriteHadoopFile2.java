package hdfs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WriteHadoopFile2 {

	public static void main(String[] args) throws Exception {
		Configuration conf= new Configuration();
		conf.addResource(new Path("/usr/local/hadoop-2.9.2/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop-2.9.2/etc/hadoop/hdfs-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop-2.9.2/etc/hadoop/mapred-site.xml"));
		String file ="src/main/java/hdfs/DisplayHadoopoFile.java";
		FileSystem hdfs=FileSystem.get(conf);
		//내컴퓨터의 파일 읽기
		FileInputStream fis=new FileInputStream(file);
		int len=0;
		byte[] buf = new byte[8096];
		Path path=new Path(file);	//하둡서버에 저장할 파일 지정. path도 같이 설정됨
		FSDataOutputStream out=hdfs.create(path);	//out: 하둡서버에 출력될 파일 지정
		
		while((len=fis.read(buf)) !=-1) {	//fis 는 local에 있는 파일
			out.writeUTF(new String(buf,0,len));
		}
		fis.close();out.flush();out.close();
		

	}

}
