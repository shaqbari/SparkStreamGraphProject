package com.sist.stream;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import javax.annotation.PostConstruct;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class AutoRead {
	private Configuration hconf;// 하둡

	private JobConf jobConf;// 하둡에 저장

	private IngrRankDAO dao=new IngrRankDAO();
	
	@PostConstruct
	public void sparkInit() {
		try {
			// 참조변수를 이용하면 못쓴다.
			hconf = new Configuration();
			hconf.set("fs.default.name", "hdfs://NameNode:9000");
			jobConf = new JobConf(hconf);

			/*
			 * sconf=new
			 * SparkConf().setAppName("Twitter-Real").setMaster("local[2]");
			 * jsc=new JavaStreamingContext(sconf, new Duration(10000));
			 */

		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
	
	
	//@Scheduled(cron = "10 * * * * *") //매분 10초마다
	@Scheduled(fixedDelay = 4000) 
	public void hadoopFileRead(){
		System.out.println("몽고디비 갱신!!");
		
		try {
			FileSystem fs=FileSystem.get(hconf);
			FileStatus[] status=fs.listStatus(new Path("/user/hadoop/"));
			for (FileStatus sss : status) {
				
				String temp=sss.getPath().getName();
				if (!temp.startsWith("food_ns1")) {
					continue; //위단어로 시작하지 않으면 넘어간다. 다른폴더가 생겨도 상관없어진다.
				}
				
				FileStatus[] status2=fs.listStatus(new Path("/user/hadoop/"+sss.getPath().getName()));
				for (FileStatus ss : status2) {
					String name=ss.getPath().getName();
					if (!name.equals("_SUCCESS")) {
						FSDataInputStream is=fs.open(new Path("/user/hadoop/"+sss.getPath().getName()+"/"+ss.getPath().getName()));
						BufferedReader br=new BufferedReader(new InputStreamReader(is));
						while (true) {
							String line=br.readLine();
							if (line==null) {
								break;
							}
							StringTokenizer st=new StringTokenizer(line);
							IngrRankVO vo=new IngrRankVO();
							vo.setName(st.nextToken().trim().replace("$", " "));
							vo.setCount(Integer.parseInt(st.nextToken().trim()));
							dao.musicInsert(vo);
							
							
						}
						br.close();
					}
				}
				//읽고 다음에 읽을때 다시 읽지 않기 위해 읽은 폴더를 지운다.
				fs.delete(new Path("/user/hadoop/"+sss.getPath().getName()), true);
				
				
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
