package com.sist.stream;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.sist.dao.OracleIngrDAO;

import scala.Tuple2;
import twitter4j.Status;

public class SparkMain {
	// @Autowired
	private Configuration hconf;// 하둡

	private JobConf jobConf;// 하둡에 저장

	private IngrRankDAO dao=new IngrRankDAO();//
	/*
	 * private SparkConf sconf; private JavaStreamingContext jsc;//참조변수를 이용하지
	 * 못한다.
	 */

	
	public static void main(String[] args) {
		try {
			// ApplicationContext app=new
			// ClassPathXmlApplicationContext("/home/sist/bigdataDev2/SpringSparkStreamMongoDBHadoopProject/src/main/webapp/WEB-INF/config/application-context.xml");
			// Web폴더를 읽지 못한다.
			// SparkMain sm=(SparkMain)app.getBean("sparkMain");
			/*
			 * SparkMain sm=new SparkMain(); sm.sparkInit(); sm.twitterStart();
			 */
			ApplicationContext app=
					  new ClassPathXmlApplicationContext("app.xml");
			
			OracleIngrDAO oracleIngrDAO=(OracleIngrDAO) app.getBean("oracleIngrDAO");
			List<IngrRankVO> ingrList=oracleIngrDAO.selectIngr();
			System.out.println("레시피 갯수는 "+ingrList.size());
			
			
			SparkMain sm = new SparkMain();
			SparkConf sconf= new SparkConf().setAppName("Twitter-Real").setMaster("local[2]");
			JavaStreamingContext jsc = new JavaStreamingContext(sconf, new Duration(10000));

			sm.sparkInit();
			
			String[] filter={
					"z3oFviZHurO6w9PrkBJOKphHA",
					"247l86gk1EQgDSKWc4ud6bLFUTrnr3SHK3YGMBpnSzfBcPOvtq",
					"867997182044942336-lzbX9QjWRdsUdf4Zn8X7PzofyfyAguK",
					"tES2gZmfDRGQWnieKnPUIDqyeCJw9VdYi9S8061v768EI"
			};

			System.setProperty("twitter4j.oauth.consumerKey", filter[0]);
			System.setProperty("twitter4j.oauth.consumerSecret", filter[1]);
			System.setProperty("twitter4j.oauth.accessToken", filter[2]);
			System.setProperty("twitter4j.oauth.accessTokenSecret", filter[3]);

			//String[] data = new String[ingrList.size()];
			//int i=0;
			/*for (IngrRankVO vo: ingrList) {
				data[i]=vo.getName();
				i++;
			}*/
			
			//너무 많아서(800개이상) 다 검색을 못하네
			
			String[] data={"라면", "햄버거", "만두", "피자", "라면", "족발", "치킨", "아이스크림", "김밥", "탕수육"};
			
			JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jsc, data);
			
			// 우리는 Status중 String만 필요하다.
			JavaDStream<String> status = twitterStream.map(new Function<Status, String>() {

				public String call(Status s) throws Exception {
					return s.getText();
				}
			});
			
			final Pattern[] p=new Pattern[data.length];
			for (int a = 0; a < p.length; a++) {
				p[a]=Pattern.compile(data[a]);
			}
			final Matcher[] m=new Matcher[data.length];
			
			JavaDStream<String> words=status.flatMap(new FlatMapFunction<String, String>() {
				public Iterable<String> call(String s) throws Exception {
					List<String> list=new ArrayList<String>();
					for (int a = 0; a < m.length; a++) {
						m[a]=p[a].matcher(s);
						while (m[a].find()) {
							list.add(m[a].group().replace(" ", "$"));//공백을 교체 나중에 다시 바꿀것임
							System.out.println(m[a].group());
						}
					}
					
					return list;
				}
			});
			
			JavaPairDStream<String, Integer> counts=words.mapToPair(new PairFunction<String, String, Integer>() {

				public Tuple2<String, Integer> call(String s) throws Exception {
					return new Tuple2<String, Integer>(s, 1);
				}
			});
			
			JavaPairDStream<String, Integer> reduce=counts.reduceByKey(new Function2<Integer, Integer, Integer>() {
				
				@Override
				public Integer call(Integer sum, Integer i) throws Exception {
					
					return sum+i;
				}
			});
			
			
			sm.hadoopGetFile(reduce);
			//status.print();
			//reduce.print();
			jsc.start();
			jsc.awaitTermination();
			
			
			
			
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

	/*
	 * local[0] ==> NameNode local[1] ==> SecondaryNode local[2] ==> DataNode
	 * ==> 데이터수집 local[3] ==> DataNode local[4] ==> DataNode
	 * 
	 * local[n] ==> n>=2
	 * 
	 * 
	 */
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


	public void hadoopGetFile(JavaPairDStream<String, Integer> jps) {
		try {

			// 몽고디비 전송
			jps.saveAsHadoopFiles("hdfs://NameNode:9000/user/hadoop/food_ns1", "", Text.class, IntWritable.class, TextOutputFormat.class, jobConf );

		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}

	}
	
	/*@Scheduled(cron = "0 * * * * *") 
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
		
	}*/
	

}
