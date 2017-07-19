package com.sist.stream;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.StringTokenizer;

import javax.annotation.PostConstruct;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.sist.mongo.MusicRankDAO;
import com.sist.mongo.MusicRankVO;
import java.util.*;

@Controller
public class MainController {
	@Autowired
	private Configuration conf;
	
	@Autowired
	private MusicRankDAO dao;
	
	@RequestMapping("main/graph.do")
	public String main_graph(Model model){
		hadoopFileRead();
		
		
		String[] color={"#FF0F00", "#FF6600", "#FF9E01", "#FCD202", "#F8FF01"
						, "#B0DE09", "#04D215", "#0D8ECF", "#0D52D1", "#8A0CCF"};
		List<MusicRankVO> list=dao.musicAllData();
		JSONArray arr=new JSONArray();
		
		int i=0;
		for (MusicRankVO vo : list) {
			JSONObject obj=new JSONObject();
			obj.put("country", vo.getName());
			obj.put("visits", vo.getCount());
			obj.put("color", color[i]);
			arr.add(obj);
			i++;
			
		}
		model.addAttribute("json", arr.toJSONString());
		
		return "main/graph";
	}
	
	@RequestMapping("main/main.do")
	public String main_main(Model model){
		
		return "main/main";
	}
	
	
	public void hadoopFileRead(){
		try {
			FileSystem fs=FileSystem.get(conf);
			FileStatus[] status=fs.listStatus(new Path("/user/hadoop/"));
			for (FileStatus sss : status) {
				
				String temp=sss.getPath().getName();
				if (!temp.startsWith("music_ns1")) {
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
							MusicRankVO vo=new MusicRankVO();
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
	
	//객채시작하자마자 이 메소드 호출
	@PostConstruct
	public void init(){
		//파일한번읽고 몽고디비에 데이터 넣음
		hadoopFileRead();
		
	}
	
	
}
