package com.sist.stream;

import java.net.InetSocketAddress;

import org.springframework.stereotype.Repository;

import com.mongodb.*;

import java.util.*;

public class IngrRankDAO {
	private MongoClient mc;
	private DB db;
	private DBCollection dbc;
	
	public IngrRankDAO() {
		try {
			mc=new MongoClient(new ServerAddress(new InetSocketAddress("211.238.142.123", 27017)));
			
			db=mc.getDB("mydb");
			dbc=db.getCollection("food");
			
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
	
	public void musicInsert(IngrRankVO vo) {
		try {
			BasicDBObject where=new BasicDBObject();
			where.put("name", vo.getName());
			DBCursor cursor=dbc.find(where);
			int count=cursor.count();
			cursor.close();
			if (count==0) {
				//insert
				BasicDBObject obj=new BasicDBObject();
				obj.put("name", vo.getName());
				obj.put("count", vo.getCount());
				dbc.insert(obj);
				
			}else{
				//update
				BasicDBObject obj=(BasicDBObject) dbc.findOne(where);
				BasicDBObject up=new BasicDBObject();
				up.put("count", obj.getInt("count")+vo.getCount());
				dbc.update(where, new BasicDBObject("$set", up));
				
			}
			
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		
	}
	
	public List<IngrRankVO> musicAllData(){
		List<IngrRankVO> list=new ArrayList<IngrRankVO>();
		try {
			DBCursor cursor=dbc.find().sort(new BasicDBObject("count", -1));
			while (cursor.hasNext()) {
				BasicDBObject obj=(BasicDBObject)cursor.next();
				IngrRankVO vo=new IngrRankVO();
				vo.setName(obj.getString("name"));
				vo.setCount(obj.getInt("count"));
				list.add(vo);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
				
		return list;
	}
	
}






