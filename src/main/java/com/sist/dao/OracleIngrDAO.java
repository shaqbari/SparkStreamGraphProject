package com.sist.dao;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.sist.stream.IngrRankDAO;
import com.sist.stream.IngrRankVO;

@Repository
public class OracleIngrDAO {

	@Autowired
	private OracleIngrMapper oracleIngrMapper;
	
	public List<IngrRankVO> selectIngr(){
		
		return oracleIngrMapper.selectAllIngr();
	};

	
}
