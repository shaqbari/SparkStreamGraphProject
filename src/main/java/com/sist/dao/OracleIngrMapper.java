package com.sist.dao;

import java.util.List;

import org.apache.ibatis.annotations.Select;

import com.sist.stream.IngrRankVO;

public interface OracleIngrMapper {
	@Select("Select name from ingredient")
	public List<IngrRankVO> selectAllIngr();
	
	
}
