package com.ohaotian.clickhouse.dao;

import java.util.List;
import java.util.Map;

import com.ohaotian.base.db.Page;
import com.ohaotian.clickhouse.bo.QueryCheckResultReqBO;

public interface CheckResultMapper {

	List<Map<String, Object>> selectSQLByPage(Page<QueryCheckResultReqBO> page, Map map);
	List<Map<String, Object>> selectTableStruct(Map map);
	String selectCurrentDataBase();
    Integer selectIsExist(String tableName,String dataBase);
}