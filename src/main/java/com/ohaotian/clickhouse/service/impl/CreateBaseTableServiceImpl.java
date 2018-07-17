package com.ohaotian.clickhouse.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ohaotian.clickhouse.bo.CreateBaseTableReqBO;
import com.ohaotian.clickhouse.bo.CreateBaseTableRspBO;
import com.ohaotian.clickhouse.bo.TableColumnDefinitionBO;
import com.ohaotian.clickhouse.config.DataSourceContextHolder;
import com.ohaotian.clickhouse.dao.CheckResultMapper;
import com.ohaotian.clickhouse.service.CreateBaseTableService;

/** <br>
 * 标题:创建基表实现类 <br>
 * 描述: <br>
 * 公司: www.tydic.com<br>
 * 
 * @autho yudg
 * @time 2018年7月10日 上午10:48:55 */
public class CreateBaseTableServiceImpl implements CreateBaseTableService {

	private static final Logger	log	= LoggerFactory.getLogger(CreateBaseTableServiceImpl.class);

	private CheckResultMapper	checkResultMapper;
	private Properties			prop;
	/*static {
		DataSourceContextHolder.setDbType(DataSourceType.CLICKHOUSESOURCE1);
	}*/

	@Override
	public CreateBaseTableRspBO createBaseTable(CreateBaseTableReqBO createBaseTableReqBO) {
		CreateBaseTableRspBO createBaseTableRspBO = new CreateBaseTableRspBO();
		createBaseTableRspBO.setRespCode("0000");
		createBaseTableRspBO.setRespDesc("成功!");
		try {
            String[] sourceArray=prop.getProperty("cluster.sources").split(",");
            
            for(int i=0;i<sourceArray.length;i++){
            	String sourceName=sourceArray[i];
            	DataSourceContextHolder.setDbType(sourceName);
            	Map<String, String> systemMap = getPropConfig(createBaseTableReqBO,sourceName);

    			Map<String, String> map = installColumn(createBaseTableReqBO);

    			String installCreateSql = installCreateSql(createBaseTableReqBO, map, systemMap);
    			String installCreateAllSql = installCreateAllSql(createBaseTableReqBO, map, systemMap);
    			log.debug("installCreateSql=" + installCreateSql);
    			log.debug("installCreateAllSql=" + installCreateAllSql);
    			Map<String, String> sqlMap = new HashMap<>();
    			Map<String, String> sqlMapALL = new HashMap<>();
    			sqlMap.put("sql", installCreateSql);
    			sqlMapALL.put("sql", installCreateAllSql);

    			checkResultMapper.selectTableStruct(sqlMap);
    			checkResultMapper.selectTableStruct(sqlMapALL);
            }
			
		}
		catch (Exception e) {
			createBaseTableRspBO.setRespCode("8888");
			createBaseTableRspBO.setRespDesc(e.getLocalizedMessage());
		}
		return createBaseTableRspBO;
	}

	private Map<String, String> getPropConfig(CreateBaseTableReqBO createBaseTableReqBO,String source) {
		Map<String, String> map = new HashMap<>();
		String scheme = checkResultMapper.selectCurrentDataBase();
		String keySource = "cluster.${source}.zookeeper";
		keySource = keySource.replace("${source}", source);

		String keyFuben = "cluster.${source}.fuben";
		keyFuben = keyFuben.replace("${source}", source);

		String fuben = prop.getProperty(keyFuben);

		String zookeeper = prop.getProperty(keySource).replace("${database}", scheme).replace("${tableName}", createBaseTableReqBO.getTable_name()).replace("${fuben}", fuben);

		String clusterName = prop.getProperty("cluster.name");

		map.put("zookeeper", zookeeper);
		map.put("clusterName", clusterName);
		return map;

	}

	/** <br>
	 * 适用场景: 组装字段<br>
	 * 调用方式: <br>
	 * 业务逻辑说明<br>
	 *
	 * @param createBaseTableReqBO
	 * @autho yudg
	 * @time 2018年7月10日 下午4:19:08 */
	private Map<String, String> installColumn(CreateBaseTableReqBO createBaseTableReqBO) {
		String scheme = checkResultMapper.selectCurrentDataBase();
		Integer cnt = checkResultMapper.selectIsExist(createBaseTableReqBO.getTable_name(), scheme);
		if (null != cnt && cnt.intValue() > 0) {
			throw new IllegalArgumentException("表已存在！");
		}
		/*
		 * if (null == createBaseTableReqBO.getZookeeperInfo()) { throw new IllegalArgumentException("未定义zookeeper节点信息！"); }
		 */
		Map<String, String> map = new HashMap<>();

		List<TableColumnDefinitionBO> columns = createBaseTableReqBO.getColumns();
		StringBuilder str = new StringBuilder("(");
		String distributedColumn = null;
		String primaryColumn = null;
		for (TableColumnDefinitionBO tableColumnDefinitionBO : columns) {
			String column_code = null;
			String column_type = null;
			Integer is_must = null;
			Integer is_distributed = null;
			Integer isPrimary = null;

			is_distributed = tableColumnDefinitionBO.getIs_distributed();
			isPrimary = tableColumnDefinitionBO.getIsPrimary();
			column_code = tableColumnDefinitionBO.getColumn_code();
			column_type = tableColumnDefinitionBO.getColumn_type();
			is_must = tableColumnDefinitionBO.getIs_must();

			str.append(column_code).append(" ");
			if (null != is_must && 1 == is_must) {
				str.append(column_type);
			}
			else {
				str.append("Nullable(").append(column_type).append(")");
			}
			str.append(",\n");
			if (null != is_distributed && 1 == is_distributed) {
				distributedColumn = column_code;
			}
			if (null != isPrimary && 1 == isPrimary) {
				primaryColumn = column_code;
			}
		}
		str.append("notNullDate Date Default now(),\n versionDateTime DateTime Default now() )");
		if (null == distributedColumn && 0 == createBaseTableReqBO.getIsHisTable()) {
			throw new IllegalArgumentException("未定义分区字段！");
		}
		if (null == primaryColumn) {
			throw new IllegalArgumentException("未定义主键字段！");
		}
		map.put("str", str.toString());
		map.put("distributedColumn", distributedColumn);
		map.put("primaryColumn", primaryColumn);
		map.put("scheme", scheme);
		return map;
	}

	/** <br>
	 * 适用场景:创建单节点复制基表 <br>
	 * 调用方式: <br>
	 * 业务逻辑说明<br>
	 *
	 * @param createBaseTableReqBO
	 * @return
	 * @autho yudg
	 * @time 2018年7月10日 下午4:14:46 */
	private String installCreateSql(CreateBaseTableReqBO createBaseTableReqBO, Map<String, String> map, Map<String, String> systemMap) {

		StringBuilder createSql = new StringBuilder();

		createSql.append("CREATE TABLE ").append(map.get("scheme")).append(".").append(createBaseTableReqBO.getTable_name()).append("\n").append(map.get("str")).append("\n")
		        .append(systemMap.get("zookeeper")).append("\n PARTITION BY ");
		if (null != createBaseTableReqBO.getIsHisTable() && 1 == createBaseTableReqBO.getIsHisTable()) {
			createSql.append("notNullDate");
		}
		else {
			createSql.append(map.get("distributedColumn"));
		}
		createSql.append(" ORDER BY (").append(map.get("primaryColumn")).append(", notNullDate) \n SETTINGS index_granularity = 8192");
		return createSql.toString();

	}

	/** <br>
	 * 适用场景:创建对外表 <br>
	 * 调用方式: <br>
	 * 业务逻辑说明<br>
	 *
	 * @param createBaseTableReqBO
	 * @param map
	 * @return
	 * @autho yudg
	 * @time 2018年7月10日 下午4:50:24 */
	private String installCreateAllSql(CreateBaseTableReqBO createBaseTableReqBO, Map<String, String> map, Map<String, String> systemMap) {

		StringBuilder createAllSql = new StringBuilder();

		createAllSql.append("CREATE TABLE ").append(map.get("scheme")).append(".").append(createBaseTableReqBO.getTable_name()).append("_ALL \n").append(map.get("str")).append("\n")
		        .append("ENGINE=Distributed(" + systemMap.get("clusterName") + ",").append("'" + map.get("scheme") + "','" + createBaseTableReqBO.getTable_name() + "',rand())");
		return createAllSql.toString();

	}

	public void setCheckResultMapper(CheckResultMapper checkResultMapper) {
		this.checkResultMapper = checkResultMapper;
	}

	public void setProp(Properties prop) {
		this.prop = prop;
	}

}
