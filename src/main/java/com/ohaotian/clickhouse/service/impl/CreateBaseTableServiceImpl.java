package com.ohaotian.clickhouse.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ohaotian.clickhouse.bo.CreateBaseTableReqBO;
import com.ohaotian.clickhouse.bo.CreateBaseTableRspBO;
import com.ohaotian.clickhouse.bo.TableColumnDefinitionBO;
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

	@Override
	public CreateBaseTableRspBO createBaseTable(CreateBaseTableReqBO createBaseTableReqBO) {
		CreateBaseTableRspBO createBaseTableRspBO = new CreateBaseTableRspBO();
		createBaseTableRspBO.setRespCode("0000");
		createBaseTableRspBO.setRespDesc("成功!");
		try {
			Map<String, String> map = installColumn(createBaseTableReqBO);

			String installCreateSql = installCreateSql(createBaseTableReqBO, map);
			String installCreateAllSql = installCreateAllSql(createBaseTableReqBO, map);
			log.debug("installCreateSql=" + installCreateSql);
			log.debug("installCreateAllSql=" + installCreateAllSql);
			Map<String, String> sqlMap = new HashMap<>();
			sqlMap.put("sql", installCreateSql);
			checkResultMapper.selectTableStruct(sqlMap);

			sqlMap.put("sql", installCreateAllSql);
			checkResultMapper.selectTableStruct(sqlMap);
		}
		catch (Exception e) {
			createBaseTableRspBO.setRespCode("8888");
			createBaseTableRspBO.setRespDesc(e.getMessage());
		}

		return createBaseTableRspBO;
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
		if (null == createBaseTableReqBO.getZookeeperInfo()) {
			throw new IllegalArgumentException("未定义zookeeper节点信息！");
		}
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
			if (is_must == null) {
				str.append(column_type);
			}
			else {
				str.append("Nullable(").append(column_type).append(")");
			}
			str.append(",\n");
			if (1 == is_distributed) {
				distributedColumn = column_code;
			}
			if (1 == isPrimary) {
				primaryColumn = column_code;
			}
		}
		str.append("notNullDate Date Default now(),\n versionDateTime DateTime Default now() )");
		if (null == distributedColumn) {
			throw new IllegalArgumentException("未定义分区字段！");
		}
		if (null == primaryColumn) {
			throw new IllegalArgumentException("未定义主键字段！");
		}
		map.put("str", str.toString());
		map.put("distributedColumn", distributedColumn);
		map.put("primaryColumn", primaryColumn);
		String scheme = checkResultMapper.selectCurrentDataBase();
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
	private String installCreateSql(CreateBaseTableReqBO createBaseTableReqBO, Map<String, String> map) {

		StringBuilder createSql = new StringBuilder();

		createSql.append("CREATE TABLE ").append(map.get("scheme")).append(".").append(createBaseTableReqBO.getTable_name()).append("\n").append(map.get("str")).append("\n")
		        .append(createBaseTableReqBO.getZookeeperInfo()).append("\n PARTITION BY ").append(map.get("distributedColumn")).append(" ORDER BY (").append(map.get("primaryColumn"))
		        .append(", notNullDate) \n SETTINGS index_granularity = 8192");
		return createSql.toString();

	}
    /**
     * 
     * <br>
     * 适用场景:创建对外表	<br>
     * 调用方式:	<br>
     * 业务逻辑说明<br>
     *
     * @param createBaseTableReqBO
     * @param map
     * @return
     * @autho yudg
     * @time 2018年7月10日 下午4:50:24
     */
	private String installCreateAllSql(CreateBaseTableReqBO createBaseTableReqBO, Map<String, String> map) {

		StringBuilder createAllSql = new StringBuilder();

		createAllSql.append("CREATE TABLE ").append(map.get("scheme")).append(".").append(createBaseTableReqBO.getTable_name()).append("_ALL \n").append(map.get("str")).append("\n")
		        .append("ENGINE=Distributed(ck_cluster,").append("'"+map.get("scheme")+"','"+createBaseTableReqBO.getTable_name()+"',rand())");
		return createAllSql.toString();

	}

	public void setCheckResultMapper(CheckResultMapper checkResultMapper) {
		this.checkResultMapper = checkResultMapper;
	}

}
