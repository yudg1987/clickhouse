package com.ohaotian.clickhouse.bo;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

import com.ohaotian.base.common.bo.ReqInfoBO;

import lombok.Data;

@Data
public class CreateBaseTableReqBO extends ReqInfoBO implements Serializable {

	/**  */
	private static final long			  serialVersionUID = 1305576893321616409L;
	private String						  scheme;
	private String						  table_zh_name;
	private String						  table_name;
	private String						  table_comment;
	private Integer						  table_type;
	private String						  oper_no;
	private Date						  create_time;
	private String						  remark;
	/*private String zookeeperInfo;*/
	/**是否是创建历史表 1：是 0：否**/
	private Integer isHisTable=0;
	private List<TableColumnDefinitionBO> columns;

}
