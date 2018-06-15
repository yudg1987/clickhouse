package com.ohaotian.clickhouse.bo;

import com.ohaotian.base.common.bo.ReqInfoBO;

import lombok.Data;

/**
 * 
 * <br>
 * 标题:查询比对视图请求BO <br>
 * 描述: <br>
 * 公司: www.tydic.com<br>
 * @autho yudg
 * @time 2018年6月7日 下午2:05:53
 */
@Data
public class ExecCompareTaskReqBO extends ReqInfoBO {

	/**  */
	private static final long serialVersionUID = 5454039052917037802L;
	/** 比对类型 **/
	private Integer			  checkType;
	/** 比对SQL **/
	private String			  checkSql;
	/** 比对结果入目标视图 **/
	private String			  targetView;

}
