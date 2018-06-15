package com.ohaotian.clickhouse.bo;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import lombok.Data;

/** <br>
 * 标题:执行比对任务应答BO <br>
 * 描述: <br>
 * 公司: www.tydic.com<br>
 * 
 * @autho yudg
 * @time 2018年6月6日 上午10:13:13 */
@Data
public class QueryCheckResultRspBO implements Serializable {

	/**  */
	private static final long		  serialVersionUID = -4736871025741036603L;
	/** 数据 **/
	private List<Map<String, Object>> data;
	/** 记录总数 */
	private int						  recordsTotal;

	/** 总页数 */
	private int						  total;

	/** 当前页 */
	private int						  pageNo;

}
