package com.ohaotian.clickhouse.bo;

import java.io.Serializable;
import java.util.List;

import com.ohaotian.base.common.bo.ReqPageBO;
import com.ohaotian.clickhouse.vo.ColumnVo;

import lombok.Data;

/** <br>
 * 标题:查询比对视图应答BO <br>
 * 描述: <br>
 * 公司: www.tydic.com<br>
 * 
 * @autho yudg
 * @time 2018年6月7日 下午2:06:05 */
@Data
public class QueryCheckResultReqBO extends ReqPageBO implements Serializable {

	/**  */
	private static final long serialVersionUID = -1848976002270495191L;
	private String sql;
	private List<ColumnVo> whereConditionList;

}
