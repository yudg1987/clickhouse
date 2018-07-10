package com.ohaotian.clickhouse.service;

import com.ohaotian.clickhouse.bo.CreateBaseTableReqBO;
import com.ohaotian.clickhouse.bo.CreateBaseTableRspBO;

/** <br>
 * 标题:创建基表业务服务 <br>
 * 描述: <br>
 * 公司: www.tydic.com<br>
 * 
 * @autho yudg
 * @time 2018年7月10日 上午10:31:22 */
public interface CreateBaseTableService {

	CreateBaseTableRspBO createBaseTable(CreateBaseTableReqBO createBaseTableReqBO);
}
