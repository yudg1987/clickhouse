package com.ohaotian.clickhouse.service;

import java.util.List;

import com.ohaotian.base.common.bo.RspPageBO;
import com.ohaotian.clickhouse.bo.ExecCompareTaskReqBO;
import com.ohaotian.clickhouse.bo.ExecCompareTaskRspBO;
import com.ohaotian.clickhouse.bo.QueryCheckResultReqBO;
import com.ohaotian.clickhouse.bo.QueryCheckResultRspBO;
import com.ohaotian.clickhouse.bo.RechangeResultAbundanceReqBO;
import com.ohaotian.clickhouse.bo.RechangeResultAbundanceRspBO;
import com.ohaotian.clickhouse.vo.ColumnVo;

/**
 * <br>
 * 标题:clickhouse操作接口 <br>
 * 描述: <br>
 * 公司: www.tydic.com<br>
 *
 * @autho yudg
 * @time 2018年6月6日 上午10:01:09
 */
public interface IClickHouseBusiService {

    /**
     * <br>
     * 适用场景:执行比对任务 <br>
     * 调用方式: <br>
     * 业务逻辑说明<br>
     *
     * @param execCompareTaskReqBO
     * @return
     * @autho yudg
     * @time 2018年6月6日 上午10:15:37
     */
    ExecCompareTaskRspBO excuteCompare(ExecCompareTaskReqBO execCompareTaskReqBO) throws Exception;

    /**
     * <br>
     * 适用场景:批量插入 <br>
     * 调用方式: <br>
     * 业务逻辑说明<br>
     *
     * @param preSql
     * @param values
     * @return
     * @autho zhoubang
     * @time 2018年6月6日 上午10:15:37
     */
    void batchInsert(String preSql, List<List<ColumnVo>> values) throws Exception;

    /**
     * <br>
     * 适用场景:分页查询比对结果 <br>
     * 调用方式: <br>
     * 业务逻辑说明<br>
     *
     * @param queryCheckResultReqBO
     * @return
     * @autho yudg
     * @time 2018年6月7日 下午3:09:33
     */
    QueryCheckResultRspBO selectSQLByPage(QueryCheckResultReqBO queryCheckResultReqBO);
}
