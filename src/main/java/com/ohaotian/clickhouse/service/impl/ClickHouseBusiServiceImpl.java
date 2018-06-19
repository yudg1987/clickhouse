package com.ohaotian.clickhouse.service.impl;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ohaotian.base.db.MySql5Dialect;
import com.ohaotian.base.db.Page;
import com.ohaotian.clickhouse.bo.ExecCompareTaskReqBO;
import com.ohaotian.clickhouse.bo.ExecCompareTaskRspBO;
import com.ohaotian.clickhouse.bo.QueryCheckResultReqBO;
import com.ohaotian.clickhouse.bo.QueryCheckResultRspBO;
import com.ohaotian.clickhouse.bo.TargetViewBO;
import com.ohaotian.clickhouse.config.ClickHouseConfig;
import com.ohaotian.clickhouse.dao.CheckResultMapper;
import com.ohaotian.clickhouse.service.IClickHouseBusiService;
import com.ohaotian.clickhouse.vo.ColumnVo;

import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.response.ClickHouseResultSet;

/**
 * <br>
 * 标题:clickHouse接口实现类 <br>
 * 描述: <br>
 * 公司: www.tydic.com<br>
 *
 * @autho yudg
 * @time 2018年6月6日 上午10:01:37
 */
public class ClickHouseBusiServiceImpl implements IClickHouseBusiService {

    private static final Logger log = LoggerFactory.getLogger(ClickHouseBusiServiceImpl.class);

    private CheckResultMapper checkResultMapper;
    private ClickHouseConfig clickHouseConfig;

    @Override
    public ExecCompareTaskRspBO excuteCompare(ExecCompareTaskReqBO execCompareTaskReqBO) throws Exception {
        String checkSql = execCompareTaskReqBO.getCheckSql();
        if (null == checkSql || checkSql.isEmpty()) {
            throw new IllegalArgumentException("比对sql不能为空");
        }
        String targetView = execCompareTaskReqBO.getTargetView();
        if (null == targetView || targetView.isEmpty()) {
            throw new IllegalArgumentException("目标视图或表名不能为空");
        }
        List<TargetViewBO> targetViewBOs = new ArrayList<TargetViewBO>();
        Map<String, String> tableViewMap = new HashMap<>();
        tableViewMap.put("sql", "DESC " + targetView);
        List<Map<String, Object>> tableStructs = checkResultMapper.selectTableStruct(tableViewMap);
        for (Map<String, Object> viewMap : tableStructs) {
            TargetViewBO bo = new TargetViewBO();
            bo.setColumnName(String.valueOf(viewMap.get("name")));
            bo.setDataType(String.valueOf(viewMap.get("type")));
            targetViewBOs.add(bo);
        }
        Map<String, String> sqlMap = new HashMap<>();
        sqlMap.put("sql", checkSql);
        List<Map<String, Object>> queryResults = checkResultMapper.selectTableStruct(sqlMap);
        //StringBuilder colomnSQL = new StringBuilder("INSERT INTO ").append(targetView).append(" (");
        StringBuilder colomnSQL = new StringBuilder("INSERT INTO ").append(targetView);
        int columnCount = targetViewBOs.size();
        log.debug("columnCount=" + targetViewBOs.size());
        log.debug("targetViewBOs=" + targetViewBOs);
        int pos = 0;
        String columnNames=targetViewBOs.stream().map(TargetViewBO::getColumnName).collect(Collectors.joining(","));
        colomnSQL.append("(").append(columnNames).append(")").append("VALUES");
        StringBuilder valueSQL = new StringBuilder();
        SimpleDateFormat myFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat myDateFmt = new SimpleDateFormat("yyyy-MM-dd");
        for (Map<String, Object> resultMap : queryResults) {
            StringBuilder valueSQLTemp = new StringBuilder();
            if (valueSQL.length() > 0) {
                valueSQLTemp.append(",(");
            } else {
                valueSQLTemp.append("(");
            }
            pos = 0;
            for (int i = 0; i < columnCount; i++) {
                TargetViewBO bo = targetViewBOs.get(i);
                String columnName = bo.getColumnName();
                Object value;
                String dataType = bo.getDataType();
                try {
                    value = resultMap.get(columnName);
                } catch (Exception e) {
                    if (dataType.indexOf("Int") != -1) {
                        valueSQLTemp.append("0");
                    } else {
                        valueSQLTemp.append("NULL");
                    }
                    continue;
                }
                if (value == null || "".equals(value)) {
                    if (dataType.indexOf("Int") != -1) {
                        valueSQLTemp.append("0");
                    } else {
                        valueSQLTemp.append("NULL");
                    }
                } else {
                    if (dataType.indexOf("DateTime") != -1 || dataType.indexOf("TimeStamp") != -1) {
                        valueSQLTemp.append("toDateTime('" + myFmt.format(value) + "')");
                    } else if (dataType.indexOf("Date") != -1) {
                        valueSQLTemp.append("toDate('" + myDateFmt.format(value) + "')");
                    } else if (dataType.indexOf("String") != -1) {
                        valueSQLTemp.append("'" + value + "'");
                    } else {
                        valueSQLTemp.append(value);
                    }
                }
                if (pos < columnCount - 1) {
                    valueSQLTemp.append(",");
                }
                pos++;
            }
            valueSQLTemp.append(")");
            valueSQL.append(valueSQLTemp);
        }
        String insertSQL = colomnSQL.append(valueSQL).toString();
        log.debug("insertSQL=" + insertSQL);
        Map<String, String> excuteSQLMap = new HashMap<>();
        excuteSQLMap.put("sql", insertSQL);
        checkResultMapper.selectTableStruct(excuteSQLMap);
        ExecCompareTaskRspBO execCompareTaskRspBO = new ExecCompareTaskRspBO();
        execCompareTaskRspBO.setRespCode("0000");
        execCompareTaskRspBO.setRespDesc("成功!");
        return execCompareTaskRspBO;
    }

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
    public void batchInsert(String preSql, List<List<ColumnVo>> values) throws Exception {
        PreparedStatement statement = null;
        ClickHouseConnection connection = null;
        try {
            connection = clickHouseConfig.clickHouseConnection();
            statement = connection.prepareStatement(preSql);
            for (List<ColumnVo> list : values) {
                for (int i = 1, size = list.size(); i <= size; i++) {
                    final ColumnVo columnVo = list.get(i - 1);
                    int dataType = Integer.valueOf(columnVo.getSqlType());
                    if (StringUtils.isBlank(columnVo.getValue())) {
                        if (dataType == 91) {// DATE类型
                            statement.setDate(i + 1, new Date(0L));
                        } else if (dataType == 92 || dataType == 93) {// timestamp datetime
                            statement.setObject(i + 1, null, Types.TIMESTAMP);
                        } else if (dataType == -5) {// Int64
                            statement.setObject(i, 0, Types.INTEGER);
                        } else {
                            statement.setObject(i, columnVo.getValue(), dataType);
                        }
                    } else {
                        statement.setObject(i, columnVo.getValue(), dataType);
                    }
                }
                statement.addBatch();
            }
            statement.executeBatch();
        } catch (SQLException e) {
            log.error(e.getMessage());
            throw e;
        } finally {
            try {
                clickHouseConfig.closeConnection(statement, null, connection);
            } catch (SQLException e) {
                log.error(e.getMessage());
                throw e;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        ClickHouseBusiServiceImpl impl = new ClickHouseBusiServiceImpl();
        /* ExecCompareTaskReqBO execCompareTaskReqBO = new ExecCompareTaskReqBO(); */
        /*
         * StringBuilder sql = new StringBuilder(
         * "select ID,BUSI_ID,ORDER_ID,	ORDER_TYPE,PAY_ORDER_ID ,fee1,REAL_FEE,OUT_ORDER_ID,REFUND_ORDER_ID,BILL_TRANS_ID from ( ") .append(
         * "select ID,BUSI_ID,ORDER_ID,	ORDER_TYPE,PAY_ORDER_ID ,fee1,REAL_FEE,OUT_ORDER_ID,REFUND_ORDER_ID,BILL_TRANS_ID  from  ( select ID,BUSI_ID,CAST(ORDER_ID AS Int64) as ORDER_ID, TYPE_ORDER_ID as PAY_ORDER_ID,ORDER_TYPE,REAL_FEE as fee1 from myDB1.P_TRANS_PAYMENT_TEST) "
         * ) .append(" any inner join myDB1.P_PAYBILL_DAY_TEST ").append(" using ORDER_ID,PAY_ORDER_ID WHERE  ORDER_TYPE ='01'  ) a  where  a.fee1==REAL_FEE  "
         * ).append(" union ALL ") .append(
         * " select ID,BUSI_ID,ORDER_ID,	ORDER_TYPE,REFUND_ORDER_ID ,fee1,REAL_FEE,OUT_ORDER_ID,REFUND_ORDER_ID,BILL_TRANS_ID from (	 ") .append(
         * " select ID,BUSI_ID,ORDER_ID,	ORDER_TYPE,REFUND_ORDER_ID ,fee1,REAL_FEE,OUT_ORDER_ID,REFUND_ORDER_ID,BILL_TRANS_ID  from  ( select ID,BUSI_ID,CAST(ORDER_ID AS Int64) as ORDER_ID, TYPE_ORDER_ID as REFUND_ORDER_ID,ORDER_TYPE,REAL_FEE as fee1 from myDB1.P_TRANS_PAYMENT_TEST)  "
         * ) .append(" any inner join myDB1.P_PAYBILL_DAY_TEST  ").append(
         * " using ORDER_ID,REFUND_ORDER_ID WHERE  ORDER_TYPE ='02'  ) a  where  a.fee1==REAL_FEE "); System.out.println("sql=" + sql.toString());
         * execCompareTaskReqBO.setCheckSql(sql.toString()); execCompareTaskReqBO.setTargetView("P_BILL_COMPARE_SAME_TEST");
         * impl.excuteCompare(execCompareTaskReqBO);
         */

        // QueryCheckResultReqBO queryCheckResultReqBO = new QueryCheckResultReqBO();
        // queryCheckResultReqBO.setSql("select * from P_BILL_COMPARE_SAME_TEST");
        // queryCheckResultReqBO.setLimit(10);
        // queryCheckResultReqBO.setOffset(0);
        // List<ColumnVo> whereConditionList = new ArrayList<>();
        // ColumnVo columnVo;
        // for (int i = 0; i < 0; i++) {
        // columnVo = new ColumnVo();
        // columnVo.setJudgeSymbol("LIKE");
        // columnVo.setName("OUT_ORDER_ID");
        // columnVo.setSqlType(61);
        // columnVo.setValue("%66%");
        // whereConditionList.add(columnVo);
        // }
        // queryCheckResultReqBO.setWhereConditionList(whereConditionList);
        // QueryCheckResultRspBO rsp=impl.query(queryCheckResultReqBO);
        // System.out.println("rsp="+rsp);

        final StringBuilder sb = new StringBuilder("INSERT INTO ").append("t_order_source");
        sb.append(" (order_id, price, num, name) VALUES (?, ?, ?, ?)");

        List<ColumnVo> list = new ArrayList<>();
        ColumnVo columnVo = new ColumnVo();
        columnVo.setName("order_id");
        columnVo.setValue("1111");
        columnVo.setSqlType(Types.INTEGER);
        list.add(columnVo);

        ColumnVo columnVo2 = new ColumnVo();
        columnVo2.setName("price");
        columnVo2.setValue("5");
        columnVo2.setSqlType(Types.FLOAT);
        list.add(columnVo2);

        ColumnVo columnVo3 = new ColumnVo();
        columnVo3.setName("num");
        columnVo3.setValue("");
        columnVo3.setSqlType(Types.INTEGER);
        list.add(columnVo3);

        ColumnVo columnVo4 = new ColumnVo();
        columnVo4.setName("name");
        columnVo4.setValue("null");
        columnVo4.setSqlType(Types.VARCHAR);
        list.add(columnVo4);

        List<List<ColumnVo>> values = new ArrayList<>();
        values.add(list);
        System.out.println("----------------" + values.toString());
        impl.batchInsert(sb.toString(), values);
    }


    @Override
    public QueryCheckResultRspBO selectSQLByPage(QueryCheckResultReqBO queryCheckResultReqBO) {
        if (null == queryCheckResultReqBO.getSql() || queryCheckResultReqBO.getSql().isEmpty()) {
            throw new IllegalArgumentException("查询比对结果sql不能为空");
        }
        QueryCheckResultRspBO rsp = new QueryCheckResultRspBO();
        // Page<QueryCheckResultReqBO> page = new Page<QueryCheckResultReqBO>(queryCheckResultReqBO.getPageNo(), queryCheckResultReqBO.getPageSize());
        Page<QueryCheckResultReqBO> page = new Page<QueryCheckResultReqBO>();
        page.setLimit(queryCheckResultReqBO.getLimit());
        page.setOffset(queryCheckResultReqBO.getOffset());
        Map<String, String> map = new HashMap<>();
        map.put("sql", queryCheckResultReqBO.getSql());
        List<Map<String, Object>> list = this.checkResultMapper.selectSQLByPage(page, map);
        rsp.setData(list);
        rsp.setRecordsTotal(page.getTotalCount());
        rsp.setTotal(page.getTotalPages());
        rsp.setPageNo(queryCheckResultReqBO.getPageNo());
        return rsp;
    }

    public void setCheckResultMapper(CheckResultMapper checkResultMapper) {
        this.checkResultMapper = checkResultMapper;
    }

    public void setClickHouseConfig(ClickHouseConfig clickHouseConfig) {
        this.clickHouseConfig = clickHouseConfig;
    }

}
