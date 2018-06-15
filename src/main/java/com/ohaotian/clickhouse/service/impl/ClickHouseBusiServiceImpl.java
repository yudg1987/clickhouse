package com.ohaotian.clickhouse.service.impl;

import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

        ClickHouseConnection clickHouseConnection = clickHouseConfig.clickHouseConnection();

        DatabaseMetaData dbmd = clickHouseConnection.getMetaData();

        String defalutCatalog = clickHouseConnection.getCatalog();

        String defaultSchema = clickHouseConnection.getSchema();

        ResultSet columnsResultSet = dbmd.getColumns(defalutCatalog, defaultSchema, targetView, "%");

        ResultSet resultSet = clickHouseConnection.createStatement().executeQuery(checkSql);

        StringBuilder colomnSQL = new StringBuilder("INSERT INTO ").append(targetView).append(" (");
        StringBuilder valuesSQL = new StringBuilder(" VALUES (");

        int columnCount = 0;
        List<TargetViewBO> targetViewBOs = new ArrayList<TargetViewBO>();
        while (columnsResultSet.next()) {
            columnCount++;
            TargetViewBO bo = new TargetViewBO();
            bo.setColumnName(columnsResultSet.getString("COLUMN_NAME"));
            bo.setDataType(columnsResultSet.getInt("DATA_TYPE"));
            bo.setNullable(columnsResultSet.getBoolean("NULLABLE"));
            targetViewBOs.add(bo);
        }
        columnsResultSet.close();
        log.debug("columnCount=" + columnCount);
        log.debug("targetViewBOs=" + targetViewBOs);
        int pos = 0;
        for (int i = 0; i < columnCount; i++) {
            String columnName = targetViewBOs.get(i).getColumnName();
            if (pos == columnCount - 1) {
                colomnSQL.append(columnName).append(")");
                valuesSQL.append("? ) ");
            } else {
                colomnSQL.append(columnName).append(",");
                valuesSQL.append("? , ");
            }
            pos++;
        }
        String insertSQL = colomnSQL.toString() + valuesSQL.toString();
        log.debug("sql=" + insertSQL);

        PreparedStatement preparedStatement = clickHouseConnection.prepareStatement(insertSQL);

        while (resultSet.next()) {
            for (int i = 0; i < columnCount; i++) {
                TargetViewBO bo = targetViewBOs.get(i);
                String columnName = bo.getColumnName();
                Object value;
                value = null;
                Integer dataType = bo.getDataType();
                try {
                    value = resultSet.getObject(columnName);
                } catch (Exception e) {
                    preparedStatement.setObject(i + 1, null, Types.VARCHAR);
                    if (dataType == 91) {// DATE类型
                        preparedStatement.setDate(i + 1, new Date(0L));
                    } else if (dataType == 92 || dataType == 93) {// timestamp datetime
                        preparedStatement.setTimestamp(i + 1, new Timestamp(0L));
                    } else if (dataType == -5) {// Int64
                        preparedStatement.setObject(i + 1, 0, Types.INTEGER);
                    } else {
                        preparedStatement.setObject(i + 1, value, dataType);
                    }
                    continue;
                }
                if (value == null) {
                    if (dataType == 91) {// DATE类型
                        preparedStatement.setDate(i + 1, new Date(0L));
                    } else if (dataType == 92 || dataType == 93) {// timestamp datetime
                        // preparedStatement.setTimestamp(i + 1, new Timestamp(0L));
                        preparedStatement.setObject(i + 1, null, Types.TIMESTAMP);
                    } else if (dataType == -5) {// Int64
                        preparedStatement.setObject(i + 1, 0, Types.INTEGER);
                    } else {
                        preparedStatement.setObject(i + 1, value, dataType);
                    }
                } else {
                    preparedStatement.setObject(i + 1, value, dataType);
                }

            }
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();
        preparedStatement.close();
        resultSet.close();
        clickHouseConnection.close();
        ExecCompareTaskRspBO execCompareTaskRspBO = new ExecCompareTaskRspBO();
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

        StringBuilder sb = new StringBuilder("INSERT INTO ").append("t_order_source");
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
    public QueryCheckResultRspBO query(QueryCheckResultReqBO queryCheckResultReqBO) throws Exception {
        QueryCheckResultRspBO rsp = new QueryCheckResultRspBO();
        ClickHouseConnection clickHouseConnection = clickHouseConfig.clickHouseConnection();
        MySql5Dialect dialect = new MySql5Dialect();
        String querySQL = queryCheckResultReqBO.getSql();
        if (null == querySQL || querySQL.isEmpty()) {
            throw new IllegalArgumentException("查询比对结果sql不能为空");
        }
        querySQL = querySQL + intallWhereCondition(queryCheckResultReqBO);
        int offset = queryCheckResultReqBO.getOffset();
        int limit = queryCheckResultReqBO.getLimit();
        String countSQL = dialect.getCountString(querySQL);
        log.debug("countSQL=" + countSQL);
        ResultSet rs = clickHouseConnection.createStatement().executeQuery(countSQL);
        int total = 0;
        while (rs.next()) {
            total = rs.getInt(1);
        }
        rs.close();
        rsp.setRecordsTotal(total);

        log.debug("total=" + total);
        String limitSQL = null;
        if (limit > 0) {
            limitSQL = dialect.getLimitString(querySQL, offset, limit);
        } else {// 不分页情况
            limitSQL = querySQL;
            limit = total;
        }
        int mod = total % limit;
        int pageTotal = total / limit;
        rsp.setTotal(mod > 0 ? (pageTotal + 1) : pageTotal);

        log.debug("limitSQL=" + limitSQL);

        ClickHouseResultSet clickHouseResultSet = (ClickHouseResultSet) clickHouseConnection.createStatement().executeQuery(limitSQL);
        String columns[] = clickHouseResultSet.getColumnNames();
        List<Map<String, Object>> datas = new ArrayList<>();
        while (clickHouseResultSet.next()) {
            Map<String, Object> map = new HashMap<>();
            for (String column : columns) {
                map.put(column, clickHouseResultSet.getObject(column));
            }
            datas.add(map);
        }
        rsp.setData(datas);
        clickHouseResultSet.close();
        clickHouseConnection.close();
        return rsp;
    }

    private StringBuilder intallWhereCondition(QueryCheckResultReqBO queryCheckResultReqBO) {
        StringBuilder whereSQL = new StringBuilder(" where 1=1 ");
        List<ColumnVo> whereConditionList = queryCheckResultReqBO.getWhereConditionList();
        if (whereConditionList != null && whereConditionList.size() > 0) {
            for (ColumnVo vo : whereConditionList) {
                int sqlType = vo.getSqlType();
                whereSQL.append(" and ").append(vo.getName() + " ").append(vo.getJudgeSymbol() + " ");
                if (sqlType == -5) {
                    whereSQL.append(vo.getValue());
                } else {
                    whereSQL.append("'" + vo.getValue() + "'");
                }

            }
        }

        return whereSQL;

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