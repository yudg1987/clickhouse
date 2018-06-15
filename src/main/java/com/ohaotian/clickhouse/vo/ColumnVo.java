package com.ohaotian.clickhouse.vo;

import lombok.Data;

import java.io.Serializable;

/**
 *
 * <br>
 * 标题:批量插入参数实体 <br>
 * 描述: <br>
 * 公司: www.tydic.com<br>
 * @autho zhoubang
 * @time 2018年6月6日 上午10:01:09
 */
@Data
public class ColumnVo implements Serializable {

    private static final long serialVersionUID = -1489954090768969036L;

    /**
     * 列名
     */
    private String name;
    /**
     * 列值
     */
    private String value;
    /**
     * 数据列类型
     */
    private int sqlType;
    /**判断条件 默认=  > < >= <= != **/
    private String judgeSymbol="=";

}
