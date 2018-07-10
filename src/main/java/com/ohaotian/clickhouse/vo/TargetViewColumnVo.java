package com.ohaotian.clickhouse.vo;

import lombok.Data;

import java.io.Serializable;

/**
 * <br>
 * 标题:批量插入参数实体 <br>
 * 描述: <br>
 * 公司: www.tydic.com<br>
 *
 * @autho yudg
 * @time 2018年6月19日 下午17:32:09
 */
@Data
public class TargetViewColumnVo implements Serializable {

    private static final long serialVersionUID = -1489954090768969036L;

    /**
     * 列名
     */
    private String name;
    /**
     * 数据列类型
     */
    private String type;
}