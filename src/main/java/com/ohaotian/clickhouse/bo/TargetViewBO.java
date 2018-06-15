package com.ohaotian.clickhouse.bo;

import java.io.Serializable;

import lombok.Data;

@Data
public class TargetViewBO implements Serializable {

	/**  */
	private static final long serialVersionUID = -3565984749572178468L;
	private String			  columnName;
	private Integer			  dataType;
	private boolean			  nullable;

}
