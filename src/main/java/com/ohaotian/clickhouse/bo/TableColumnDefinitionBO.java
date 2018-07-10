package com.ohaotian.clickhouse.bo;

import java.io.Serializable;

import lombok.Data;

@Data
public class TableColumnDefinitionBO implements Serializable {

	/**  */
	private static final long serialVersionUID = 7829670947801153370L;
	private String	column_code;
	private String	column_type;
	private Integer	is_must;
	private Integer	column_length;
	private String	column_name;
	private String	field_notes;
	private Integer	is_distributed;
	private String	seqnum;
	private Integer isPrimary;

}
