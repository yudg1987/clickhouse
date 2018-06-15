package com.ohaotian.clickhouse.bo;

import java.util.Date;

import lombok.Data;

@Data
public class RechangeResultAbundanceRspBO {

	private Long	withholdingPaymentId;

	private Long	withholdingOrderId;

	private String	serialNumber;

	private String	fee;

	private String	realFee;

	private String	userId;

	private String	acntCode;

	private Date	rechargeTime;

	private String	chanType;

	private String	chargeParty;

	private String	accessType;

	private String	paymentType;

	private String	npTag;

	private String	provinceCode;

	private String	operatorId;

	private String	cityCode;

	private String	channelId;

	private String	channelType;

	private String	eparchyCode;

	private String	serviceClassCode;

	private String	operType;

	private String	pointDeductNumber;

	private Integer	reconciliationType;

	private String	pointFee;

	private String	pointDeductId;

	private String	serviceType;

	private String	pointChanType;

	private String	consumeType;
}
