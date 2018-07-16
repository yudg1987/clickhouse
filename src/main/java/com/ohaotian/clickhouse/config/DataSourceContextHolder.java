package com.ohaotian.clickhouse.config;

/**
 * @ClassName: DataSourceContextHolder
 * @Description: 数据库切换工具类
 * @author: yudg
 * @date: 2018-6-23 上午11:52:27
 */
public class DataSourceContextHolder {
	private static final ThreadLocal<String> contextHolder = new ThreadLocal<String>();

	public static void setDbType(String dbType) {
		contextHolder.set(dbType);
	}

	public static String getDbType() {
		String source= ((String) contextHolder.get());
		if (source == null) {  
			source = DataSourceType.CLICKHOUSESOURCE1;// 默认是读写库  
        }  
		return source;
	}

	public static void clearDbType() {
		contextHolder.remove();
	}
}
