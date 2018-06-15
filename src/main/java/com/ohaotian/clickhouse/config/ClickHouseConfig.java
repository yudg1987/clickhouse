package com.ohaotian.clickhouse.config;

import lombok.Data;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseConnectionSettings;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

/**
 * <br>
 * 标题: ClickHouse配置信息 <br>
 * 描述: <br>
 * 公司: www.tydic.com<br>
 *
 * @autho zhoubang
 * @time 2018年6月6日 上午10:01:37
 */
@Data
public class ClickHouseConfig {
    private static Properties properties;

    public void setProperties(Properties properties) {
        ClickHouseConfig.properties = properties;
    }

    /**
     * 数据库连接字符串
     */
    private static String dataSourceUrl = null;
    /**
     * 数据库名称(默认值:null)
     */
    private static String database = (String) ClickHouseQueryParam.DATABASE.getDefaultValue();
    /**
     * 数据库用户名(默认值:null)
     */
    private static String user = (String) ClickHouseQueryParam.USER.getDefaultValue();
    /**
     * 数据库密码(默认值:null)
     */
    private static String password = (String) ClickHouseQueryParam.PASSWORD.getDefaultValue();
    /**
     * 数据库连接超时时间(单位:毫秒)(默认值:10*1000)
     */
    private static int connectionTimeout = (int) ClickHouseConnectionSettings.CONNECTION_TIMEOUT.getDefaultValue();
    /**
     * 是否压缩传输数据(默认值:false)
     */
    private static boolean decompress = (boolean) ClickHouseQueryParam.DECOMPRESS.getDefaultValue();
    /**
     * 单位buffer内最大压缩(默认值: 1024*1024)
     */
    private static int maxCompressBufferSize = (int) ClickHouseConnectionSettings.MAX_COMPRESS_BUFFER_SIZE.getDefaultValue();

    private ClickHouseProperties clickHouseProperties() {
        final ClickHouseProperties clickHouseProperties = new ClickHouseProperties();
        dataSourceUrl = properties.getProperty("com.ohaotian.clickhouse.dataSourceUrl");
        connectionTimeout = Integer.valueOf(properties.getProperty("com.ohaotian.clickhouse.connectionTimeout"));
        database = properties.getProperty("com.ohaotian.clickhouse.database");
        user = properties.getProperty("com.ohaotian.clickhouse.user");
        password = properties.getProperty("com.ohaotian.clickhouse.password");
        decompress = Boolean.valueOf(properties.getProperty("com.ohaotian.clickhouse.decompress"));
        maxCompressBufferSize = Integer.valueOf(properties.getProperty("com.ohaotian.clickhouse.maxCompressBufferSize"));
        // 连接超时（毫秒）
        clickHouseProperties.setConnectionTimeout(connectionTimeout);
        clickHouseProperties.setDatabase(database);
        // 设置用户名（本单元测试用户和密码采用clickhouse默认值）
        clickHouseProperties.setUser(user);
        // 设置密码
        clickHouseProperties.setPassword(password);
        /** 下面两个properties用于TabSeparated格式批量插入（有无它插入效率很明显） */
        // 压缩传输数据
        clickHouseProperties.setDecompress(decompress);
        // 单位buffer内最大压缩
        clickHouseProperties.setMaxCompressBufferSize(maxCompressBufferSize);
        return clickHouseProperties;
    }


    /**
     * 获取连接
     *
     * @return
     * @throws Exception
     */
    public ClickHouseConnection clickHouseConnection() throws Exception {
        ClickHouseProperties properties = clickHouseProperties();
        ClickHouseDataSource clickHouseDataSource = new ClickHouseDataSource(dataSourceUrl, properties);
        return clickHouseDataSource.getConnection();
    }

    /**
     * 将连接释放到连接池中.
     *
     * @param stmt
     * @param rs
     * @param connection
     */
    public void closeConnection(Statement stmt, ResultSet rs, Connection connection) throws SQLException {
        if (rs != null && !rs.isClosed()) {
            rs.close();
        }
        if (stmt != null && !stmt.isClosed()) {
            stmt.close();
        }
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }


}
