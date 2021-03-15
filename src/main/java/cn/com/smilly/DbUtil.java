package cn.com.smilly;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class DbUtil {

    public static final String TYPE_INSERT = "INSERT";
    public static final String TYPE_UPDATE = "UPDATE";
    public static final String TYPE_DELETE = "DELETE";

    private final static Map<String, DataSource> dataSourceMap = new HashMap<>();
    private final static DataSource dsMaster = connect(Config.PG_MASTER);

    static {
        dataSourceMap.put(Config.PG_MASTER, dsMaster);
    }

    static synchronized HikariDataSource connect(String node) {
        HikariConfig conf = new HikariConfig();
        conf.setUsername(Config.PG_USER);
        conf.setPassword(Config.PG_PASSWORD);
        conf.setJdbcUrl("jdbc:postgresql://" + node + ":5432/postgres");
        conf.setMaximumPoolSize(Config.PG_CON_NUM);
        conf.setDriverClassName("org.postgresql.Driver");
        return new HikariDataSource(conf);
    }


    private static DataSource getDataSource(String node) {
        DataSource ds = dataSourceMap.get(node);
        if (ds == null) {
            synchronized (node) {
                ds = connect(node);
                dataSourceMap.put(node, ds);
            }
        }
        return ds;
    }

    static void execute(String sql) {
        if (StringUtils.isEmpty(sql)) {
            return;
        }
        sql = StringUtils.replace(sql, "\u0000", "");
        Connection connection = null;
        Statement statement = null;
        try {
            connection = dsMaster.getConnection();
            statement = connection.createStatement();
            statement.execute(sql);
        } catch (Exception e) {
            log.error("sql execute error:{},sql:{}", e.getMessage(), sql);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {}
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {}
            }
        }
    }

    /**
     * 通过Map拼接Insert SQL语句
     *
     * @param database
     * @param tableName
     * @param dataMap
     * @return
     */
    static String genInsertSql(String database, String tableName, Map<String, String> dataMap) {
        if (StringUtils.isBlank(database) || StringUtils.isBlank(tableName) || MapUtils.isEmpty(dataMap)) {
            return null;
        }
        //生成INSERT INTO table(field1,field2) 部分
        StringBuffer sbField = new StringBuffer();
        //生成VALUES('value1','value2') 部分
        StringBuffer sbValue = new StringBuffer();

        sbField.append("INSERT INTO " + database + ".\"" + tableName.toLowerCase() + "\" (");
        for (Map.Entry<String, String> entry : dataMap.entrySet()) {
            String mapKey = entry.getKey();
            String mapValue = encrypt(database, mapKey, entry.getValue());
            sbField.append("\"" + mapKey + "\",");
            if (mapValue == null) {
                sbValue.append("null,");
            } else {
                sbValue.append("'" + mapValue + "',");
            }
        }
        String field = sbField.substring(0, sbField.length() - 1);
        String value = sbValue.substring(0, sbValue.length() - 1);
        return field + ") VALUES(" + value + ")";
    }

    //字段加密+转义
    private static String encrypt(String database, String key, String value) {
        if (value == null) {
            return null;
        }
        value = StringUtils.replace(value, "'", "''");
        if (Encrypt.needEncrypt(database, key)) {
            return Encrypt.encrypt(value);
        }else {
            return StringUtils.replace(value, "0000-00-00", "1970-01-01");
        }
    }

    /**
     * 通过Map拼接Update SQL语句
     *
     * @param database
     * @param tableName
     * @param dataMap
     * @param pkNames
     * @return String
     */
    static String genUpdateSql(String database, String tableName, Map<String, String> dataMap, List<String> pkNames) {
        if (StringUtils.isBlank(database) || StringUtils.isBlank(tableName) || MapUtils.isEmpty(dataMap) || CollectionUtils.isEmpty(pkNames)) {
            return null;
        }
        StringBuffer data = new StringBuffer();
        StringBuffer pk = new StringBuffer();
        data.append("UPDATE " + database + ".\"" + tableName.toLowerCase() + "\" SET ");
        for (Map.Entry<String, String> entry : dataMap.entrySet()) {
            String mapKey = entry.getKey();
            String mapValue = encrypt(database, mapKey, entry.getValue());
            //uuid做分发条件
            if (pkNames.contains(mapKey) || "uuid".equals(mapKey)) {
                pk.append("\"" + mapKey + "\"='" + mapValue + "' and ");
            } else {
                if (mapValue == null) {
                    data.append("\"" + mapKey + "\"=null,");
                } else {
                    data.append("\"" + mapKey + "\"='" + mapValue + "',");
                }
            }
        }

        String value = data.substring(0, data.length() - 1);
        String pkValue = pk.substring(0, pk.length() - 4);
        return String.format("%s where %s", value, pkValue);
    }


    static String genDelSql(String tableName, Map<String, String> dataMap, List<String> pkNames) {
        if (StringUtils.isBlank(tableName) || MapUtils.isEmpty(dataMap) || CollectionUtils.isEmpty(pkNames)) {
            return null;
        }
        StringBuffer pkBuffer = new StringBuffer();
        for (String pk : pkNames) {
            pkBuffer.append("\"" + pk + "\"='" + dataMap.get(pk) + "' and ");
        }
        String pkValue = pkBuffer.substring(0, pkBuffer.length() - 4);
        String v = "delete from " + tableName.toLowerCase();
        return String.format("%s where %s", v, pkValue);
    }

    public static void save(CanalData data) {
        String type = data.getType();
        String sql = null;
        if (TYPE_INSERT.equals(type)) {
            sql = DbUtil.makeInsertSql(data);
        } else if (TYPE_UPDATE.equals(type)) {
            sql = DbUtil.makeUpdateSql(data);
        } else if (TYPE_DELETE.equals(type)) {
            sql = DbUtil.makeDelSql(data);
        } else {
            log.warn("not_dml_data:{}", data);
        }
        execute(sql);
    }

    static String makeInsertSql(CanalData data) {
        if (StringUtils.isBlank(data.getDatabase()) || StringUtils.isBlank(data.getTable()) || CollectionUtils.isEmpty(data.getData())) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (Object map : data.getData()) {
            sb.append(DbUtil.genInsertSql(data.getDatabase(), data.getTable(), (Map<String, String>) map) + ";");
        }
        return sb.toString();
    }

    static String makeUpdateSql(CanalData data) {
        if (StringUtils.isBlank(data.getDatabase()) || StringUtils.isBlank(data.getTable()) || CollectionUtils.isEmpty(data.getData())) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (Object map : data.getData()) {
            sb.append(DbUtil.genUpdateSql(data.getDatabase(), data.getTable(), (Map<String, String>) map,
                    data.getPkNames()) + ";");
        }
        return sb.toString();
    }

    static String makeDelSql(CanalData data) {
        if (StringUtils.isBlank(data.getDatabase()) || StringUtils.isBlank(data.getTable()) || CollectionUtils.isEmpty(data.getData())) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (Object map : data.getData()) {
            sb.append(DbUtil.genDelSql(data.getFullTable(), (Map<String, String>) map, data.getPkNames()) + ";");
        }
        return sb.toString();
    }
}
