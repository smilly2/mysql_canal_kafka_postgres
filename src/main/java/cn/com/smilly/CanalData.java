package cn.com.smilly;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * Created by smilly on 2021-01-31
 */
@Data
//@JsonIgnoreProperties(ignoreUnknown = true)  //忽略不识别的字段
public class CanalData {
    private String database;
    private String table;
    private String type;
    private List<Object> data;
    private List<String> pkNames;
    private String sql;
    public String getFullTable(){
        return database+".\""+table+"\"";
    }
    public Map<String,String> getCommonData(){
        return  (Map<String,String>) data.get(0);
    }

    //String sql;
    //boolean isDdl;
    //String es;
    //String id;
    //Object mysqlType;
    //Object sqlType;
    //String ts;
    //Object old;

}
