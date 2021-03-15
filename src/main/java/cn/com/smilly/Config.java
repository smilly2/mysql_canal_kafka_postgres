package cn.com.smilly;

import java.util.Arrays;
import java.util.List;

public class Config {

    //kafka
    public static final String KAFKA_SERVER = "127.0.0.1:9092";
    public static List<String> KAFKA_TOPICS = Arrays.asList("test","test1");

    //postgres
    public static final String PG_MASTER = "10.27.0.1";
    public static final String PG_USER = "postgres";
    public static final String PG_PASSWORD = "postgres";
    public static final int PG_CON_NUM = 5;

	//checkpoint redis key 
    public static final String CHECKPOINT_KEY = "kafka_pg_sync_checkpoint";

    public final static String ENCRYPT_KEY = "";
}
