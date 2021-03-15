package cn.com.smilly;

/**
 * Created by smilly on 2021-01-29
 */
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.Map;

@Slf4j
public class RedisUtil {
    
    
    private static JedisPool pool = new JedisPool("localhost",6379);
    public static JedisPool getPool() {
        if(pool==null){
            synchronized (RedisUtil.class) {
                pool = new JedisPool("localhost", 6379);
            }
        }
        return pool;
    }

    public static String set(String key, String value) {
        Jedis jedis = getPool().getResource();
        String var = null;
        try {
            var = jedis.set(key, value);
        }catch (Exception e){
            log.error(e.toString());
        }finally {
            jedis.close();
        }
        return var;
    }

    public static String set(String key, String value, int expire) {
        Jedis jedis = getPool().getResource();
        String var = null;
        try {
            var = jedis.setex(key, expire, value);//jedis.set(key, value);
        }catch (Exception e){
            log.error(e.toString());
        }finally {
            jedis.close();
        }
        return var;
    }

    public static Long expire(String key, int sec) {
        Jedis jedis = getPool().getResource();
        Long var = null;
        try {
            var = jedis.expire(key, sec);//jedis.set(key, value);
        }catch (Exception e){
            log.error(e.toString());
        }finally {
            jedis.close();
        }
        return var;
    }

    public static Map<String, String> hGetAll(String key) {
        Jedis jedis = getPool().getResource();
        Map var = null;
        try {
            var = jedis.hgetAll(key);
        }catch (Exception e){
            log.error(e.toString());
        }finally {
            jedis.close();
        }
        return var;
    }
    public static String hGet(String key,String field) {
        Jedis jedis = getPool().getResource();
        String var = null;
        try {
            var = jedis.hget(key,field);
        }catch (Exception e){
            log.error(e.toString());
        }finally {
            jedis.close();
        }
        return var;
    }

    public static String get(String key) {
        Jedis jedis = getPool().getResource();
        String var = null;
        try {
            var = jedis.get(key);
        }catch (Exception e){
            log.error(e.toString());
        }finally {
            jedis.close();
        }
        return var;
    }

    public static void hSet(String key, Map<String, String> map) {
        Jedis jedis = getPool().getResource();
        try {
            jedis.hmset(key, map);
        }catch (Exception e){
            log.error(e.toString());
        }finally {
            jedis.close();
        }

    }

    public static void lPush(String key, String value) {
        Jedis jedis = getPool().getResource();
        try {
            jedis.lpush(key, new String[]{value});
        }catch (Exception e){
            log.error(e.toString());
        }finally {
            jedis.close();
        }
    }
    public static String lPop(String key) {
        Jedis jedis = getPool().getResource();
        String var = null;
        try {
            var = jedis.lpop(key);
        }catch (Exception e){
            log.error(e.toString());
        }finally {
            jedis.close();
        }
        return  var;
    }

    public static List<String> lRang(String key, long start, long end) {
        Jedis jedis = getPool().getResource();
        List var =null;
        try {
            var = jedis.lrange(key, start, end);
        }catch (Exception e){
            log.error(e.toString());
        }finally {
            jedis.close();
        }
        return var;
    }

    public static void close() {
        pool.close();
    }
}
