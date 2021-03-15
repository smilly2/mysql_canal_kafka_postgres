package cn.com.smilly;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@Slf4j
public class AppMain {
    public static void main(String[] args) {
        //每个topic创建一个线程去消费一个topic，单线程写入保证单个数据库写入顺序
        for (String topic : Config.KAFKA_TOPICS) {
            Consumer consumer = new Consumer(topic);
            new Thread(consumer).start();
        }
    }

}

class Consumer implements Runnable{
    private static Logger log = LoggerFactory.getLogger(Consumer.class);
    private static Properties props;
    KafkaConsumer kafkaConsumer;
    static {
        props = new Properties();
        props.put("bootstrap.servers", Config.KAFKA_SERVER);
        // 消费者组名称
        props.put("group.id", "GID-postgres");
        // 设置 enable.auto.commit,偏移量由 auto.commit.interval.ms 控制自动提交的频率。
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }

    Consumer(String topic){

        log.info("init Consumer "+topic);

        kafkaConsumer = new KafkaConsumer<>(props);
        // 指定订阅 topic 名称
        kafkaConsumer.subscribe(Arrays.asList(topic));

        String offset = RedisUtil.hGet(Config.CHECKPOINT_KEY,topic);

        log.info(topic+" offset:"+offset);

        if(offset!=null) {
            Set<TopicPartition> assignment = new HashSet<>();
            while (assignment.size() == 0) {
                kafkaConsumer.poll(100L);
                assignment = kafkaConsumer.assignment();
            }
            for (TopicPartition tp : assignment) {
                kafkaConsumer.seek(tp, Long.valueOf(offset));
            }
        }
    }

    public void run() {
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(200);
            for (ConsumerRecord<String, String> record : records){
                PostgresSink.save(record);
            }
        }
    }
}

