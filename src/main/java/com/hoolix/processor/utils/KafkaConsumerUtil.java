package com.hoolix.processor.utils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Hoolix 2017
 * Created by simon on 1/16/17.
 */
public class KafkaConsumerUtil {
    private static Map<Set<String>, KafkaConsumer<String, String>> sharedConsumerInstanceMap = new ConcurrentHashMap<>();

    private static Map<String, Object> configMap = new HashMap<>();

    public static KafkaConsumer<String, String> getInstance(Set<String> topics) {
        Set<String> frozenTopics = Collections.unmodifiableSet(topics);

        if (!sharedConsumerInstanceMap.containsKey(frozenTopics)) {
            synchronized (KafkaConsumerUtil.class) {
                if (!sharedConsumerInstanceMap.containsKey(frozenTopics)) {
                    if (configMap.isEmpty()) {
                        readConfig();
                    }

                    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configMap, new StringDeserializer(), new StringDeserializer());
                    consumer.subscribe(topics);

                    sharedConsumerInstanceMap.put(frozenTopics, consumer);
                }
            }
        }

        return sharedConsumerInstanceMap.get(frozenTopics);
    }

    private static void readConfig() {
        Config config = ConfigFactory.parseFile(new File("conf/application.conf"));
        Config kafkaConfig = config.getConfig("kafka");

        for (Map.Entry<String, ConfigValue> entry : kafkaConfig.entrySet()) {
            configMap.put(entry.getKey(), kafkaConfig.getString(entry.getKey()));
        }
    }
}
