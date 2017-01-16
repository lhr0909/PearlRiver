package com.hoolix.processor.sinks;

import akka.stream.Attributes;
import akka.stream.SinkShape;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * Hoolix 2017
 * Created by simon on 1/17/17.
 */
public class ReactiveKafkaSink<K, V> extends GraphStage<SinkShape<Map<TopicPartition, OffsetAndMetadata>>> {
    private final KafkaConsumer<K, V> kafkaConsumer;

    public ReactiveKafkaSink(KafkaConsumer<K, V> consumer) {
        kafkaConsumer = consumer;
    }

    @Override
    public SinkShape<Map<TopicPartition, OffsetAndMetadata>> shape() {
        return null;
    }

    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) throws Exception {
        return null;
    }
}
