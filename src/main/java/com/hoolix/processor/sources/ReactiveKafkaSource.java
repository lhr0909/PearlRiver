package com.hoolix.processor.sources;

import akka.stream.Attributes;
import akka.stream.SourceShape;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Hoolix 2017
 * Created by simon on 1/16/17.
 */
public class ReactiveKafkaSource<K, V> extends GraphStage<SourceShape<ConsumerRecord<K, V>>> {
    private final KafkaConsumer<K, V> kafkaConsumer;

    public ReactiveKafkaSource(KafkaConsumer<K, V> consumer) {
        kafkaConsumer = consumer;
    }

    @Override
    public SourceShape<ConsumerRecord<K, V>> shape() {
        return null;
    }

    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) throws Exception {
        return null;
    }
}
