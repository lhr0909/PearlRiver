package com.hoolix.processor.models;

import akka.kafka.ConsumerMessage.CommittableOffset;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.elasticsearch.action.index.IndexRequest;

import java.io.Serializable;
import java.util.Map;

/**
 * Hoolix 2017
 * Created by simon on 1/2/17.
 */
@AllArgsConstructor
@Getter
public class KafkaEvent implements Serializable, Event, ESSink {
    private static final long serialVersionUID = 667291746043562470L;

    private final CommittableOffset committableOffset;
    private final Event event;

    @Override
    public String getIndexName() {
        return "kafka." + getTopic() + "." + event.getIndexName();
    }

    @Override
    public String getType() {
        return "kafka_" + event.getType();
    }

    @Override
    public IndexRequest toIndexRequest() {
        return new IndexRequest(getIndexName(), getType(), getId());
    }

    @Override
    public Map<String, Object> toPayload() {
        return event.toPayload();
    }

    public String getId() {
        return getTopic() + "." + Integer.toString(getPartition()) + "." + Long.toString(getOffset());
    }

    private String getTopic() {
        return committableOffset.partitionOffset().key().topic();
    }

    private int getPartition() {
        return committableOffset.partitionOffset().key().partition();
    }

    private long getOffset() {
        return committableOffset.partitionOffset().offset();
    }
}
