package com.hoolix.processor.models;

import akka.kafka.ConsumerMessage;
import lombok.Getter;
import org.elasticsearch.action.index.IndexRequest;
import java.util.Map;

/**
 * Hoolix 2017
 * Created by simon on 1/1/17.
 */
@Getter
public abstract class Event {
    private static final long serialVersionUID = 667291746043562470L;

    // TODO 移至配置文件
    public static final String INDEX_NAME_SEPARATOR = ".";
    public static final String INDEX_NAME_WILDCARD = "_";

    private final ConsumerMessage.CommittableOffset committableOffset;

    Event() {
        committableOffset = null;
    }

    Event(ConsumerMessage.CommittableOffset committableOffset) {
        this.committableOffset = committableOffset;
    }

    private String getId() {
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

    public abstract String getIndexName();

    public abstract String getType();

    public IndexRequest toIndexRequest() {
        return new IndexRequest(getIndexName(), getType(), getId());
    }

    public abstract Map<String, Object> toPayload();
}
