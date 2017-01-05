package com.hoolix.processor.models;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.elasticsearch.action.index.IndexRequest;
import java.io.Serializable;
import java.util.Map;

/**
 * Created by peiyuchao on 2017/1/3.
 */
@AllArgsConstructor
@Getter
public class IntermediateEvent extends Event implements Serializable {
    private final Map<String, Object> payload;

    @Override
    public String getIndexName() {
        String token = payload.get("token").toString();
        String type = getType();
        Long uploadTimestamp = ((Long) payload.get("upload_timestamp"));
        Long eventTimestamp = ((Long) payload.getOrDefault("event_timestamp", null));
        // TODO 日期轮转
        if (eventTimestamp != null) {
            return token + Event.INDEX_NAME_SEPARATOR + type + eventTimestamp + uploadTimestamp;
        } else {
            return token + Event.INDEX_NAME_SEPARATOR + type + Event.INDEX_NAME_WILDCARD + uploadTimestamp;
        }
    }

    @Override
    public String getType() {
        return payload.get("type").toString();
    }

    @Override
    public IndexRequest toIndexRequest() {
        return new IndexRequest(getIndexName(), getType());
    }

    @Override
    public Map<String, Object> toPayload() {
        return payload;
    }
}
