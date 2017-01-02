package com.hoolix.processor.models;

import org.elasticsearch.action.index.IndexRequest;

import java.util.Map;

/**
 * Hoolix 2017
 * Created by simon on 1/1/17.
 */
public interface Event {
    String getIndexName();
    String getType();
    IndexRequest toIndexRequest();
    Map<String, Object> toPayload();
}
