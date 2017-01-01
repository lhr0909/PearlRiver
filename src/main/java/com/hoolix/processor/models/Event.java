package com.hoolix.processor.models;

import org.elasticsearch.action.index.IndexRequest;

import java.text.ParseException;
import java.util.Map;

/**
 * Hoolix 2017
 * Created by simon on 1/1/17.
 */
public interface Event {
    String getType();
    IndexRequest toIndexRequest();
    Map<String, Object> toPayload();
}
