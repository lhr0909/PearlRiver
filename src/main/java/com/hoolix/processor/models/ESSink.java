package com.hoolix.processor.models;

import org.elasticsearch.action.index.IndexRequest;

/**
 * Created by peiyuchao on 2017/1/4.
 */
public interface ESSink {
    String INDEX_NAME_SEPARATOR = ".";
    String INDEX_NAME_WILDCARD = "_";
    String getIndexName();
    String getType();
    IndexRequest toIndexRequest();
}
