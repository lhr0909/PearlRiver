package com.hoolix.processor.models;

import java.util.Map;

/**
 * Hoolix 2017
 * Created by simon on 1/1/17.
 */
public interface Event {
    Map<String, Object> toPayload();
}
