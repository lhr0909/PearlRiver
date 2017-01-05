package com.hoolix.processor.models;

import lombok.AllArgsConstructor;
import lombok.Getter;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by peiyuchao on 2017/1/4.
 */
@AllArgsConstructor
@Getter
public class XYZLineEvent extends Event implements Serializable {
    private final String message;

    @Override
    public String getIndexName() {
        return null; // TODO
    }

    @Override
    public String getType() {
        return null; // TODO
    }

    @Override
    public Map<String, Object> toPayload() {
        Map<String, Object> payload = new HashMap<>();
        payload.put("message", message);
        return payload;
    }
}
