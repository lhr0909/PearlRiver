package com.hoolix.processor.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.elasticsearch.action.index.IndexRequest;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hoolix 2017
 * Created by simon on 1/1/17.
 */
@AllArgsConstructor
@Getter
public class FilebeatEvent implements Serializable {
    private static final long serialVersionUID = -1061534942493817146L;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @JsonProperty("@timestamp")
    private final String timestamp;

    private final Beat beat;

    private final Map<String, String> fields;

    @JsonProperty("input_type")
    private final String inputType;

    private final String message;

    private final long offset;

    private final String source;

    private final List<String> tags;

    private final String type;

    public static FilebeatEvent fromJsonString(String json) throws IOException {
        return objectMapper.readValue(json, FilebeatEvent.class);
    }

    public IndexRequest toIndexRequest() {
        String index = fields.getOrDefault("token", "null") + "." + type;
        return new IndexRequest(
                index,
                type
        );
    }

    public Map<String, String> toEsPayload() {
        Map<String, String> payload = new HashMap<>();
        payload.put("message", message);
        return payload;
    }
}
