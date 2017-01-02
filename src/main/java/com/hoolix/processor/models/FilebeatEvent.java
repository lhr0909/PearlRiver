package com.hoolix.processor.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.elasticsearch.action.index.IndexRequest;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Hoolix 2017
 * Created by simon on 1/1/17.
 */
@AllArgsConstructor
@Getter
public class FilebeatEvent implements Serializable, Event {
    private static final long serialVersionUID = -1061534942493817146L;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormat
            .forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
            .withLocale(Locale.ENGLISH)
            .withZone(DateTimeZone.UTC)
            .withChronology(ISOChronology.getInstanceUTC());

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

    @Override
    public String getIndexName() {
        return fields.getOrDefault("token", "_default_") + "." + getType();
    }
    @Override
    public IndexRequest toIndexRequest() {
        return new IndexRequest(getIndexName(), getType());
    }

    @Override
    public Map<String, Object> toPayload() {
        Map<String, Object> payload = new HashMap<>();
        payload.put("@timestamp", dateTimeFormatter.parseMillis(timestamp));
        payload.put("beat", beat);
        payload.put("fields", fields);
        payload.put("message", message);
        payload.put("offset", offset);
        payload.put("source", source);
        payload.put("tags", tags);
        payload.put("type", type);
        return payload;
    }
}
