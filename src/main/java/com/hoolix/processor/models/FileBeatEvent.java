package com.hoolix.processor.models;

import akka.kafka.ConsumerMessage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
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
// @NoArgsConstructor
@Getter
public class FileBeatEvent extends Event implements Serializable {
    private static final long serialVersionUID = -1061534942493817146L;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormat
            .forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
            .withLocale(Locale.ENGLISH)
            .withZone(DateTimeZone.UTC)
            .withChronology(ISOChronology.getInstanceUTC());

    private final String timestamp;

    private final Beat beat;

    private final Map<String, String> fields;

    private final String inputType;

    private final String message;

    private final long offset;

    private final String source;

    private final List<String> tags;

    private final String type;

    FileBeatEvent(ConsumerMessage.CommittableOffset committableOffset, String json) {
        super(committableOffset);
        FileBeatEvent event = null;
        JsonNode jsonNode = null;
        try {
            event = objectMapper.readValue(json, FileBeatEvent.class);

            jsonNode = objectMapper.readTree(json);

        } catch (IOException e) {
            e.printStackTrace();
        }
        this.timestamp = event.getTimestamp();
        this.beat = event.beat;
        this.fields = event.fields;
        this.inputType = event.inputType;
        this.message = event.message;
        this.offset = event.offset;
        this.source = event.source;
        this.tags = event.tags;
        this.type = event.type;

        // this.timestamp = jsonNode.get("timestamp").asText();
        // this.beat = objectMapper.readValue(jsonNode.get("beat").asText(), Beat.class);
        // this.fields = jsonNode.get("fields").;
        // this.inputType;
        // this.message;
        // this.offset;
        // this.source;
        // this.tags;
        // this.type;
    }

    // public static FileBeatEvent fromJsonString(ConsumerMessage.CommittableOffset committableOffset, String json) throws IOException {
    //     new Event()
    //
    //     FileBeatEvent event = objectMapper.readValue(json, FileBeatEvent.class);
    //     new FileBeatEvent(e)
    // }

    @Override
    public String getIndexName() {
        return null; // TODO
    }

    @Override
    public Map<String, Object> toPayload() {
        Map<String, Object> payload = new HashMap<>();
        payload.put("timestamp", dateTimeFormatter.parseMillis(timestamp));
        payload.put("beat", beat);
        payload.put("fields", fields);
        payload.put("inputType", inputType);
        payload.put("message", message);
        payload.put("offset", offset);
        payload.put("source", source);
        payload.put("tags", tags);
        payload.put("type", type);
        return payload;
    }
}
