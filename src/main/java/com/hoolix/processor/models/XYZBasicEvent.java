// package com.hoolix.processor.models;
//
// import com.fasterxml.jackson.annotation.JsonProperty;
// import lombok.AllArgsConstructor;
// import lombok.Getter;
// import org.elasticsearch.action.index.IndexRequest;
// import java.io.Serializable;
// import java.util.HashMap;
// import java.util.List;
// import java.util.Map;
//
// /**
//  * Created by peiyuchao on 2017/1/4.
//  */
// @AllArgsConstructor
// @Getter
// public class XYZBasicEvent extends Event implements Serializable {
//     private final String token;
//     private final String type;
//     private final List<String> tags;
//     private final String message;
//     @JsonProperty("upload_type")
//     private final String uploadType; // "streaming" or "file"
//     @JsonProperty("upload_timestamp")
//     private final Long uploadTimestamp;
//
//     @Override
//     public String getIndexName() {
//         return getToken() + Event.INDEX_NAME_SEPARATOR + getType();
//     }
//
//     @Override
//     public IndexRequest toIndexRequest() {
//         return new IndexRequest(getIndexName(), getType());
//     }
//
//     @Override
//     public Map<String, Object> toPayload() {
//         Map<String, Object> payload = new HashMap<>();
//         payload.put("token", token);
//         payload.put("type", type);
//         payload.put("tags", tags);
//         payload.put("message", message);
//         payload.put("upload_type", uploadType);
//         payload.put("upload_timestamp", uploadTimestamp);
//         return payload;
//     }
// }
