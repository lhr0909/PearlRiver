package com.hoolix.processor.models;

import com.avaje.ebean.Model;
import com.avaje.ebean.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;

import javax.persistence.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Stream;

@Entity
@Table(name = "pipeline_config_entry")
public class PipelineDBConfigEntry extends Model{
    @Id
    public String id;
    public String type;
    public String name;
    public String description;

    //public String config_id;

    @JsonIgnore
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "config_id")
    public PipelineDBConfig config;

    public String source_field;
    public String source_option;
    public String target_option;

    public String requires;
    public String argument;
    public int    orders;


    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        builder.append("id:"+id);

        Stream<String> items = Arrays.stream(new String[]{
                "type",type,
                "name",name,
                "description",description,
                "source_field",source_field,
                "source_option",source_option,
                "target_option",target_option,
                "requires",requires,
                "argument",argument,
        });

        Iterator<String> it = items.iterator();
        while(it.hasNext()) {
            String key   = it.next();
            String value = it.next();
            if (!StringUtils.isEmpty(value)) {
                builder.append("; "+key+":"+value);
            }
        }

        return builder.toString();
    }
}

