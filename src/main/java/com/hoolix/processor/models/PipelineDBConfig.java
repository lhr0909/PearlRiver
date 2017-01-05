package com.hoolix.processor.models;

import com.avaje.ebean.ExpressionList;
import com.avaje.ebean.Model;

import javax.persistence.*;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "pipeline_config")
public class PipelineDBConfig extends Model{

    public static final Model.Find<String, PipelineDBConfig> find = new Model.Find<String, PipelineDBConfig>() {};

    public static ExpressionList<PipelineDBConfig>  find_by_user(String user_id) {
        return find_by_user(user_id, 0);
    }

    public static ExpressionList<PipelineDBConfig>  find_by_user(String user_id, int max) {

        ExpressionList<PipelineDBConfig> query = null;
        if (max > 0)
            query = find.setMaxRows(max).where();
        else
            query = find.where();

        return query.eq("user_id", user_id).eq("is_delete", false);
    }

    @Id
    public String id;
    public String name;
    public String description;

    public String user_id;

    public String message_type; //type in message

    public String options;

    public long   version; //atomic increase

    @OneToMany(mappedBy = "config")
    public List<PipelineDBConfigEntry> entries = new ArrayList<>();

    public Timestamp enter_time;
    public Timestamp leave_time;


    public Boolean is_delete;


    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        builder.append("id:"+id);

        builder.append("; name:"+name);
        builder.append("; description:"+ description);
        builder.append("; user_id:"    + user_id);
        builder.append("; message_type:"+ message_type);
        builder.append("; version:"+ version);
        builder.append("; entryes:{"+ entries.stream().map(e->e.toString()).reduce((e1,e2)->e1+" "+e2)+"}");

        return builder.toString();
    }

}

