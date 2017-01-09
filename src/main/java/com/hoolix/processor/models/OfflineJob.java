//package com.hoolix.processor.models;
//
//import com.avaje.ebean.ExpressionList;
//import com.avaje.ebean.Model;
//import com.avaje.ebean.annotation.CreatedTimestamp;
//import com.avaje.ebean.annotation.UpdatedTimestamp;
//
//import javax.persistence.Entity;
//import javax.persistence.Id;
//import java.sql.Timestamp;
//import java.util.UUID;
//
//@Entity
//public class OfflineJob extends Model {
//    @Id
//    public UUID    uuid;
//    public UUID    user;
//    public String  name;
//    public String  type;
//    public Double  progress;
//
//    public String  status;
//
//    public String  request;
//    public String  response;
//
//    public Boolean deleted;
//
//    @CreatedTimestamp
//    public Timestamp createTime;
//    @UpdatedTimestamp
//    public Timestamp updateTime;
//
//    public static final Model.Find<String, OfflineJob> find = new Model.Find<String, OfflineJob>() {};
//
//    public static ExpressionList<OfflineJob> find_by_id(String id) {
//        return find.where().eq("uuid", id).eq("deleted", false);
//    }
//
//    public static ExpressionList<OfflineJob> find_where() {
//        return find.where().eq("deleted", false);
//    }
//}
//
