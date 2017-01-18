package com.hoolix.processor.models

import slick.driver.MySQLDriver.api._
import java.sql.Timestamp

/**
  * Created by peiyuchao on 2017/1/12.
  */
class User(tag: Tag) extends Table[(String, String, String, String, String, String, Timestamp, String, Timestamp, Boolean, Boolean, String, Long, Int, Long, Long, Long, Long)](tag, "User") {
  // TODO http://slick.lightbend.com/doc/3.1.1/code-generation.html
  def uuid = column[String]("uuid", O.PrimaryKey)
  def name = column[String]("name")
  def email = column[String]("email")
  def password  = column[String]("password")
  def phone = column[String]("phone")
  def company = column[String]("company")
  def loginTime = column[Timestamp]("login_time")
  def userRoleUuid = column[String]("user_role_uuid")
  def createTime = column[Timestamp]("create_time")
  def isDeleted = column[Boolean]("is_deleted")
  def stop = column[Boolean]("stop")
  def owner = column[String]("owner")
  def limits = column[Long]("limits")
  def retention = column[Int]("retention")
  def totalUsage = column[Long]("total_usage")
  def currentUsage = column[Long]("current_usage")
  def totalDrop = column[Long]("total_drop")
  def currentDrop = column[Long]("current_drop")
  def * = (uuid, name, email, password, phone, company, loginTime, userRoleUuid, createTime, isDeleted, stop, owner, limits, retention, totalUsage, currentUsage, totalDrop, currentDrop)
}
