package com.glodon.config

import java.sql.{Connection, DriverManager, Statement}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat


case class TaskConifg(id: String, paramKey: String, paramValue: String, project: String)

class Config (val params: List[TaskConifg]) {
  def sparkMaster: String = params.filter(t => t.paramKey == "spark_master").head.paramValue
  def userLogParquet: String = params.filter(t => t.paramKey == "parquet_root_dir").head.paramValue
  def startDate: Int = params.filter(t => t.paramKey == "start_date").head.paramValue.toInt
  def endDate: Int ={
    val tmp  = params.filter(t => t.paramKey == "end_date").head.paramValue.toInt
    DateTime.parse(tmp.toString, DateTimeFormat.forPattern("yyyyMMdd")).plusDays(1).toString("yyyyMMdd").toInt
  }
  def stopMonthDay: Int = {
    var stopDate  = params.filter(t => t.paramKey == "stop_date").head.paramValue.toInt
    if (stopDate == 0){
      var dt:DateTime = new DateTime()
      var yesterday:DateTime = dt.minusDays(1)
      yesterday.toString("yyyyMMdd").toInt
    }else{
      stopDate
    }
  }
  def gidLockParquet: String = params.filter(t => t.paramKey == "gid_lock_parquet").head.paramValue

  def gidParquet: String = params.filter(t => t.paramKey == "gid_parquet").head.paramValue

  def lockParquet: String = params.filter(t => t.paramKey == "lock_parquet").head.paramValue

  def projectParquet: String = params.filter(t => t.paramKey == "project_parquet").head.paramValue

  def dimProjectParquet: String = params.filter(t => t.paramKey == "dim_project_parquet").head.paramValue

  def dimUserParquet: String = params.filter(t => t.paramKey == "dimuser_parquet").head.paramValue

}

object Config {

  lazy val cf = new Config(initParam)

  def initParam: List[TaskConifg] = {
    val sql =
      """
    select * from sys_task_config_b
    where  (id = 'common' or id ='project-from-action')
    and  project='io_batch' """
    println(sql)
    var params: List[TaskConifg] = List[TaskConifg]()
    val conn = DriverManager.getConnection(Constants.MART_JDBC_URL)
    try {
      val rs = conn.prepareStatement(sql).executeQuery()
      while (rs.next()) {
        val tk = new TaskConifg(rs.getString("id"), rs.getString("param_key"), rs.getString("param_value"), rs.getString("project"))
        params = params :+ tk
      }
    } finally {
      conn.close()
    }
    params
  }

  def udpateDate(start : Int , end : Int) = {
    lazy val conn: Connection  = DriverManager.getConnection(Constants.MART_JDBC_URL)
    conn.setAutoCommit(false)
    lazy val ps : Statement = conn.createStatement()
    try{
      var sql = s"update sys_task_config_b set param_value='${start}' where id ='project-from-action' and param_key='start_date' and project='io_batch' "
      ps.addBatch(sql)
      sql = s"update sys_task_config_b set param_value='${end}' where id ='project-from-action' and param_key='end_date' and project='io_batch' "
      ps.addBatch(sql)
      ps.executeBatch()
      conn.commit()
    }catch {
      case e:Exception=>e.printStackTrace()
    }finally {
      if(ps != null) ps.close()
      if(conn != null) conn.close()
    }
  }

}
