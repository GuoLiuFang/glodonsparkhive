package com.glodon

import java.sql._

import com.glodon.config.{Config, Constants}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import scala.sys.process._


object Main {

  val conf = new SparkConf().setAppName("project-from-action")
  implicit val sc = new SparkContext(conf)
  implicit val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  val parquet_userlog_path = "/glodon/layer2_wide_table/parquet_userlog"
  val projectParquetPath = "/glodon/apps/public/fact_project_by_product_lock_a/"
  val projectParquetPathTemp = "/glodon/apps/public/fact_project_by_product_lock_a_temp/"
  val dimProjectParquetPath = "/glodon/apps/public/dim_project_from_action_a/"

  def projectCompute(baseDataDf: DataFrame) = {
    //allProject 基础数据
    sqlContext.read.load(projectParquetPath).registerTempTable("allProject")

    //dayProject
    baseDataDf.where("(projectid <> 'N/A') or (prjname <> 'N/A') or (prjfullpath<>'N/A')").
      select("pcode", "gid", "dognum", "ver", "projectid", "prjname", "prjfullpath", "prjcost", "prjsize", "major", "duration", "utype", "receivetime").
      na.fill(0).na.fill("N/A").na.replace("pcode" :: "gid" :: "dognum" :: "ver" :: "projectid" :: "prjname" :: "prjfullpath" :: "prjcost" :: "prjsize" :: "major" :: "duration" :: "utype" :: "receivetime" :: Nil, Map("" -> "N/A")).coalesce(200).
      map(r => {
        var prjname: String = r.getAs[String]("prjname")
        var prjfullpath: String = r.getAs[String]("prjfullpath")
        if ((prjname == null || prjname == "N/A") && (prjfullpath != null && prjfullpath != "N/A")) {
          prjname = if (prjfullpath.indexOf("\\") > 0) prjfullpath.split("\\\\").last else prjfullpath
          prjname = if (prjname.indexOf("/") > 0) prjname.split("/").last else prjname
          if (prjname.indexOf("\\") > 0) {
            prjname = prjname.replace("\\", "")
          }
        }
        var key = r.getAs[String]("pcode") + r.getAs[String]("dognum") + r.getAs[String]("projectid") + prjname
        key = java.security.MessageDigest.getInstance("MD5").digest(key.getBytes()).map(0xFF & _).map {"%02x".format(_)}.foldLeft("") {_ + _}
        val vv = new scala.Array[String](17)
        vv(0) = r.getAs[String]("pcode")
        vv(1) = r.getAs[String]("gid")
        vv(2) = r.getAs[String]("dognum")
        vv(3) = r.getAs[String]("ver")
        vv(4) = r.getAs[String]("projectid")
        vv(5) = prjname
        vv(6) = r.getAs[String]("prjfullpath")
        vv(7) = if (r.getAs[String]("prjcost") == "N/A") "0" else r.getAs[String]("prjcost")
        vv(8) = if (r.getAs[String]("prjsize") == "N/A") "0" else r.getAs[String]("prjsize")
        vv(9) = r.getAs[String]("major")
        vv(10) = if (r.getAs[String]("duration") == "N/A") "0" else r.getAs[String]("duration")
        vv(11) = r.getAs[String]("utype")
        vv(12) = r.getAs[String]("receivetime")
        vv(13) = vv(12) // first_open_datetime
        vv(14) = vv(12) //last_close_datetime
        vv(15) = vv(10) //total_duration
        vv(16) = vv(10) //last_long
        (key, vv)
      }).reduceByKey((v1, v2) => {
      v1(13) = if (v1(13) < v2(13)) v1(13) else v2(13) //first_open_datetime
      v1(14) = if (v1(14) > v2(14)) v1(14) else v2(14) //last_close_datetime
      val v1_date_str = v1(12)
      val v2_date_str = v2(12)
      if (v1_date_str < v2_date_str) {
        if (v2(0) != "N/A") v1(0) = v2(0) //pocde
        if (v2(1) != "N/A") v1(1) = v2(1) //gid
        if (v2(2) != "N/A") v1(2) = v2(2) //dognum
        if (v2(3) != "N/A") v1(3) = v2(3) //ver
        if (v2(4) != "N/A") v1(4) = v2(4) //projectid
        if (v2(5) != "N/A") v1(5) = v2(5) //prjname
        if (v2(6) != "N/A") v1(6) = v2(6) //prjfullpath
        if (v2(7) != "0") v1(7) = v2(7) //prjcost
        if (v2(8) != "0") v1(8) = v2(8) //prjsize
        if (v2(9) != "N/A") v1(9) = v2(9) //major
        if (v2(10) != "0") v1(10) = v2(10) //duration
        if (v2(11) != "N/A") v1(11) = v2(11) //utype
        v1(12) = v2(12) //receivetime
        if (v2(15) != "0") v1(15) = v2(15) //total_duration
        if (v2(16) != "0") v1(16) = v2(16) //last_long
      }
      v1(15) = (v1(15).toDouble + v2(15).toDouble).toString
      if(v1(1)=="N/A"){
        v1(1) = "N/A" + (new util.Random).nextInt(100).toString
      }
      v1
    }).coalesce(200).
      map(row => (row._1, (row._2) (0), (row._2) (1), (row._2) (2), (row._2) (3), (row._2) (4), (row._2) (5), (row._2) (6), (row._2) (7), (row._2) (8), (row._2) (9), (row._2) (10), (row._2) (11), (row._2) (12), (row._2) (13), (row._2) (14), (row._2) (15), (row._2) (16))).
      toDF("pId", "pcode", "gid", "dognum", "ver", "projectid", "prjname", "prjfullpath", "prjcost", "prjsize", "major", "duration", "utype", "receivetime", "first_open_datetime", "last_close_datetime", "total_duration", "last_long").registerTempTable("dayProject")

    //增量
    //dim_user from hdfs
    sqlContext.read.load(Config.cf.dimUserParquet).select($"gid", $"user_id".cast("string"), $"user_name", $"email", $"mobile").distinct().persist().registerTempTable("dim_user")
    sqlContext.sql("SELECT gid as gid, max(user_id) as user_id, max(user_name) as user_name, max(email) as email, max(mobile) as mobile FROM  dim_user group by gid ").coalesce(200).registerTempTable("dim_u")
    sqlContext.sql("SELECT user_id as user_id, max(user_name) as user_name, max(email) as email, max(mobile) as mobile FROM  dim_user group by user_id").coalesce(200).registerTempTable("dim_du")

    //join glodon user
    sqlContext.sql(
      """
      SELECT d.pId, d.pcode, d.gid, d.dognum,d.ver,
             d.projectid,d.prjname,d.prjfullpath,d.prjcost,d.prjsize,d.major,d.duration,d.utype,d.receivetime,
             d.first_open_datetime,
             d.last_close_datetime,
             d.total_duration,d.last_long,
             case when u.user_id is not null then u.user_id else du.user_id end as user_id,
             case when u.user_name is not null then u.user_name else du.user_name end as user_name,
             case when u.email is not null then u.email else du.email end as email,
             case when u.mobile is not null then u.mobile else du.mobile end as mobile
      FROM dayProject d  LEFT  JOIN dim_u u ON (d.gid=u.gid)
      LEFT JOIN dim_du du ON (d.gid= du.user_id)
      """).coalesce(200).na.fill(-1).na.fill("N/A").na.replace("user_id" :: "user_name" :: "email" :: "mobile" :: Nil, Map("" -> "N/A")).registerTempTable("joinDayProject")
    val projectDf = sqlContext.sql(
      """
          SELECT
                 case when d.pId is not null and a.pId is not null then d.pId
                      when d.pId is not null and a.pId is null then d.pId
                      when d.pId is null and a.pId is not null then a.pId
                 end as pId,
                 case when d.pId is not null and a.pId is not null then d.pcode
                      when d.pId is not null and a.pId is null then d.pcode
                      when d.pId is null and a.pId is not null then a.pcode
                 end as pcode,
                 case when d.pId is not null and a.pId is not null then d.gid
                      when d.pId is not null and a.pId is null then d.gid
                      when d.pId is null and a.pId is not null then a.gid
                 end as gid,
                 case when d.pId is not null and a.pId is not null then d.dognum
                      when d.pId is not null and a.pId is null then d.dognum
                      when d.pId is null and a.pId is not null then a.dognum
                 end as dognum,
                 case when d.pId is not null and a.pId is not null then d.ver
                      when d.pId is not null and a.pId is null then d.ver
                      when d.pId is null and a.pId is not null then a.ver
                 end as ver,
                 case when d.pId is not null and a.pId is not null then d.projectid
                      when d.pId is not null and a.pId is null then d.projectid
                      when d.pId is null and a.pId is not null then a.projectid
                 end as projectid,
                 case when d.pId is not null and a.pId is not null then d.prjname
                      when d.pId is not null and a.pId is null then d.prjname
                      when d.pId is null and a.pId is not null then a.prjname
                 end as prjname,
                 case when d.pId is not null and a.pId is not null then d.prjfullpath
                      when d.pId is not null and a.pId is null then d.prjfullpath
                      when d.pId is null and a.pId is not null then a.prjfullpath
                 end as prjfullpath,
                 case when d.pId is not null and a.pId is not null then d.prjcost
                      when d.pId is not null and a.pId is null then d.prjcost
                      when d.pId is null and a.pId is not null then a.prjcost
                 end as prjcost,
                 case when d.pId is not null and a.pId is not null then d.prjsize
                      when d.pId is not null and a.pId is null then d.prjsize
                      when d.pId is null and a.pId is not null then a.prjsize
                 end as prjsize,
                 case when d.pId is not null and a.pId is not null then d.major
                      when d.pId is not null and a.pId is null then d.major
                      when d.pId is null and a.pId is not null then a.major
                 end as major,
                 case when d.pId is not null and a.pId is not null then d.duration
                      when d.pId is not null and a.pId is null then d.duration
                      when d.pId is null and a.pId is not null then a.duration
                 end as duration,
                 case when d.pId is not null and a.pId is not null then d.utype
                      when d.pId is not null and a.pId is null then d.utype
                      when d.pId is null and a.pId is not null then a.utype
                 end as utype,
                 case when d.pId is not null and a.pId is not null then d.receivetime
                      when d.pId is not null and a.pId is null then d.receivetime
                      when d.pId is null and a.pId is not null then a.receivetime
                 end as receivetime,
                 case when d.pId is not null and a.pId is not null and a.first_open_datetime <= d.first_open_datetime then a.first_open_datetime
                      when d.pId is not null and a.pId is not null and a.first_open_datetime >= d.first_open_datetime then d.first_open_datetime
                      when d.pId is not null and a.pId is null then d.first_open_datetime
                      when d.pId is null and a.pId is not null then a.first_open_datetime
                 end as first_open_datetime,
                 case when d.pId is not null and a.pId is not null and  a.last_close_datetime >= d.last_close_datetime then a.last_close_datetime
                      when d.pId is not null and a.pId is not null and  a.last_close_datetime <= d.last_close_datetime then d.last_close_datetime
                      when d.pId is not null and a.pId is null then d.last_close_datetime
                      when d.pId is null and a.pId is not null then a.last_close_datetime
                 end as last_close_datetime,
                 case when d.pId is not null and a.pId is not null  then cast(cast(a.total_duration as double) + cast(d.total_duration as double) as string)
                      when d.pId is not null and a.pId is null then d.total_duration
                      when d.pId is null and a.pId is not null then a.total_duration
                 end as total_duration,
                 case when d.pId is not null and a.pId is not null then d.last_long
                      when d.pId is not null and a.pId is null then d.last_long
                      when d.pId is null and a.pId is not null then a.last_long
                 end as last_long,
                 case when d.pId is not null and a.pId is not null and d.user_id ='N/A' then a.user_id
                      when d.pId is not null and a.pId is not null and d.user_id <>'N/A' then d.user_id
                      when d.pId is not null and a.pId is null then d.user_id
                      when d.pId is null and a.pId is not null then a.user_id
                 end as user_id,
                 case when d.pId is not null and a.pId is not null and d.user_name ='N/A' then a.user_name
                      when d.pId is not null and a.pId is not null and d.user_name <>'N/A' then d.user_name
                      when d.pId is not null and a.pId is null then d.user_name
                      when d.pId is null and a.pId is not null then a.user_name
                 end as user_name,
                 case when d.pId is not null and a.pId is not null and d.email ='N/A' then a.email
                      when d.pId is not null and a.pId is not null and d.email <>'N/A' then d.email
                      when d.pId is not null and a.pId is null then d.email
                      when d.pId is null and a.pId is not null then a.email
                 end as email,
                 case when d.pId is not null and a.pId is not null and d.mobile ='N/A' then a.mobile
                      when d.pId is not null and a.pId is not null and d.mobile <>'N/A' then d.mobile
                      when d.pId is not null and a.pId is null then d.mobile
                      when d.pId is null and a.pId is not null then a.mobile
                 end as mobile,
                 a.pId as all_pid,
                 a.gid as all_gid
          FROM joinDayProject d  FULL JOIN allProject a on a.pId = d.pId
      """).explode("gid","newgid"){gid:String =>
           if(gid.startsWith("N/A")){
             scala.Array("N/A")
           }else{
             scala.Array(gid)
           }
    }.drop("gid").withColumnRenamed("newgid","gid").explode("all_pid", "is_insert") { all_pid: String =>
      if (all_pid == null)
        scala.Array(1)
      else
        scala.Array(0)
    }.coalesce(200)

//    insertToEdw(projectDf)

//    updateProject(projectDf)

    projectToHdfs(projectDf)

    //projectDf.unpersist()
  }


  def projectToHdfs(projectDf : DataFrame)={
    //增量存盘hdfs
    println(s"save increment project to hdfs:${projectParquetPath}\n")
    projectDf.
      select("pId", "pcode", "gid", "dognum", "ver", "projectid", "prjname", "prjfullpath", "prjcost", "prjsize", "major", "duration", "utype", "receivetime", "first_open_datetime", "last_close_datetime", "total_duration", "last_long", "user_id", "user_name", "email", "mobile").
      coalesce(200).write.mode("overwrite").save(projectParquetPathTemp)
      var cmd = "hadoop fs  -rmr "+ projectParquetPath + "part*"
      println("#######################################################################################################")
      println(cmd)
      println("#######################################################################################################")
      cmd!

      cmd = "hadoop  fs  -mv " + projectParquetPathTemp + "part* " + projectParquetPath
      println("#######################################################################################################")
      println(cmd)
      println("#######################################################################################################")
      cmd!

    //增量存储edw service
    println(s"start to save increment project to edw-service\n")
  }

  //暂时不用同步至mysql
  def insertToEdw(projectDf:DataFrame)={
    projectDf.where("is_insert=1").foreachPartition((iterator: Iterator[Row]) =>{
      lazy val conn: Connection  = DriverManager.getConnection(Constants.EDW_JDBC_URL,"webuser","123.c0m")
      conn.setAutoCommit(false)
      val sql =
        """insert ignore into fact_project_by_product_lock_b(
           id,
           product_id,
           lock_number,
           yun_account_id,
           yun_account_name,
           mobile,
           email,
           version,
           project_id,
           project_name,
           project_addr,
           project_type,
           project_size,
           project_cost,
           total_long,
           first_open_datetime,
           last_close_datetime,
           last_long) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
      lazy val ps : PreparedStatement = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_READ_ONLY)
      try{
        var count:Long = 0
        iterator.foreach(row=>{

          ps.clearParameters()
          ps.setString(1, row.getAs[String]("pId"))
          ps.setString(2, row.getAs[String]("pcode"))
          ps.setString(3, row.getAs[String]("dognum"))
          ps.setString(4, row.getAs[String]("user_id"))
          ps.setString(5, row.getAs[String]("user_name"))
          ps.setString(6, row.getAs[String]("mobile"))
          ps.setString(7, row.getAs[String]("email"))
          ps.setString(8, row.getAs[String]("ver"))
          ps.setString(9, row.getAs[String]("projectid"))
          ps.setString(10, row.getAs[String]("prjname"))
          ps.setString(11, row.getAs[String]("prjfullpath"))
          ps.setString(12, row.getAs[String]("utype"))
          ps.setString(13, row.getAs[String]("prjsize"))
          ps.setString(14, row.getAs[String]("prjcost"))
          ps.setDouble(15, row.getAs[String]("total_duration").toDouble)
          ps.setString(16 , row.getAs[String]("first_open_datetime"))
          ps.setString(17 , row.getAs[String]("last_close_datetime"))
          ps.setDouble(18, row.getAs[String]("last_long").toDouble)
          count = count + 1
          ps.addBatch()
          if (count % 10000 ==0){
            ps.executeBatch()
            conn.commit()
            ps.clearBatch()
            count =0
          }
        })
        if(count >0){
          ps.executeBatch()
          conn.commit()
        }
      }catch {
        case e:Exception=>e.printStackTrace()
      }finally {
        if(ps != null) ps.close()
        if(conn != null) conn.close()
      }
    })
    println(s"save increment project to edw-service finished.\n")
  }
 //暂时不用同步至mysql
  def updateProject(projectDf : DataFrame) = {
    println(s"start to update project from edw-service\n")
    projectDf.where("is_insert=0").foreachPartition((iterator: Iterator[Row]) => {
      lazy val conn: Connection = DriverManager.getConnection(Constants.EDW_JDBC_URL,"webuser","123.c0m")
      conn.setAutoCommit(false)
      var sql = "update fact_project_by_product_lock_b set yun_account_id =if(yun_account_id='N/A',?, yun_account_id),  total_long = ?, first_open_datetime=?,last_close_datetime =  ?, last_long =  ? where id = ? "
      lazy val ps: PreparedStatement = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY)
      try {
        var count: Long = 0
        iterator.foreach(row => {
          ps.setString(1, row.getAs[String]("user_id"))
          ps.setDouble(2, row.getAs[String]("total_duration").toDouble)
          ps.setString(3, row.getAs[String]("first_open_datetime"))
          ps.setString(4, row.getAs[String]("last_close_datetime"))
          ps.setDouble(5, row.getAs[String]("last_long").toDouble)
          ps.setString(6, row.getAs[String]("pId"))
          count = count + 1
          ps.addBatch()
          if (count % 10000 == 0) {
            ps.executeBatch()
            conn.commit()
            ps.clearBatch()
            count = 0
          }
        })
        if (count > 0) {
          ps.executeBatch()
          conn.commit()
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (ps != null) ps.close()
        if (conn != null) conn.close()
      }
    })
  }

  def dimProjectCompute = {
    sqlContext.read.load(dimProjectParquetPath).select("pId").registerTempTable("allDimProject")
    sqlContext.sql("select projectid, prjname,prjfullpath,prjcost,prjsize,major,duration,utype from dayProject").
      map(r => {
        var key = r.getAs[String]("projectid") + r.getAs[String]("prjname")
        key = java.security.MessageDigest.getInstance("MD5").digest(key.getBytes()).map(0xFF & _).map {"%02x".format(_)}.foldLeft("") {_ + _}
        var prjname = r.getAs[String]("prjname")
        if (prjname.indexOf("\\") > 0) {
          prjname = prjname.replace("\\", "")
        }
        val v = r.getAs[String]("projectid") ::
          prjname ::
          r.getAs[String]("prjfullpath") ::
          r.getAs[String]("prjcost") ::
          r.getAs[String]("prjsize") ::
          r.getAs[String]("major") ::
          r.getAs[String]("duration") ::
          r.getAs[String]("utype") :: Nil
        (key, v)
      }).reduceByKey((v1, v2) => v2).coalesce(200).map(row => (row._1, (row._2) (0), (row._2) (1), (row._2) (2), (row._2) (3), (row._2) (4), (row._2) (5), (row._2) (6), (row._2) (7))).
      toDF("pId", "projectid", "prjname", "prjfullpath", "prjcost", "prjsize", "major", "duration", "utype").registerTempTable("dayDimProject")
    //增量
    val dimProjectDf = sqlContext.sql(
      """
      select d.pId,
             d.projectid,
             d.prjname,
             d.prjfullpath,
             d.prjcost,
             d.prjsize,
             d.major,
             d.duration,
             d.utype
      from dayDimProject d  left join allDimProject a on a.pId = d.pId where a.pId is null and d.pId is not null
      """).coalesce(200)//.persist()
    //入库edw //暂时不用同步至mysql
//    dimProjectDf.foreachPartition((iterator: Iterator[Row]) => {
//      lazy val conn: Connection = DriverManager.getConnection(Constants.EDW_JDBC_URL, "webuser", "123.c0m")
//      conn.setAutoCommit(false)
//      val sql = "insert ignore into dim_project_from_action_b(pid, project_id, project_name,prjfullpath,prjcost,prjsize,major,duration,utype,regionrule) values (?,?,?,?,?,?,?,?,?,?)"
//      lazy val ps: PreparedStatement = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY)
//      try {
//        var count: Long = 0
//        iterator.foreach(row => {
//          ps.clearParameters()
//          ps.setString(1, row.getAs[String]("pId"))
//          ps.setString(2, row.getAs[String]("projectid"))
//          ps.setString(3, row.getAs[String]("prjname"))
//          ps.setString(4, row.getAs[String]("prjfullpath"))
//          ps.setString(5, row.getAs[String]("prjcost"))
//          ps.setString(6, row.getAs[String]("prjsize"))
//          ps.setString(7, row.getAs[String]("major"))
//          ps.setString(8, row.getAs[String]("duration"))
//          ps.setString(9, row.getAs[String]("utype"))
//          ps.setString(10, "N/A")
//          count = count + 1
//          ps.addBatch()
//          if (count % 10000 == 0) {
//            ps.executeBatch()
//            conn.commit()
//            ps.clearBatch()
//            count = 0
//          }
//        })
//        if (count > 0) {
//          ps.executeBatch()
//          conn.commit()
//        }
//      } catch {
//        case e: Exception => e.printStackTrace()
//      } finally {
//        if (ps != null) ps.close()
//        if (conn != null) conn.close()
//      }
//    })
    //存盘hdfs
    dimProjectDf.coalesce(10).write.mode("append").save(dimProjectParquetPath)
    //dimProjectDf.unpersist()
  }


  def projectSum ={
    lazy val conn: Connection = DriverManager.getConnection(Constants.EDW_JDBC_URL, "webuser", "123.c0m")
    conn.setAutoCommit(false)
    val sql = ""
    lazy val ps: Statement = conn.createStatement()
    try {
      lazy val ps : Statement = conn.createStatement()
      var sql = "TRUNCATE batch.fact_project_sum_by_product_lock_a"
      ps.addBatch(sql)
//      sql = "INSERT INTO fact_project_sum_by_product_ver_lock_a(product_id,ver,lock_number,project_cnt) " +
//        "select product_id ,version, lock_number, count(1) from fact_project_by_product_lock_a " +
//        " where project_name<>'N/A'  and INSTR(project_name,'tmp')=0 " +
//        "GROUP BY product_id , version,lock_number"
//      ps.addBatch(sql)
      ps.executeBatch()
      conn.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (ps != null) ps.close()
      if (conn != null) conn.close()
    }
    sqlContext.read.load(projectParquetPath).select("pcode","ver","dognum").registerTempTable("projectSum")
    sqlContext.sql("select pcode as product_id,ver as ver, dognum as lock_number, count(1) as project_cnt from projectSum   GROUP BY pcode , ver, dognum").coalesce(200).
      foreachPartition((iterator: Iterator[Row]) => {
      lazy val conn: Connection = DriverManager.getConnection("jdbc:mysql://10.127.84.14:13306/batch?user=webuser&password=123.c0m", "webuser", "123.c0m")
      conn.setAutoCommit(false)
      val sql = "insert into batch.fact_project_sum_by_product_lock_a(product_id,ver,lock_number,project_cnt) values (?,?,?,?)"
      lazy val ps: PreparedStatement = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY)
      try {
        var count: Long = 0
        iterator.foreach(row => {
          ps.clearParameters()
          ps.setString(1, row.getAs[String]("product_id"))
          ps.setString(2, row.getAs[String]("ver"))
          ps.setString(3, row.getAs[String]("lock_number"))
          ps.setInt(4, row.getAs[Long]("project_cnt").toInt)
          count = count + 1
          ps.addBatch()
          if (count % 10000 == 0) {
            ps.executeBatch()
            conn.commit()
            ps.clearBatch()
            count = 0
          }
        })
        if (count > 0) {
          ps.executeBatch()
          conn.commit()
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (ps != null) ps.close()
        if (conn != null) conn.close()
      }
    })
  }


  def main(args: scala.Array[String]) {

    var (batchStart, batchEnd, run) = (0, 0, "all")
    if (args != null && args.length == 2) {
      batchStart = args(0).toInt
      batchEnd = args(1).toInt
    }
    if (args != null && args.length == 3) {
      batchStart = args(0).toInt
      batchEnd = args(1).toInt
      run = args(2).toString
    }
    if (batchStart == 0 && batchEnd == 0) {
      batchStart = Config.cf.endDate
      batchEnd = Config.cf.stopMonthDay
    }
    println(s"date:[${batchStart} , ${batchEnd}]\n")
    var baseDataDf = sqlContext.read.load(parquet_userlog_path).where(s"mday >= '${batchStart}' and mday <= '${batchEnd}'")
    projectCompute(baseDataDf)
    dimProjectCompute
    projectSum
    //if (args == null || args.length == 0) {
      println(s"update date:[${batchStart} , ${batchEnd}]\n")
      Config.udpateDate(batchStart, batchEnd)
    //}
  }
}
