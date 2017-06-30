package com.glodon

import com.glodon.config.{Config, Constants}
import java.sql._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import scala.sys.process._


case class Project(var pId: String, var pcode: String, var gid: String, var dognum: String, var ver: String, var projectid: String, var prjname: String, var prjfullpath: String,
                   var prjcost: String, var prjsize: String, var major: String, var duration: String, var utype: String, var receivetime: String, var first_open_datetime: String,
                   var last_close_datetime: String, var total_duration: String, var last_long: String)

object ProjectFromAction {

  val conf = new SparkConf().setAppName("project-from-action")
  implicit val sc = new SparkContext(conf)
  implicit val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._

  val parquet_userlog_path = "/glodon/layer2_wide_table/parquet_userlog"
  val projectParquetPath = "/glodon/apps/public/fact_project_by_product_lock/"
  val projectParquetPathTemp = "/glodon/apps/public/fact_project_by_product_lock_temp/"

  def projectCompute(baseDataDf: DataFrame) = {
    //allProject 基础数据
    sqlContext.read.load(projectParquetPath).registerTempTable("allProject")

    //dayProject
    baseDataDf.where("(projectid <> 'N/A') or (prjname <> 'N/A') or (prjfullpath<>'N/A')").
      select("pcode", "gid", "dognum", "ver", "projectid", "prjname", "prjfullpath", "prjcost", "prjsize", "major", "duration", "utype", "receivetime").
      na.fill(0).na.fill("N/A").na.replace("pcode" :: "gid" :: "dognum" :: "ver" :: "projectid" :: "prjname" :: "prjfullpath" :: "prjcost" :: "prjsize" :: "major" :: "duration" :: "utype" :: "receivetime" :: Nil, Map("" -> "N/A")).coalesce(200).
      map(r => {
        var (pcode, gid, dognum, ver, projectid, prjname) = (r.getAs[String]("pcode"), r.getAs[String]("gid"), r.getAs[String]("dognum"), r.getAs[String]("ver"), r.getAs[String]("projectid"), r.getAs[String]("prjname"))
        var (prjfullpath, prjcost, prjsize, major, duration, utype) = (r.getAs[String]("prjfullpath"), r.getAs[String]("prjcost"), r.getAs[String]("prjsize"), r.getAs[String]("major"), r.getAs[String]("duration"), r.getAs[String]("utype"))
        duration = if (duration == "N/A") "0" else duration
        var (receivetime, first_open_datetime, last_close_datetime, total_duration, last_long) = (r.getAs[String]("receivetime"), r.getAs[String]("receivetime"), r.getAs[String]("receivetime"), duration, duration)
        if ((prjname == null || prjname == "N/A") && (prjfullpath != null && prjfullpath != "N/A")) {
          prjname = if (prjfullpath.indexOf("\\") > 0) prjfullpath.split("\\\\").last else prjfullpath
          prjname = if (prjname.indexOf("/") > 0) prjname.split("/").last else prjname
          if (prjname.indexOf("\\") > 0) {
            prjname = prjname.replace("\\", "")
          }
        }
        var key = pcode + dognum + projectid + prjname
        key = java.security.MessageDigest.getInstance("MD5").digest(key.getBytes()).map(0xFF & _).map {
          "%02x".format(_)
        }.foldLeft("") {
          _ + _
        }
        (key, Project(key, pcode, gid, dognum, ver, projectid, prjname, prjfullpath, prjcost, prjsize, major, duration, utype, receivetime, first_open_datetime, last_close_datetime, total_duration, last_long))
      }).reduceByKey((v1, v2) => {
      v1.first_open_datetime = if (v1.first_open_datetime < v2.first_open_datetime) v1.first_open_datetime else v2.first_open_datetime
      v1.last_close_datetime = if (v1.last_close_datetime > v2.last_close_datetime) v1.last_close_datetime else v2.last_close_datetime
      if (v1.receivetime < v2.receivetime) {
        if (v2.pcode != "N/A") v1.pcode = v2.pcode
        if (v2.gid != "N/A") v1.gid = v2.gid
        if (v2.dognum != "N/A") v1.dognum = v2.dognum
        if (v2.ver != "N/A") v1.ver = v2.ver
        if (v2.projectid != "N/A") v1.projectid = v2.projectid
        if (v2.prjname != "N/A") v1.prjname = v2.prjname
        if (v2.prjfullpath != "N/A") v1.prjfullpath = v2.prjfullpath
        if (v2.prjcost != "N/A") v1.prjcost = v2.prjcost
        if (v2.prjsize != "N/A") v1.prjsize = v2.prjsize
        if (v2.major != "N/A") v1.major = v2.major
        if (v2.duration != "N/A") v1.duration = v2.duration
        if (v2.utype != "N/A") v1.utype = v2.utype
        v1.receivetime = v2.receivetime
        if (v2.last_long != "0") v1.last_long = v2.last_long
      }
      v1.total_duration = (v1.total_duration.toDouble + v2.total_duration.toDouble).toString
      v1
    }).coalesce(200).map(r => r._2).toDF().registerTempTable("dayProject")

    val cmd = s"hadoop fs  -rmr ${projectParquetPathTemp}*"
    cmd !

    val projectDf = sqlContext.sql("select d.* from dayProject d  left join allProject a on a.pId = d.pId where a.pId is null and d.pId is not null and d.dognum<>'N/A'").coalesce(200)
    projectDf.write.mode("overwrite").save(projectParquetPathTemp)
    sqlContext.read.load(projectParquetPathTemp).coalesce(10).write.mode("append").save(projectParquetPath)
  }


  def projectSum = {
    lazy val conn: Connection = DriverManager.getConnection(Constants.EDW_JDBC_URL, "webuser", "123.c0m")
    conn.setAutoCommit(false)
    val sql = ""
    lazy val ps: Statement = conn.createStatement()
    try {
      lazy val ps: Statement = conn.createStatement()
      var sql = "TRUNCATE batch.fact_project_sum_by_product_lock_a"
      ps.addBatch(sql)
      ps.executeBatch()
      conn.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (ps != null) ps.close()
      if (conn != null) conn.close()
    }
    sqlContext.read.load(projectParquetPath).select("pcode", "ver", "dognum").registerTempTable("projectSum")
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


  def bim5dProject(baseDf: DataFrame) = {
    val bim5d_project_path = "/glodon/apps/bim5d_project/project"
    val bim5d_project_tmp_path = "/glodon/apps/bim5d_project/tmp"

    var df = baseDf.select("mday", "pcode", "projectid", "prjname", "dognum", "gid", "hardwareid", "fncode", "fnname", "trigertime").
      where("pcode in ('11036','-103000','-103001')").na.fill("N/A").repartition(200)
    //11036 -103000 -103001
    val dayDf = df.map(x => {
      val mday = x.getAs[Int]("mday")
      val pcode = x.getAs[String]("pcode")
      val projectid = x.getAs[String]("projectid")
      val prjname = x.getAs[String]("prjname")
      val dognum = x.getAs[String]("dognum")
      val gid = x.getAs[String]("gid")
      val hardwareid = x.getAs[String]("hardwareid")
      val fncode = x.getAs[Int]("fncode").toString()
      val fnname = x.getAs[String]("fnname")
      var trigertime = x.getAs[String]("trigertime")
      var tmday = "20850101"
      if (trigertime != null && trigertime.length() > 0) {
        trigertime = trigertime.replaceAll("年", "").replaceAll("月", "").replaceAll("日", "")
        tmday = trigertime.substring(0, 4) + trigertime.substring(5, 7) + trigertime.substring(8, 10)
      }
      val bimProject = BimProject("N/A", tmday, pcode, projectid, prjname, dognum, gid, hardwareid, fncode, fnname, 1, trigertime)
      (bimProject.key, bimProject)
    }).reduceByKey((r1, r2) => {
      if (r1.trigertime < r2.trigertime) {
        r1.prjname = if (r2.prjname != "N/A") r2.prjname else r1.prjname
        r1.fnname = if (r2.fnname != "N/A") r2.fnname else r1.fnname
      } else {
        r1.prjname = if (r1.prjname == "N/A") r2.prjname else r1.prjname
        r1.fnname = if (r1.fnname == "N/A") r2.fnname else r1.fnname
      }
      r1.count = r1.count + r2.count
      r1
    }, 200).map(x => x._2).toDF()
    dayDf.persist()
    val tmdays = dayDf.select("tmday").distinct().map(_.getAs[String]("tmday")).cache().collect()
    val historyDf = sqlContext.read.load(bim5d_project_path).where(s"""tmday in ('${tmdays.mkString("','")}')""").
      withColumnRenamed("key", "h_key").
      withColumnRenamed("tmday", "h_tmday").
      withColumnRenamed("pcode", "h_pcode").
      withColumnRenamed("projectid", "h_projectid").
      withColumnRenamed("prjname", "h_prjname").
      withColumnRenamed("dognum", "h_dognum").
      withColumnRenamed("gid", "h_gid").
      withColumnRenamed("hardwareid", "h_hardwareid").
      withColumnRenamed("fncode", "h_fncode").
      withColumnRenamed("fnname", "h_fnname").
      withColumnRenamed("count", "h_count").
      withColumnRenamed("trigertime", "h_trigertime")

    val saveDf = dayDf.join(historyDf, dayDf("key") === historyDf("h_key"), "full").map(x => {
      var (key, h_key) = (x.getAs[String]("key"), x.getAs[String]("h_key"))
      var tmday = x.getAs[String]("tmday")
      var pcode = x.getAs[String]("pcode")
      var projectid = x.getAs[String]("projectid")
      var prjname = x.getAs[String]("prjname")
      var dognum = x.getAs[String]("dognum")
      var gid = x.getAs[String]("gid")
      var hardwareid = x.getAs[String]("hardwareid")
      var fncode = x.getAs[String]("fncode")
      var fnname = x.getAs[String]("fnname")
      var count = x.getAs[Int]("count")
      var trigertime = x.getAs[String]("trigertime")
      var h_tmday = x.getAs[Int]("h_tmday")
      var h_pcode = x.getAs[String]("h_pcode")
      var h_projectid = x.getAs[String]("h_projectid")
      var h_prjname = x.getAs[String]("h_prjname")
      var h_dognum = x.getAs[String]("h_dognum")
      var h_gid = x.getAs[String]("h_gid")
      var h_hardwareid = x.getAs[String]("h_hardwareid")
      var h_fncode = x.getAs[String]("h_fncode")
      var h_fnname = x.getAs[String]("h_fnname")
      var h_count = x.getAs[Int]("h_count")
      var h_trigertime = x.getAs[String]("h_trigertime")
      if (key != null && h_key != null) {
        if (trigertime < h_trigertime) {
          tmday = h_tmday.toString()
          pcode = h_pcode
          projectid = h_projectid
          prjname = h_prjname
          dognum = h_dognum
          gid = h_gid
          hardwareid = h_hardwareid
          fncode = h_fncode
          fnname = h_fnname
          trigertime = h_trigertime
        }
        count = count + h_count
      }
      if (key == null && h_key != null) {
        key = h_key
        tmday = h_tmday.toString()
        pcode = h_pcode
        projectid = h_projectid
        prjname = h_prjname
        dognum = h_dognum
        gid = h_gid
        hardwareid = h_hardwareid
        fncode = h_fncode
        fnname = h_fnname
        count = h_count
        trigertime = h_trigertime
      }
      var trigertimeMday = if (tmday != null) tmday.toString() else "20850101"
      BimProject(key, trigertimeMday, pcode, projectid, prjname, dognum, gid, hardwareid, fncode, fnname, count, trigertime)
    }).toDF()
    saveDf.persist()
    saveDf.repartition(1).write.mode("overwrite").partitionBy("tmday").save(bim5d_project_tmp_path)

    var cmd = ""
    saveDf.select("tmday").distinct().map(_.getAs[String]("tmday")).collect().foreach(day => {
      cmd = cmd + s" ${bim5d_project_path}/tmday=${day} "
    })
    println(cmd)
    if (cmd.length > 0) {
      cmd = "hadoop fs  -rmr " + cmd
      cmd !
    }

    cmd = s"hadoop fs  -cp  ${bim5d_project_tmp_path}/tmday*  ${bim5d_project_path}/"
    cmd !

    dayDf.unpersist()
    saveDf.unpersist()

  }


  def bim5dProjectUser(baseDataDf: DataFrame) = {
    val bim5d_user_path = "/glodon/apps/bim5d_project/userproject"
    val bim5d_project_path = "/glodon/apps/bim5d_project/project"
    val bim5d_user_tmp_path = "/glodon/apps/bim5d_project/usertmp"

    val df = baseDataDf.select("mday", "pcode", "dognum", "gid", "hardwareid", "fncode", "fnname", "trigertime").where("pcode in ('11036','-103000','-103001')").na.fill("N/A").repartition(200)

    val dayDf = df.map(x => {
      val mday = x.getAs[Int]("mday")
      val pcode = x.getAs[String]("pcode")
      val dognum = x.getAs[String]("dognum")
      val gid = x.getAs[String]("gid")
      val hardwareid = x.getAs[String]("hardwareid")
      val fncode = x.getAs[Int]("fncode").toString()
      val fnname = x.getAs[String]("fnname")
      var trigertime = x.getAs[String]("trigertime")
      var tmday = "20850101"
      if (trigertime != null && trigertime.length() > 0) {
        trigertime = trigertime.replaceAll("年", "").replaceAll("月", "").replaceAll("日", "")
        tmday = trigertime.substring(0, 4) + trigertime.substring(5, 7) + trigertime.substring(8, 10)
      }
      val bimUser = BimUser("N/A", tmday, pcode, dognum, gid, hardwareid, fncode, fnname, 1, trigertime)
      (bimUser.key, bimUser)
    }).reduceByKey((r1, r2) => {
      if (r1.trigertime < r2.trigertime) {
        r1.fnname = if (r2.fnname != "N/A") r2.fnname else r1.fnname
      } else {
        r1.fnname = if (r1.fnname == "N/A") r2.fnname else r1.fnname
      }
      r1.count = r1.count + r2.count
      r1
    }, 200).map(x => x._2).toDF() //只要 bimuser 对象。

    dayDf.persist()

    val tmdays = dayDf.select("tmday").distinct().map(_.getAs[String]("tmday")).cache().collect()
    //第一次跑应该从原始数据哪里搞。。
    val historyDf = sqlContext.read.load(bim5d_project_path).where(s"""tmday in ('${tmdays.mkString("','")}')""").
      withColumnRenamed("key", "h_key").
      withColumnRenamed("tmday", "h_tmday").
      withColumnRenamed("pcode", "h_pcode").
      withColumnRenamed("dognum", "h_dognum").
      withColumnRenamed("gid", "h_gid").
      withColumnRenamed("hardwareid", "h_hardwareid").
      withColumnRenamed("fncode", "h_fncode").
      withColumnRenamed("fnname", "h_fnname").
      withColumnRenamed("count", "h_count").
      withColumnRenamed("trigertime", "h_trigertime")

    val saveDf = dayDf.join(historyDf, dayDf("key") === historyDf("h_key"), "full").map(x => {
      var (key, h_key) = (x.getAs[String]("key"), x.getAs[String]("h_key"))
      var tmday = x.getAs[String]("tmday")
      var pcode = x.getAs[String]("pcode")
      var dognum = x.getAs[String]("dognum")
      var gid = x.getAs[String]("gid")
      var hardwareid = x.getAs[String]("hardwareid")
      var fncode = x.getAs[String]("fncode")
      var fnname = x.getAs[String]("fnname")
      var count = x.getAs[Int]("count")
      var trigertime = x.getAs[String]("trigertime")
      var h_tmday = x.getAs[Int]("h_tmday")
      var h_pcode = x.getAs[String]("h_pcode")
      var h_dognum = x.getAs[String]("h_dognum")
      var h_gid = x.getAs[String]("h_gid")
      var h_hardwareid = x.getAs[String]("h_hardwareid")
      var h_fncode = x.getAs[String]("h_fncode")
      var h_fnname = x.getAs[String]("h_fnname")
      var h_count = x.getAs[Int]("h_count")
      var h_trigertime = x.getAs[String]("h_trigertime")
      if (key != null && h_key != null) {
        if (trigertime < h_trigertime) {
          tmday = h_tmday.toString()
          pcode = h_pcode
          dognum = h_dognum
          gid = h_gid
          hardwareid = h_hardwareid
          fncode = h_fncode
          fnname = h_fnname
          trigertime = h_trigertime
        }
        count = count + h_count
      }
      if (key == null && h_key != null) {
        key = h_key
        tmday = h_tmday.toString()
        pcode = h_pcode
        dognum = h_dognum
        gid = h_gid
        hardwareid = h_hardwareid
        fncode = h_fncode
        fnname = h_fnname
        count = h_count
        trigertime = h_trigertime
      }
      var trigertimeMday = if (tmday != null) tmday.toString() else "20850101"
      BimUser(key, trigertimeMday, pcode, dognum, gid, hardwareid, fncode, fnname, count, trigertime)
    }).toDF()

    saveDf.persist()
    saveDf.repartition(1).write.mode("overwrite").partitionBy("tmday").save(bim5d_user_tmp_path)

    var cmd = ""
    saveDf.select("tmday").distinct().map(_.getAs[String]("tmday")).collect().foreach(day => {
      cmd = cmd + s" ${bim5d_user_path}/tmday=${day} "
    })
    println(cmd)
    if (cmd.length > 0) {
      cmd = "hadoop fs  -rmr " + cmd
      cmd !
    }

    cmd = s"hadoop fs  -cp  ${bim5d_user_tmp_path}/tmday*  ${bim5d_user_path}/"
    cmd !

    dayDf.unpersist()
    saveDf.unpersist()
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
    run match {
      case "all" => {
        projectCompute(baseDataDf)
        projectSum
        bim5dProject(baseDataDf)
      }
      case "zjproject" => projectCompute(baseDataDf)
      case "zjprojectsum" => projectSum
      case "bim5dproject" => bim5dProject(baseDataDf)
      case "bim5dprojectuser" => bim5dProjectUser(baseDataDf)
    }
    Config.udpateDate(batchStart, batchEnd)
  }
}
