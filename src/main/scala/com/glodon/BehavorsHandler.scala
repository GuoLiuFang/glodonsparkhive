package com.glodon


import java.sql.{Connection, DriverManager}

import com.glodon.config.{Config, Constants}
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by LiuFangGuo on 7/17/17.
  */
object BehavorsHandler {
  private val conf = new SparkConf().setAppName("Behavors-Handler")
  private val sc = new SparkContext(conf)
  private val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._

  //parquet_userlog_path原始的行为日志
  private val glodon_userlog_path = "/glodon/layer2_wide_table/glodon_userlog"
  //  private val glodon_userlog_path = "/glodon/layer2_wide_table/parquet_userlog"
  //projectParquetPath 追加线上。。
  //  private val projectParquetPath = "/glodon/apps/public/fact_project_by_product_lock/"
  private val projectParquetPathNew = "/glodon/apps/public/fact_project_by_product_lock_new/"

  private val bim5d_project_path_new = "/glodon/apps/bim5d_project/project_new"

  private case class Project(var pId: String, var pcode: Int, var gid: String, var dognum: String, var ver: String, var projectid: String, var prjname: String, var prjfullpath: String,
                             var prjcost: String, var prjsize: String, var major: String, var duration: String, var utype: String, var receivetime: String, var first_open_datetime: String,
                             var last_close_datetime: String, var total_duration: String, var last_long: String)

  private case class BimProject(var key: String, tmday: String, pcode: Int, projectid: String, var prjname: String, dognum: String, gid: String, hardwareid: String, fncode: String, var fnname: String, var count: Int = 0, trigertime: String)

  def printUsage(): Unit = {
    println(
      """---------------------------------------------------------------------------------
           Usage: batchStart batchEnd jobtype [pcodeList]
           bim5dProject: batchStart batchEnd bim5dproject 11036 -103000 -103001
          example1: 20170101 20170704 default
          example1: 20170101 20170704 default 11036 -103000 -103001
        ---------------------------------------------------------------------------------""")
    System.exit(-1);
  }

  def main(args: Array[String]): Unit = {
    var pcodeList = new ListBuffer[String]()
    if (args.length < 3) {
      printUsage
    }
    var batchStart = args(0).toInt
    var batchEnd = args(1).toInt
    batchStart = Config.cf.endDate
    batchEnd = Config.cf.stopMonthDay
    val run = args(2).toString
    var i = 3
    while (i < args.length) {
      pcodeList += args(i)
      i += 1
    }
    //默认是 pcodeList=default
    var baseDataDf = sqlContext.read.parquet(glodon_userlog_path).filter(s"mday >= '${batchStart}' and mday <= '${batchEnd}'")
    if (pcodeList.size > 0) {
      val plist = pcodeList.mkString(",")
      println(s"指定的产品列表为：${plist}")
      baseDataDf = baseDataDf.filter(s"pcode in (${plist})")
    }
    run match {
      case "default" => {
        projectCompute(baseDataDf)
        projectSum
      }
      case "bim5dproject" => bim5dProject(baseDataDf)
      case _ => {
        printUsage()
      }
    }
    Config.udpateDate(batchStart, batchEnd)
  }

  def projectCompute(baseDataDf: DataFrame) = {
    //projectid,prjname,prjfullpath任意一个不为空值的就算有效记录。。
    //na.fill double numeric string
    val dayBaseDF = baseDataDf.filter("(projectid <> 'N/A') or (prjname <> 'N/A') or (prjfullpath<>'N/A')")
      .select("pcode", "gid", "dognum", "ver", "projectid", "prjname", "prjfullpath", "prjcost", "prjsize", "major", "duration", "utype", "receivetime")
      .na.fill(0.0).na.fill("N/A")
      .na.replace("*", Map("" -> "N/A"))
      .map(r => {
        var (pcode, gid, dognum, ver, projectid, prjname) = (r.getAs[Int]("pcode"), r.getAs[String]("gid"), r.getAs[String]("dognum"), r.getAs[String]("ver"), r.getAs[String]("projectid"), r.getAs[String]("prjname"))
        var (prjfullpath, prjcost, prjsize, major, duration, utype) = (r.getAs[String]("prjfullpath"), r.getAs[String]("prjcost"), r.getAs[String]("prjsize"), r.getAs[String]("major"), r.getAs[String]("duration"), r.getAs[String]("utype"))
        duration = if (duration == "N/A") "0" else duration
        var (receivetime, first_open_datetime, last_close_datetime, total_duration, last_long) = (r.getAs[String]("receivetime"), r.getAs[String]("receivetime"), r.getAs[String]("receivetime"), duration, duration)
        //因为之前做了去 null 的处理，所以，以后所有的判断，都应该不用判断 null 值。。。
        if (prjname == "N/A" && prjfullpath != "N/A") {
          prjname = if (prjfullpath.indexOf("\\") > 0) prjfullpath.split("\\\\").last else prjfullpath
        }
        var key = "" + pcode + dognum + projectid + prjname
        key = DigestUtils.md5Hex(key)
        (key, Project(key, pcode, gid, dognum, ver, projectid, prjname, prjfullpath, prjcost, prjsize, major, duration, utype, receivetime, first_open_datetime, last_close_datetime, total_duration, last_long))
      })
      .reduceByKey((v1, v2) => {
        v1.first_open_datetime = if (v1.first_open_datetime < v2.first_open_datetime) v1.first_open_datetime else v2.first_open_datetime
        v1.last_close_datetime = if (v1.last_close_datetime > v2.last_close_datetime) v1.last_close_datetime else v2.last_close_datetime
        if (v1.receivetime < v2.receivetime) {
          if (v2.pcode != 0.0) v1.pcode = v2.pcode
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
      })
      .map(r => r._2)
      .toDF()
    val hadoopConfiguration = sc.hadoopConfiguration
    val fileSystem = org.apache.hadoop.fs.FileSystem.get(hadoopConfiguration)
    val path = new Path(projectParquetPathNew)
    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path)
    }
    val files = fileSystem.listFiles(path, true)
    val hasNext = files.hasNext
    var projectDf = dayBaseDF
    if (hasNext) {
      sqlContext.read.parquet(projectParquetPathNew).registerTempTable("allProject")
      dayBaseDF.registerTempTable("dayProject")
      projectDf = sqlContext.sql("select d.* from dayProject d  left join allProject a on a.pId = d.pId where a.pId is null and d.pId is not null and d.dognum<>'N/A'")
    }
    projectDf.repartition($"pcode").write.partitionBy("pcode").mode("append").parquet(projectParquetPathNew)
  }

  def projectSum = {

    val connection: Connection = DriverManager.getConnection(Constants.BULK_JDBC_URL, "webuser", "123.c0m")
    connection.setAutoCommit(false)
    val statement = connection.createStatement()
    //设置还原点
    val savepoint = connection.setSavepoint("savepoint1")
    try {
      val SQL = "TRUNCATE batch.fact_project_sum_by_product_lock_b"
      //      val SQL = "TRUNCATE batch.fact_project_sum_by_product_lock_a"
      statement.addBatch(SQL)
      statement.executeBatch()
      //如果没有问题就提交到数据库
      connection.commit()
    } catch {
      //如果出问题，就回滚到出事状态
      case e: Exception => {
        e.printStackTrace()
        connection.rollback(savepoint)
      }
    } finally {
      if (statement != null) {
        statement.close()
      }
      if (connection != null) {
        connection.close()
      }
    }
    sqlContext.read.parquet(projectParquetPathNew).select("pcode", "ver", "dognum").registerTempTable("projectSum")
    sqlContext.sql("select pcode as product_id,ver as ver, dognum as lock_number, count(1) as project_cnt from projectSum   GROUP BY pcode , ver, dognum")
      .coalesce(20)
      .foreachPartition((iterator: Iterator[Row]) => {
        val connection: Connection = DriverManager.getConnection(Constants.BULK_JDBC_URL, "webuser", "123.c0m")
        connection.setAutoCommit(false)
        val PRESQL = "insert into batch.fact_project_sum_by_product_lock_b(product_id,ver,lock_number,project_cnt) values (?,?,?,?)"
        //    val PRESQL = "insert into batch.fact_project_sum_by_product_lock_a(product_id,ver,lock_number,project_cnt) values (?,?,?,?)"
        val prepareStatement = connection.prepareStatement(PRESQL)
        //首先清理之前的批处理工作
        val savepoint1 = connection.setSavepoint()
        iterator.foreach((row) => {
          prepareStatement.setString(1, "" + row.getAs[Int]("product_id"))
          prepareStatement.setString(2, row.getAs[String]("ver"))
          prepareStatement.setString(3, row.getAs[String]("lock_number"))
          prepareStatement.setInt(4, row.getAs[Long]("project_cnt").toInt)
          prepareStatement.addBatch()
        })
        try {
          prepareStatement.executeBatch()
          connection.commit()
        } catch {
          case e: Exception => {
            e.printStackTrace()
            connection.rollback(savepoint1)
          }
        } finally {
          if (prepareStatement != null) {
            prepareStatement.close()
          }
          if (connection != null) {
            connection.close()
          }
        }

      })
    //最后收拾资源释放问题

  }

  def bim5dProject(baseDataDf: DataFrame) = {
    val df = baseDataDf.select("mday", "pcode", "projectid", "prjname", "dognum", "gid", "hardwareid", "fncode", "fnname", "trigertime")
      .na.fill(0.0).na.fill("N/A")
    val dayDf = df.map((x) => {
      val mday = x.getAs[Int]("mday")
      val pcode = x.getAs[Int]("pcode")
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
      var key = tmday + pcode + projectid + dognum + gid + hardwareid + fncode
      key = DigestUtils.md5Hex(key)
      (key, BimProject(key, tmday, pcode, projectid, prjname, dognum, gid, hardwareid, fncode, fnname, 1, trigertime))
    })
      .reduceByKey((r1, r2) => {
        if (r1.trigertime < r2.trigertime) {
          r1.prjname = if (r2.prjname != "N/A") r2.prjname else r1.prjname
          r1.fnname = if (r2.fnname != "N/A") r2.fnname else r1.fnname
        } else {
          r1.prjname = if (r1.prjname == "N/A") r2.prjname else r1.prjname
          r1.fnname = if (r1.fnname == "N/A") r2.fnname else r1.fnname
        }
        r1.count = r1.count + r2.count
        r1
      })
      .map(x => x._2)
      .toDF()
      .coalesce(200)
    val tmdays = dayDf.select("tmday").distinct().map(_.getAs[String]("tmday")).cache().collect()
    //存在的话，读取信息，删除目录及下面的东西。。
    val hadoopConfiguration = sc.hadoopConfiguration
    val fileSystem = org.apache.hadoop.fs.FileSystem.get(hadoopConfiguration)
    val path = new Path(bim5d_project_path_new)
    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path)
    }
    val files = fileSystem.listFiles(path, true)
    val hasNext = files.hasNext
    var saveDf = dayDf
    if (hasNext) {
      val historyDf = sqlContext.read.parquet(bim5d_project_path_new).filter(s"""tmday in ('${tmdays.mkString("','")}')""").
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

      saveDf = dayDf
        .join(historyDf, dayDf("key") === historyDf("h_key"), "outer")
        .map(x => {
          var (key, h_key) = (x.getAs[String]("key"), x.getAs[String]("h_key"))
          var tmday = x.getAs[String]("tmday")
          var pcode = x.getAs[Int]("pcode")
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
          var h_pcode = x.getAs[Int]("h_pcode")
          var h_projectid = x.getAs[String]("h_projectid")
          var h_prjname = x.getAs[String]("h_prjname")
          var h_dognum = x.getAs[String]("h_dognum")
          var h_gid = x.getAs[String]("h_gid")
          var h_hardwareid = x.getAs[String]("h_hardwareid")
          var h_fncode = x.getAs[String]("h_fncode")
          var h_fnname = x.getAs[String]("h_fnname")
          var h_count = x.getAs[Int]("h_count")
          var h_trigertime = x.getAs[String]("h_trigertime")
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
          var trigertimeMday = if (tmday != null) tmday.toString() else "20850101"
          BimProject(key, trigertimeMday, pcode, projectid, prjname, dognum, gid, hardwareid, fncode, fnname, count, trigertime)
        })
        .toDF()
        .coalesce(200)
      //信息读取,并使用完成后，清空目录下的所有文件
      //      fileSystem.delete(path, true)
    }
    saveDf.repartition($"pcode", $"tmday").write.partitionBy("pcode", "tmday").mode("overwrite").parquet(bim5d_project_path_new)

  }
}
