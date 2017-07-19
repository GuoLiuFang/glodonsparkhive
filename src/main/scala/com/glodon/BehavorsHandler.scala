package com.glodon


import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

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
  private val projectParquetPathTemp = "/glodon/apps/public/fact_project_by_product_lock_temp/"

  private case class Project(var pId: String, var pcode: Int, var gid: String, var dognum: String, var ver: String, var projectid: String, var prjname: String, var prjfullpath: String,
                             var prjcost: String, var prjsize: String, var major: String, var duration: String, var utype: String, var receivetime: String, var first_open_datetime: String,
                             var last_close_datetime: String, var total_duration: String, var last_long: String)

  def main(args: Array[String]): Unit = {
    var pcodeList = new ListBuffer[String]()
    if (args.length < 2) {
      println(
        """Usage: batchStart batchEnd [pcodeList]
          example1: 20170101 20170704
          example1: 20170101 20170704 11036 -103000 -103001""")
      System.exit(-1);
    }
    val batchStart = args(0).toInt
    val batchEnd = args(1).toInt
    var i = 2
    while (i < args.length) {
      pcodeList += args(i)
      i += 1
    }
    //默认是 pcodeList=all
    var baseDataDf = sqlContext.read.parquet(glodon_userlog_path).filter(s"mday >= '${batchStart}' and mday <= '${batchEnd}'")
    if (pcodeList.size > 0) {
      val plist = pcodeList.mkString(",")
      print(s"plist")
      baseDataDf = sqlContext.read.parquet(glodon_userlog_path).filter(s"pcode in (${plist}) and mday >= '${batchStart}' and mday <= '${batchEnd}'")
    }
    projectCompute(baseDataDf)
    //    projectSum
    //    bim5dProject(baseDataDf)
    //    bim5dProjectUser(baseDataDf)
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
    val files = fileSystem.listFiles(new Path(projectParquetPathTemp), true)
    val hasNext = files.hasNext
    var projectDf = dayBaseDF
    if (hasNext) {
      sqlContext.read.parquet(projectParquetPathTemp).registerTempTable("allProject")
      dayBaseDF.registerTempTable("dayProject")
      projectDf = sqlContext.sql("select d.* from dayProject d  left join allProject a on a.pId = d.pId where a.pId is null and d.pId is not null and d.dognum<>'N/A'")
    }
    projectDf.repartition($"pcode").write.partitionBy("pcode").mode("append").parquet(projectParquetPathTemp)
  }

}
