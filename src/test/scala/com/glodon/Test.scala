package com.glodon

import java.util.Locale
import java.text.SimpleDateFormat
import java.util.Date

import breeze.linalg.max
import org.joda.time._
import org.joda.time.format._

/**
  * Created by lius-f on 2016/7/19.
  */
object Test {


  def main(args: Array[String]) {
//    var s : String = "E:\\2016年7月17日优盘备份\\Ⅱ-1图纸\\华南城\\2016-4-22\\地库2016年7月17日版本\\华南城地库土建.GCL10"
//    s = "C:\\Users\\燕\\Desktop\\张富泉\\小院围墙七-YS20\\图形\\小院围墙七-YS20.GCL10"
//    println(s.split("\\\\").last)
//    println("小院围墙七-YS20.GCL10" == s.split("\\\\").last)
//    var t : String  = "2016/07/19 15:11:49"
//    val  loc = new Locale("CN")
//    val sf = new SimpleDateFormat("yyyy/MMM/dd HH:mm:ss")
//    val tm = "30/Jul/2015:05:00:50"
//    val dt2 = sf.parse(t);
//    dt2.getTime()

    var d1 = DateTime.parse("2016/07/20 15:11:49 000",DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss SSS"))
    var d2 = DateTime.parse("2016/07/19 15:11:49 000",DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss SSS"))
    println(d1.getMillis)
    println(d2.getMillis)
    println("2016/07/20 15:11:49 000".length)

    var site = ("Google" :: "Baidu" :: Nil)
    println(site:+"liusong":+"abc":+"N/A")
    print(site)

    println("3677674774".toLong)

    var s : String = "E:\\2016年7月17日优盘备份\\Ⅱ-1图纸\\华南城\\2016-4-22\\地库2016年7月17日版本\\华南城地库土建.GCL10"
    println(s.indexOf("\\"))

    var aaa  = new Array[String](17)

    var dt:DateTime = new DateTime()
    var yesterday:DateTime = dt.minusDays(1)
    println(yesterday.toString("yyyyMMdd").toInt)

    println(0 % 10000)

    val name = "\\xF0\\xA8\\x9F\\xA0\\xE9\\x95"

    println(name.replace("\\",""))

    println("0" < "2016/08/04 11:28:00")

    println("5.100".toDouble)


    println((new util.Random).nextInt(10))
    println((new util.Random).nextInt(10))
    println((new util.Random).nextInt(10))
    println((new util.Random).nextInt(10))
    println((new util.Random).nextInt(10))
    println((new util.Random).nextInt(10))


  }

}
