package com.practice.afirstTimes

import scala.util.control.Breaks._

object FIPAnalysisUtils {

  /**
    * 查找ip对应的地域信息
    */
  def analysisIP(ip:String,ipRules:Array[FIPRule]) ={

    val regionInfo = FRegionInfo()
    val numIP = ip2Long(ip)
    val index = binnerySearch(numIP,ipRules)
    if (index > -1) {
      val ipRule = ipRules(index)
      regionInfo.country = ipRule.country
      regionInfo.province = ipRule.province
      regionInfo.city = ipRule.city
    }
    regionInfo
  }

  /**
    * 通过二分查找发中ip对应的地域信息索引
    */
  private def binnerySearch(numIP:Long,ipRules:Array[FIPRule]) ={
    var min = 0
    var max = ipRules.length - 1
    var index = -1
    breakable({
      while (min <= max){
        var minddle = (min + max) / 2
        val ipRule = ipRules(minddle)
        if(numIP >= ipRule.startIP && numIP <= ipRule.endIP){
          index = minddle
          break()
        }else if (numIP < ipRule.startIP){
          max = minddle - 1
        }else if (numIP > ipRule.endIP){
          min = minddle + 1
        }
      }
    })
    index
  }

  /**
    *将ip转换为十进制
    */
  private def ip2Long(ip:String):Long = {
    var numIP:Long = 0
    val fileds = ip.split("[.]")
    for (i <- 0 until(fileds.length)){
      numIP = numIP << 8 | fileds(i).toLong
    }
    numIP
  }

}
