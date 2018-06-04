package com.daoke360.task.analysislog.utils

import java.net.URLDecoder
import com.daoke360.caseclass.IPRule
import com.daoke360.common.{EventLogContants, GlobalContants}
import com.daoke360.utils.Utils
import org.apache.commons.lang.StringUtils
import scala.collection.mutable

object LogAnalysisUtils {
  /**
    * 处理请求参数
    */
  private def handlerRequestBody(requestBody: String,eventLogMap: mutable.HashMap[String,String]) = {
    val fields = requestBody.split("[?]")
    if (fields.length == 2){
      val requestParams = fields(1)
      val items = requestParams.split("&")
      for (item <- items){
        val kv = item.split("=")
        if (kv.length == 2){
          val key = URLDecoder.decode(kv(0),"utf-8")
          val value = URLDecoder.decode(kv(1),"utf-8")
          eventLogMap.put(key,value)
        }
      }
    }
  }

  /**
    * 处理ip,解析成地域信息
    */
  def handlerIP(eventLogMap: mutable.HashMap[String,String],ipRules:Array[IPRule]) = {
    val ip = eventLogMap(EventLogContants.LOG_COLUMN_NAME_IP)
    val regionInfo = IPAnalysisUtils.analysisIP(ip,ipRules)
    eventLogMap.put(EventLogContants.LOG_COLUMN_NAME_COUNTRY,regionInfo.country)
    eventLogMap.put(EventLogContants.LOG_COLUMN_NAME_PROVINCE,regionInfo.province)
    eventLogMap.put(EventLogContants.LOG_COLUMN_NAME_CITY,regionInfo.city)
  }

  /**
    * 对单条日志进行解析返回一个hashMap
    */
  def analysisLog(logText: String,ipRules:Array[IPRule]) = {
    var eventLogMap: mutable.HashMap[String,String] = null
    if (StringUtils.isNotBlank(logText)){
      val field = logText.split(GlobalContants.LOG_SPLIT_FLAG)
      //标准日志是4段
      if (field.length == 4){
        eventLogMap = new mutable.HashMap[String,String]()
        //用户ip
        eventLogMap.put(EventLogContants.LOG_COLUMN_NAME_IP,field(0))
        //用户访问时间
        eventLogMap.put(EventLogContants.LOG_COLUMN_NAME_ACCESS_TIME,Utils.nginxTime2Long(field(1)).toString)
        val requestBody = field(3)
        //处理请求参数
        handlerRequestBody(requestBody,eventLogMap)
        //处理ip
        handlerIP(eventLogMap,ipRules)
      }
    }
    eventLogMap
  }

}
