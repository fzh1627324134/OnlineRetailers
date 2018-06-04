package com.practice.afirstTimes

import java.net.URLDecoder

import org.apache.commons.lang.StringUtils

import scala.collection.mutable

object FLogAnalysisUtils {

  /**
    * 处理请求参数
    */
  private def handlerRequestBody(requestBody:String,eventLogMap:mutable.HashMap[String,String]) = {
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
    * 处理ip，解析成地域信息
    */
  def handlerIP(eventLogMap:mutable.HashMap[String,String],ipRules:Array[FIPRule]) = {
    val ip = eventLogMap(FEventLogContants.LOG_COLUMN_NAME_IP)
    val regionInfo = FIPAnalysisUtils.analysisIP(ip,ipRules)
    eventLogMap.put(FEventLogContants.LOG_COLUMN_NAME_COUNTRY,regionInfo.country)
    eventLogMap.put(FEventLogContants.LOG_COLUMN_NAME_PROVINCE,regionInfo.province)
    eventLogMap.put(FEventLogContants.LOG_COLUMN_NAME_CITY,regionInfo.city)
  }

  /**
    * 对单条日志进行解析返回一个hashMap
    */
  def analysisLog(LogText:String,ipRules:Array[FIPRule]) = {
    var eventLogMap:mutable.HashMap[String,String] = null
    if (StringUtils.isNotBlank(LogText)){
      val filed = LogText.split(FGlobalContants.LOG_SPLIT_FLAG)
      if (filed.length == 4){
        eventLogMap = new mutable.HashMap[String,String]()
        eventLogMap.put(FEventLogContants.LOG_COLUMN_NAME_IP,filed(0))
        eventLogMap.put(FEventLogContants.LOG_COLUMN_NAME_ACCESS_TIME,FUtils.nginxTime2Long(filed(1)).toString)
        val requestBody = filed(3)
        handlerRequestBody(requestBody,eventLogMap)
        handlerIP(eventLogMap,ipRules)
      }
    }
    eventLogMap
  }

}
