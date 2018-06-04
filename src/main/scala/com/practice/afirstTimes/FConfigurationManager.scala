package com.practice.afirstTimes

import org.apache.hadoop.conf.Configuration

object FConfigurationManager {

  //这个配置对象会自动加载你项目中的core-site.xml文件的配置项目替换掉默认的配置项
  val config = new Configuration()
  config.addResource("mysql-site.xml")
  def getProperty(key: String) = {
    config.get(key)
  }

}
