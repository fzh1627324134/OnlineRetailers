package com.daoke360.conf

import org.apache.hadoop.conf.Configuration

object ConfigurationManager {

  //这个配置对象会自动加载你项目中的core-site.xml文件的配置项替换掉默认的配置项
  val config = new Configuration()
  config.addResource("mysql-site.xml")

  def getProperty(key: String) = {
    config.get(key)
  }

}
