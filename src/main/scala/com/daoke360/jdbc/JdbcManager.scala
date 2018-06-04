package com.daoke360.jdbc

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util
import com.daoke360.common.GlobalContants
import com.daoke360.conf.ConfigurationManager

/**
  * 创建数据库连接对象,定义通用的增,删,改,查方法
  */
object JdbcManager {

  //定义一个数据库连接池对象
  private val connectionQune = new util.LinkedList[Connection]()
  /**
    * 从数据库连接池获取连接对象
    */
  def getConnection() = {
    if (connectionQune.size() == 0){
      //创建连接对象,并将其加入到连接池中
      val driver = ConfigurationManager.getProperty(GlobalContants.JDBC_DRIVER)
      val url = ConfigurationManager.getProperty(GlobalContants.JDBC_URL)
      val user = ConfigurationManager.getProperty(GlobalContants.JDBC_USER)
      val password = ConfigurationManager.getProperty(GlobalContants.JDBC_PASSWORD)
      val dataSoourceSize = ConfigurationManager.getProperty(GlobalContants.JDBC_DATA_SOURCE_SIZE).toInt
      //注册驱动
      Class.forName(driver)
      for (i <- 0 until(dataSoourceSize)){
        val connection = DriverManager.getConnection(url,user,password)
        connectionQune.push(connection)
      }
    }
    //从连接池中获取连接对象
    connectionQune.poll()
  }

  /**
    * 归还数据库连接对象到连接池中,以便下次进行复用
    */
  def returnConnection(connection: Connection) = {
    connectionQune.push(connection)
  }

  /**
    * 定义通用的增,删,改方法
    */
  def excuteUpdate(sql: String,params:Array[Any]) = {
    var connection: Connection = null
    var pstmt:PreparedStatement = null
    try{
      connection = getConnection()
      //创建一个执行sql命令对象
      pstmt = connection.prepareStatement(sql)
      for (i <- 0 until(params.length)){
        pstmt.setObject(i + 1,params(i))
      }
      pstmt.executeUpdate()
    }catch {
      case e:Exception => throw e
    }finally {
      if (pstmt != null)
        pstmt.close()
      if (connection != null)
        returnConnection(connection)
    }
  }

  /**
    * 定义增，删，改批处理方法
    */
  def executeBatch(sql:String,paramsArray:Array[Array[Any]]) = {
    var connection:Connection = null
    var pstmt: PreparedStatement = null
    try{
      connection = getConnection()
      //设置手动提交事务
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement(sql)
      for (i <- 0 until(paramsArray.length)){
        var params = paramsArray(i)
        for (j <- 0 until(params.length)){
          pstmt.setObject(j + 1,params(j))
        }
        pstmt.addBatch()
      }
      //执行sql
      pstmt.executeBatch()
      //提交事务
      connection.commit()
    }catch {
      case e: Exception =>{
        //事物回滚
        connection.rollback()
        throw e
      }
    }finally {
      if (pstmt != null)
        pstmt.close()
      if (connection != null)
        returnConnection(connection)
    }
  }

  /**
    * 定义通过查询方法
    */
  def executeQuery(sql:String,params:Array[Any],f:(ResultSet) => Unit) = {
    var connection : Connection = null
    var pstmt : PreparedStatement = null
    var resultSet: ResultSet = null
    try{
      connection = getConnection()
      pstmt = connection.prepareStatement(sql)
      for (i <- 0 until(params.length)){
        pstmt.setObject(i + 1,params(i))
      }
      resultSet = pstmt.executeQuery()
      f(resultSet)
    }catch {
      case e : Exception => throw e
    } finally{
      if (resultSet != null)
        resultSet.close()
      if (pstmt != null)
        pstmt.close()
      if (connection != null)
        returnConnection(connection)
    }
  }

}
