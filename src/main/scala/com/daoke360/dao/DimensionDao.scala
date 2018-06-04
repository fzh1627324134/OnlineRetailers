package com.daoke360.dao

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException, Statement}
import com.daoke360.domain.{DateDimension, LocationDimension}

object DimensionDao {

  private def buildDateSql() = {
    Array[String](
      "select id from dimension_date where `year`=? and season=? and `month`=? and `week`=? and `day`=? and calendar=? and type=?",
      "insert into dimension_date(`year`,season,`month`,`week`,`day`,calendar,type)values(?,?,?,?,?,?,?)"
    )
  }

  private def buildLocationSql() = {
    Array[String](
      "select id from dimension_location where country=? and province=? and city=?",
      "insert into dimension_location(country,province,city)values(?,?,?)"
    )
  }

  private def setArgs(pstmt: PreparedStatement, dimension: Any) = {
    if (dimension.isInstanceOf[DateDimension]) {
      val dateDimension = dimension.asInstanceOf[DateDimension]
      pstmt.setObject(1, dateDimension.year)
      pstmt.setObject(2, dateDimension.season)
      pstmt.setObject(3, dateDimension.month)
      pstmt.setObject(4, dateDimension.week)
      pstmt.setObject(5, dateDimension.day)
      pstmt.setObject(6, dateDimension.calendar)
      pstmt.setObject(7, dateDimension.dateType)
    } else if (dimension.isInstanceOf[LocationDimension]) {
      val locationDimension = dimension.asInstanceOf[LocationDimension]
      pstmt.setObject(1, locationDimension.country)
      pstmt.setObject(2, locationDimension.province)
      pstmt.setObject(3, locationDimension.city)
    }
  }

  private def executeSql(sqls: Array[String], dimension: Any, connnection: Connection) = {
    var pstmt: PreparedStatement = null
    var resultSet: ResultSet = null
    try {
      val querySql = sqls(0)
      pstmt = connnection.prepareStatement(querySql)
      setArgs(pstmt, dimension)
      resultSet = pstmt.executeQuery()
      if (resultSet.next()) {
        resultSet.getInt("id")
      } else {
        val insertSql = sqls(1)
        //插入记录并返回这条记录生成的id
        pstmt = connnection.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS)
        setArgs(pstmt, dimension)
        if (pstmt.executeUpdate() > 0) {
          resultSet = pstmt.getGeneratedKeys
          if (resultSet.next()) {
            resultSet.getInt(1)
          } else {
            throw new SQLException("从数据库中维度id失败...")
          }
        } else {
          throw new SQLException("插入维度信息失败...")
        }
      }
    } catch {
      case e: SQLException => throw e
    } finally {
      if (pstmt != null)
        pstmt.close()
      if (resultSet != null)
        resultSet.close()
    }


  }

  def getDimensionId(dimension: Any, connnection: Connection) = {
    var sqls: Array[String] = null
    if (dimension.isInstanceOf[DateDimension]) {
      sqls = buildDateSql()
    } else if (dimension.isInstanceOf[LocationDimension]) {
      sqls = buildLocationSql()
    }

    synchronized({
      executeSql(sqls, dimension, connnection)
    })
  }
}
