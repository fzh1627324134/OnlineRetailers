package com.practice.testMain

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisUtils {

  val config = new JedisPoolConfig
  val jedisPool = new JedisPool(config,"hadoop3",6380,0)
  def getJedis():Jedis = {
    jedisPool.getResource
  }

  def main(args: Array[String]): Unit = {
    val jedisClient = getJedis()
    jedisClient.set("key","hello word")
    println(jedisClient.get("key"))
    jedisClient.close()
  }

}
