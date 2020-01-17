package com.westar.streaming


import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  * Internal Redis client for managing Redis connection {@link Jedis} based on {@link RedisPool}
  */
object InternalRedisClient extends Serializable {

  @transient private lazy val pool: JedisPool = {
    val maxTotal = 10
    val maxIdle = 10
    val minIdle = 1
    val redisHost = "slave1"
    val redisPort = 6379
    val redisTimeout = 30000
    val poolConfig = new GenericObjectPoolConfig()
    poolConfig.setMaxTotal(maxTotal)
    poolConfig.setMaxIdle(maxIdle)
    poolConfig.setMinIdle(minIdle)
    poolConfig.setTestOnBorrow(true)
    poolConfig.setTestOnReturn(false)
    poolConfig.setMaxWaitMillis(100000)


    val hook = new Thread{
      override def run = pool.destroy()
    }
    sys.addShutdownHook(hook.run)

    new JedisPool(poolConfig, redisHost, redisPort, redisTimeout)
  }

  def getPool: JedisPool = {
    assert(pool != null)
    pool
  }

  def main(args: Array[String]): Unit = {
    val p = getPool

    val j = p.getResource

    j.set("much", "")
  }
}
