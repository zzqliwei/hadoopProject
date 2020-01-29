package com.westar.hbase.spark

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, RegionLocator, Table}
import org.apache.hadoop.hbase.ipc.RpcControllerFactory
import org.apache.hadoop.hbase.security.{User, UserProvider}
import org.apache.hadoop.hbase.{HConstants, TableName}
import org.apache.spark.internal.Logging

import scala.collection.mutable

private[spark] object HBaseConnectionCache extends Logging {
  // 维护一个Map来缓存一个Hbase的连接

  val connectionMap = new mutable.HashMap[HBaseConnectionKey, SmartConnection]()

  // in milliseconds
  private var timeout = 50000L
  private var closed: Boolean = false

  var housekeepingThread = new Thread(new Runnable {
    override def run(): Unit = {
      while (true){
        try {
          Thread.sleep(timeout)
        }catch {
          case e:InterruptedException =>
        }

        if(closed){
          return
        }
        performHousekeeping(false)

      }
    }
  })
  housekeepingThread.setDaemon(true)
  housekeepingThread.start()

  def performHousekeeping(forceClean: Boolean) = {
    val tsNow: Long = System.currentTimeMillis()
    connectionMap.synchronized{
      connectionMap.foreach{
        x  => {
          if(x._2.refCount < 0){
            logError(s"Bug to be fixed: negative refCount of connection ${x._2}")
          }
          if(forceClean || ((x._2.refCount <= 0) && (tsNow - x._2.timestamp > timeout))) {
            try{
              x._2.connection.close()
            } catch {
              case e: IOException => logWarning(s"Fail to close connection ${x._2}", e)
            }
            connectionMap.remove(x._1)
          }
        }
      }
    }
  }

  def close():Unit = {
      try{
        connectionMap.synchronized{
          if (closed)
            return
          closed = true
          housekeepingThread.interrupt()
          housekeepingThread = null
          HBaseConnectionCache.performHousekeeping(true)
        }
      }catch {
        case e:Exception =>
      }
  }


  def getConnection(key: HBaseConnectionKey, conn: => Connection):SmartConnection ={
    connectionMap.synchronized{
      if(closed){
        return null;
      }
      val sc =connectionMap.getOrElseUpdate(key,new SmartConnection(conn))
      sc.refCount += 1
      sc
    }
  }

  def getConnection(config:Configuration):SmartConnection = {
    getConnection(new HBaseConnectionKey(config),ConnectionFactory.createConnection(config))
  }

  def setTimeout(to:Long):Unit ={
    connectionMap.synchronized{
      if(closed){
        return
      }
      timeout = to
      housekeepingThread.interrupt();

    }
  }

}

private[hbase] case class SmartConnection (connection: Connection, var refCount: Int = 0, var timestamp: Long = 0) {
  def getTable(tableName: TableName): Table = connection.getTable(tableName)
  def isClosed: Boolean = connection.isClosed
  def getAdmin: Admin = connection.getAdmin
  def close() = {
    HBaseConnectionCache.connectionMap.synchronized {
      refCount -= 1
      if(refCount <= 0)
        timestamp = System.currentTimeMillis()
    }
  }
}


/**
  *  唯一标识一个HBase Connection的Key
  *  主要是看Configuration这个对象中的配置，如果这个对象的配置没有变(内容没变)，那么就是相同的key
  */
class HBaseConnectionKey(c:Configuration) extends Logging{
  val conf: Configuration = c
  val CONNECTION_PROPERTIES: Array[String] = Array[String](
    HConstants.ZOOKEEPER_QUORUM,
    HConstants.ZOOKEEPER_ZNODE_PARENT,
    HConstants.ZOOKEEPER_CLIENT_PORT,
    HConstants.HBASE_CLIENT_PAUSE,
    HConstants.HBASE_CLIENT_RETRIES_NUMBER,
    HConstants.HBASE_RPC_TIMEOUT_KEY,
    HConstants.HBASE_META_SCANNER_CACHING,
    HConstants.HBASE_CLIENT_INSTANCE_ID,
    HConstants.RPC_CODEC_CONF_KEY,
    HConstants.USE_META_REPLICAS,
    RpcControllerFactory.CUSTOM_CONTROLLER_CONF_KEY)

  var username: String = _
  var m_properties = mutable.HashMap.empty[String, String]
  if (conf != null) {
    for (property <- CONNECTION_PROPERTIES) {
      val value: String = conf.get(property)
      if (value != null) {
        m_properties.+=((property, value))
      }
    }
    try {
      val provider: UserProvider = UserProvider.instantiate(conf)
      val currentUser: User = provider.getCurrent
      if (currentUser != null) {
        username = currentUser.getName
      }
    }
    catch {
      case e: IOException => {
        logWarning("Error obtaining current user, skipping username in HBaseConnectionKey", e)
      }
    }
  }

  // 使得m_properties不可变
  val properties = m_properties.toMap
  override def hashCode: Int = {
    val prime: Int = 31
    var result: Int = 1
    if (username != null) {
      result = username.hashCode
    }
    for (property <- CONNECTION_PROPERTIES) {
      val value: Option[String] = properties.get(property)
      if (value.isDefined) {
        result = prime * result + value.hashCode
      }
    }
    result
  }

  override def equals(obj: Any): Boolean = {
    if (obj == null) return false
    if (getClass ne obj.getClass) return false
    val that: HBaseConnectionKey = obj.asInstanceOf[HBaseConnectionKey]
    if (this.username != null && !(this.username == that.username)) {
      return false
    }
    else if (this.username == null && that.username != null) {
      return false
    }
    if (this.properties == null) {
      if (that.properties != null) {
        return false
      }
    }
    else {
      if (that.properties == null) {
        return false
      }
      var flag: Boolean = true
      for (property <- CONNECTION_PROPERTIES) {
        val thisValue: Option[String] = this.properties.get(property)
        val thatValue: Option[String] = that.properties.get(property)
        flag = true
        if (thisValue eq thatValue) {
          flag = false //continue, so make flag be false
        }
        if (flag && (thisValue == null || !(thisValue == thatValue))) {
          return false
        }
      }
    }
    true
  }

  override def toString: String = {
    "HBaseConnectionKey{" + "properties=" + properties + ", username='" + username + '\'' + '}'
  }
}
