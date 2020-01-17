package com.westar.submit.kryo

import com.esotericsoftware.kryo.Kryo
import com.westar.spark.rdd.Dog
import org.apache.spark.serializer.KryoRegistrator

class CustomKryoRegistrator extends KryoRegistrator{
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Dog])
  }
}
