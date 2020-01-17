package com.westar.dataset.sql.innnerfunction

import org.apache.spark.sql.SparkSession

object MiscFunctionTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MiscFunctionTest")
      .master("local[*]")
      .getOrCreate()

    //assert_true 用法 assert_true(expr) 返回元素expr是否为true，如果expr不为true，则抛异常
    spark.sql("SELECT assert_true(0 < 1)").show(false) //输出：null

    //crc32 用法 crc32(expr) 返回expr的cyclic redundancy check value
    spark.sql("SELECT crc32('spark')").show(false) //输出：2635321133

    //md5 用法 md5(expr) Returns an MD5 128-bit checksum as a hex string of `expr`
    spark.sql("SELECT md5('spark')").show(false) //输出：98f11b7a7880169c3bd62a5a507b3965

    //hash 用法 hash(expr1, expr2, ...) 返回expr1, expr2, ...的hash值
    spark.sql("SELECT hash('Spark', array(123), 2)").show(false) //输出：-1321691492

    //sha 用法 sha(expr) Returns a sha1 hash value as a hex string of the `expr`
    //sha1的用法和sha是一样的
    spark.sql("SELECT sha('Spark')").show(false) //输出：85f5955f4b27a9a4c2aab6ffe5d7189fc298b92c

    //sha2 用法 sha2(expr, bitLength) Returns a checksum of SHA-2 family as a hex string of `expr`.
    // SHA-224, SHA-256, SHA-384, and SHA-512 are supported. Bit length of 0 is equivalent to 256
    spark.sql("SELECT sha2('Spark', 256)").show(false) //输出：529bc3b07127ecb7e53a4dcf1991d9152c24537d919178022b2c42657f79a26b
    //spark_partition_id 用法 spark_partition_id() 返回当前数据的partition id
    spark.sql("SELECT spark_partition_id()").show(false) //输出：0

    //input_file_name 用法 input_file_name() 返回当前正在读取数据的文件名字
    spark.sql("SELECT input_file_name()").show(false) //输出：""

    //monotonically_increasing_id 用法 monotonically_increasing_id()
    //生成递增的唯一id
    spark.sql("SELECT monotonically_increasing_id()").show(false) //输出：0

    //current_database 用法 current_database() 返回当前的database
    spark.sql("SELECT current_database()").show(false) //输出：default

    //reflect 用法 reflect(class, method[, arg1[, arg2 ..]]) 利用反射调用class中的方法methon
    //java_method和reflect功能是一样的
    spark.sql("SELECT reflect('java.util.UUID', 'randomUUID')").show(false) //输出：8f3d20fa-4e0f-4ef9-9935-5972cc5b0d79
    spark.sql("SELECT reflect('java.util.UUID', 'fromString', 'a5cf6c42-0c85-418f-af6c-3e4e5b1328f2')").show(false) //输出：a5cf6c42-0c85-418f-af6c-3e4e5b1328f2

    spark.stop()
  }

}
