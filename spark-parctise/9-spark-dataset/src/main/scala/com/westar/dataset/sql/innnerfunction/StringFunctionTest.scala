package com.westar.dataset.sql.innnerfunction

import com.westar.dataset.TestData
import org.apache.spark.sql.SparkSession

/**
 * ascii
 * base64
 * concat
 * concat_ws
 * decode
 * encode
 * elt
 * find_in_set
 * format_string
 * format_number
 * get_json_object
 * initcap
 * instr
 */
object StringFunctionTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("StringFunctionTest")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = spark.sparkContext.parallelize(
      (1 to 100).map(i => TestData(i,i.toString))).toDF()
    df.createOrReplaceTempView("testData")

    //ascii 用法：ascii(str) 返回字符串str第一个字母的数值
    spark.sql("SELECT ascii('222')").show()//50
    spark.sql("SELECT ascii(2)").show()//50

    //base64 用法：base64(bin) 返回二进制的bin的base 64 string
    spark.sql("SELECT base64('222')").show()//输出：MjIy

    //concat 用法：concat(str1, str2, ..., strN) 返回str1, str2, ..., strN拼接起来的字符串
    spark.sql("SELECT concat('222', 'spark')").show()//222spark

    //concat_ws 用法：concat_ws(sep, [str | array(str)]+) 返回str1, str2, ..., strN拼接起来的字符串, 用sep隔开
    spark.sql("SELECT concat_ws(' ','222', 'spark')").show()//222  spark

    //decode 用法：decode(bin, charset) 用charset来解码bin
    spark.sql("SELECT decode(encode('abc', 'utf-8'),'utf-8')").show()

    //encode 用法：encode(bin, charset) 用charset来编码bin
    spark.sql("SELECT encode('abc', 'utf-8')").show()

    //elt 用法：elt(n, str1, str2, ...) 返回str1, str2, ...中的第n个字符串,从1开始
    spark.sql("SELECT elt(1,'scala', 'java')").show()//输出：scala

    //find_in_set 用法：find_in_set(str, str_array) 返回str在str_array中的位置(1表示第一个)
    spark.sql("SELECT find_in_set('ab','abc,b,ab,c,def')").show()//输出：3

    //format_string 用法：format_string(strfmt, obj, ...) 返回str在str_array中的位置(1表示第一个)
    spark.sql("SELECT format_string('Hello World %d %s', 100, 'days')").show() //输出：Hello World 100 days

    //format_number 用法：format_number(expr1, expr2) 将expr1的小数点format到expr2个
    spark.sql("SELECT format_number(12332.123456, 4)").show() //输出：12,332.1235

    //get_json_object 用法：get_json_object(json_txt, path)
    spark.sql("SELECT get_json_object('{\"a\":\"b\"}', '$.a')").show() //输出：b

    //initcap 用法：initcap(str), 使的单词的第一个字母大写，其他字母小写，每个单词是以空格隔开
    spark.sql("SELECT initcap('sPark sql')").show() //输出：Spark Sql

    //instr 用法：instr(str, substr) 返回substr在str中出现的位置(1 based)
    spark.sql("SELECT instr('SparkSQL', 'SQL')").show() //输出：6

    //lcase 用法：lcase(str) 返回str的小写化之后的字符串
    //lower和lcase功能是一样的
    spark.sql("SELECT lcase('SparkSql')").show() //输出：sparksql

    //ucase 用法：ucase(str) 将str换成大写
    //upper的功能和ucase一样
    spark.sql("SELECT ucase('SparkSql')").show() //输出：SPARKSQL

    //length 用法：length(str) 返回str的长度
    spark.sql("SELECT length('SparkSql')").show() //输出：9

    //levenshtein 用法：levenshtein(str1, str2) 返回str1和str2的Levenshtein distance
    //编辑距离（Edit Distance），又称Levenshtein距离，是指两个字串之间，由一个转成另一个所需的最少编辑操作次数。
    // 许可的编辑操作包括将一个字符替换成另一个字符，插入一个字符，删除一个字符。一般来说，编辑距离越小，两个串的相似度越大。
    spark.sql("SELECT levenshtein('kitten', 'sitting')").show() //输出：3(因为有3个字母相同)

    //like 用法：expr like str 判断expr是否匹配str
    spark.sql("SELECT * from testData where value like '%3%'").show()

    //rlike 用法：expr rlike str 判断expr是否正则匹配str
    spark.sql("SELECT * from testData where value rlike '\\\\d+'").show()

    //locate 用法：locate(substr, str[, pos]) 返回substr在从pos开始的str后的首次出现的位置
    spark.sql("SELECT locate('bar', 'foobarbar', 5)").show() //输出：7

    //lpad 用法：lpad(str, len, pad) 将str左拼接pad到长度为len，如果str的长度大于len的话，则返回截取长度为len的str
    spark.sql("SELECT lpad('hi', 5, '??')").show() //输出：???hi
    spark.sql("SELECT lpad('hi', 1, '??')").show() //输出：h

    //rpad 用法：rpad(str, len, pad) 将str右拼接pad到长度为len，如果str的长度大于len的话，则返回截取长度为len的str
    spark.sql("SELECT rpad('hi', 5, '??')").show() //输出：hi???
    spark.sql("SELECT rpad('hi', 1, '??')").show() //输出：h

    //ltrim 用法：ltrim(str) 将str左边的空格都去掉
    spark.sql("SELECT ltrim('    SparkSQL')").show() //输出：SparkSQL

    //rtrim 用法：rtrim(str) 将str右边的空格都去掉
    spark.sql("SELECT rtrim('SparkSQL    ')").show() //输出：SparkSQL

    //trim 用法：trim(str) 将str左右两边的空格都去掉
    spark.sql("SELECT trim('   SparkSQL    ')").show() //输出：SparkSQL

    //json_tuple 用法：json_tuple(jsonStr, p1, p2, ..., pn) 分别提取jsonStr中域p1, p2, ..., pn相对应的值
    spark.sql("SELECT json_tuple('{\\\"a\\\":1, \\\"b\\\":2}', 'a', 'b')").show()

    //parse_url 用法：parse_url(url, partToExtract[, key]) 解析u并提取url中的一个部分
    spark.sql("SELECT parse_url('http://spark.apache.org/path?query=1', 'HOST')").show() //输出：spark.apache.org
    spark.sql("SELECT parse_url('http://spark.apache.org/path?query=1', 'QUERY')").show() //输出：query=1
    spark.sql("SELECT parse_url('http://spark.apache.org/path?query=1', 'QUERY', 'query')").show() //输出：1

    //printf 用法：printf(strfmt, obj, ...) 根据printf-style的形式来格式化strfmt
    spark.sql("SELECT printf('Hello World %d %s', 100, 'days')").show() //输出：Hello World 100 days

    //regexp_extract 用法：regexp_extract(str, regexp[, idx]) 提取正则匹配到字符串`regexp`的字符串
    spark.sql("SELECT regexp_extract('100-200', '(\\\\d+)-(\\\\d+)', 1)").show() //输出：100

    //regexp_replace 用法：regexp_replace(str, regexp, rep) 如果regexp能匹配到str中某部分，则str中的这部分字符串会被rep替换掉
    spark.sql("SELECT regexp_replace('100-200', '(\\\\d+)', 'num')").show() //输出：num-num

    //regexp_replace 用法：regexp_replace(str, regexp, rep) 如果regexp能匹配到str中某部分，则str中的这部分字符串会被rep替换掉
    spark.sql("SELECT regexp_replace('100-200', '(\\\\d+)', 'num')").show() //输出：num-num

    //repeat 用法：repeat(str, n) 返回str重复n次的字符串
    spark.sql("SELECT repeat('123', 2)").show() //输出：123123

    //reverse 用法：reverse(str) 将str反转
    spark.sql("SELECT reverse('Spark SQL')").show() //输出：LQS krapS


    //sentences 用法：sentences(str[, lang, country]) 将字符串str分割成句子数据，每一个句子又分割成由单词组成的数组
    //lang表述语言， country表示国家
    spark.sql("SELECT sentences('Hi there! Good morning.')").show(false) //输出：[WrappedArray(Hi, there), WrappedArray(Good, morning)]
    spark.sql("SELECT sentences('你 好! 早 上好.', 'zh', 'CN')").show(false) //输出：[WrappedArray(你, 好), WrappedArray(早, 上好)]

    //soundex 用法：soundex(str) 返回字符串的Soundex code
    spark.sql("SELECT soundex('Miller')").show() //输出：M460


    //space 用法：space(n) 返回n个空格
    spark.sql("SELECT concat('hi', space(3), 'hek')").show() //输出：hi   hek

    //split 用法：split(str, regex) 对字符串str按照regex切割
    spark.sql("SELECT split('oneAtwoBthreeC', '[ABC]')").show() //输出：["one", "two", "three", ""]

    //substr 用法：substr(str, pos[, len]) 从字符串的pos位置开始对str切割长度为len的字符串
    //substring和substr功能是一样的
    spark.sql("SELECT substr('Spark SQL', 5)").show() //输出：k SQL
    spark.sql("SELECT substr('Spark SQL', -3)").show() //输出：SQL
    spark.sql("SELECT substr('Spark SQL', 5, 1)").show() //输出：k

    //substring_index 用法：substring_index(str, delim, count) 从开始到delim在str出现的count次的地方切割字符串str
    spark.sql("SELECT substring_index('www.apache.org', '.', 2)").show() //输出：www.apache

    //translate 用法：translate(input, from, to) 将input中的from替换成to
    spark.sql("SELECT translate('AaBbCc', 'abc', '123')").show() //输出：A1B2C3

    //xpath 用法：xpath(xml, xpath) 按照xpath从xml中提取对应的数值
    spark.sql("SELECT xpath('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>','a/b/text()')").show() //输出：[b1, b2, b3]

    //xpath_boolean 用法：xpath_boolean(xml, xpath) 如果xpath在xml中存在则返回true
    spark.sql("SELECT xpath_boolean('<a><b>1</b></a>','a/b')").show() //输出：true

    //xpath_double 用法：xpath_double(xml, xpath) 按照xpath提取xml中的double值，如果在xml没有发现xpath则返回零，如果xpath返回的值不是数字则返回NaN
    //xpath_number的功能和xpath_double是一样的
    spark.sql("SELECT xpath_double('<a><b>1</b><b>2</b></a>', 'sum(a/b)')").show() //输出：3.0

    //xpath_float 用法：xpath_float(xml, xpath) 按照xpath提取xml中的float值，如果在xml没有发现xpath则返回零，如果xpath返回的值不是数字则返回NaN
    spark.sql("SELECT xpath_float('<a><b>1</b><b>2</b></a>', 'sum(a/b)')").show() //输出：3.0

    //xpath_int 用法：xpath_int(xml, xpath) 按照xpath提取xml中的int值，如果在xml没有发现xpath则返回零，如果xpath返回的值不是数字则返回0
    spark.sql("SELECT xpath_int('<a><b>1</b><b>2</b></a>', 'sum(a/b)')").show() //输出：3

    //xpath_long 用法：xpath_long(xml, xpath) 按照xpath提取xml中的long值，如果在xml没有发现xpath则返回零，如果xpath返回的值不是数字则返回0
    spark.sql("SELECT xpath_long('<a><b>1</b><b>2</b></a>', 'sum(a/b)')").show() //输出：3.0

    //xpath_short 用法：xpath_short(xml, xpath) 按照xpath提取xml中的short值，如果在xml没有发现xpath则返回零，如果xpath返回的值不是数字则返回0
    spark.sql("SELECT xpath_short('<a><b>1</b><b>2</b></a>', 'sum(a/b)')").show() //输出：3.0

    //xpath_string 用法：xpath_string(xml, xpath) 按照xpath在xml第一次出现的文本值
    spark.sql("SELECT xpath_string('<a><b>b</b><c>cc</c><c>c2</c></a>','a/c')").show() //输出：3.0

    spark.stop()
  }

}
