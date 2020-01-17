package com.westar.dataset.sql.innnerfunction

import org.apache.spark.sql.SparkSession

object MathFunctionTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SqlApiTest")
      .getOrCreate()

    //acos 用法：acos(expr) - 如果-1<=`expr`<=1则返回the inverse cosine (a.k.a. arccosine) of `expr` 否则返回 NaN.
    spark.sql("SELECT acos(1)").show() //输出为1
    spark.sql("SELECT acos(2)").show() //输出为NaN

    //asin 用法：asin(expr) - 如果-1<=`expr`<=1则返回the inverse sine (a.k.a. arcsine) of `expr` 否则返回 NaN.
    spark.sql("SELECT asin(0)").show() //输出为0.0
    spark.sql("SELECT asin(2)").show() //输出为NaN

    //atan 用法：atan(expr) - 返回the inverse tangent (a.k.a. arctangent).
    spark.sql("SELECT atan(0)").show() //输出为0.0

    //atan2 用法：atan2(expr1, expr2)
    // - Returns the angle in radians between the positive x-axis of a plane and the point given by the coordinates (`expr1`, `expr2`).
    spark.sql("SELECT atan2(0, 0)").show() //输出为0.0

    //bin 用法：bin(expr) - 返回Long类型的expr的二进制的字符串数据
    spark.sql("SELECT bin(13)").show() //输出为：1101
    spark.sql("SELECT bin(-13)").show() //输出为1111111111111111111111111111111111111111111111111111111111110011
    spark.sql("SELECT bin(13.3)").show() //输出为：1101

    //bround 用法：bround(expr, d) - 按照四舍五入的规则保留expr的小数点后d位
    spark.sql("SELECT bround(2.6, 0)").show() //输出为3

    //cbrt 用法：cbrt(expr) - 返回expr的立方根
    spark.sql("SELECT cbrt(27)").show() //返回3.0

    //ceil 用法：ceil(expr) - 返回不比expr小的最小的整数
    spark.sql("SELECT ceil(-0.1)").show() //返回0
    spark.sql("SELECT cbrt(5)").show() //返回5

    //ceiling 用法：和cell是一样的

    //cos 用法： cos(expr) - Returns the cosine of `expr`.
    spark.sql("SELECT cos(0)").show() //返回1.0

    //cosh 用法：cosh(expr) - Returns the hyperbolic cosine of `expr`.
    spark.sql("SELECT cosh(0)").show() //返回1.0

    // conv 用法：conv(num, from_base, to_base) - Convert `num` from `from_base` to `to_base`.
    //表示将2进制的100转换成十进制
    spark.sql("SELECT conv('100', 2, 10)").show() // 4
    spark.sql("SELECT conv(-10, 16, -10)").show() //16

    // degrees 用法： degrees(expr) - Converts radians(弧度) to degrees(角度).
    spark.sql("SELECT degrees(3.141592653589793)").show() //180.0

    //e 用法： e() - Returns Euler's number, e.
    spark.sql("SELECT e()").show() //输出为2.718281828459045

    //exp 用法：exp(expr) - Returns e to the power of `expr`.
    spark.sql("SELECT exp(0)").show() //输出为1.0

    //expm1 用法： expm1(expr) - Returns exp(`expr`) - 1.
    spark.sql("SELECT expm1(0)").show() //输出为0

    // floor 用法： floor(expr) - 返回不比expr大的最大整数.
    spark.sql("SELECT floor(-0.1)").show() //输出-1
    spark.sql("SELECT floor(5.4)").show() //输出5

    //factorial 用法：factorial(expr) 返回expr的阶乘，expr的取值范围为[0,20]，超过这个范围就返回null
    spark.sql("SELECT factorial(5)").show() //输出120

    //hex 用法：hex(expr) 将expr转化成16进制
    spark.sql("SELECT hex(17)").show() //输出11
    spark.sql("SELECT hex(Spark SQL)").show() //输出537061726B2053514C

    //sqrt 用法：sqrt(expr)返回expr的平方根
    spark.sql("SELECT sqrt(4)").show() //输出是2.0

    //hypot 用法：返回hypot(`expr1`**2 + `expr2`**2)
    spark.sql("SELECT hypot(3, 4)").show() //输出是5.0

    //log 用法：log(base, expr) - Returns the logarithm of `expr` with `base`
    spark.sql("SELECT log(10, 100)").show() //输出是2.0

    //log10 用法：log10(expr) - Returns the logarithm of `expr` with base 10
    spark.sql("SELECT log10(10)").show() //输出是1.0

    //log1p 用法：log1p(expr) - Returns log(1 + `expr`)
    spark.sql("SELECT log1p(0)").show() //输出是0

    //log2 用法：log2(expr) - Returns the logarithm of `expr` with base 2
    spark.sql("SELECT log2(2)").show() //输出是1.0

    //ln 用法：ln(expr) - Returns the natural logarithm (base e) of `expr`
    spark.sql("SELECT ln(1)").show() //输出是0.0

    //negative 用法：negative(expr)返回expr的相反数
    spark.sql("SELECT negative(1)").show() //输出是-1

    //pi 用法：pi() 返回PI的值
    spark.sql("SELECT pi()").show() //输出是3.141592653589793

    //pmod 用法：pmod(expr1, expr2) - 返回expr1与expr2取模的正数.
    spark.sql("SELECT pmod(10, 3)").show() //输出是1
    spark.sql("SELECT pmod(-10, 3)").show() //输出是2

    //positive 用法：positive(expr)返回expr
    spark.sql("SELECT positive(-10)").show() //输出是-10

    //power和pow一样 用法：pow(expr1, expr2) - 返回expr1的expr2次方
    spark.sql("SELECT pow(2, 3)").show() //输出是2的3次方，即8
    spark.sql("SELECT power(2, 3)").show() //输出是2的3次方，即8

    //radians 用法：radians(expr) - 将角度转成弧度
    spark.sql("SELECT radians(180)").show() //输出是3.141592653589793

    //rint 用法：rint(expr) - 返回最接近expr的整数的浮点型数据
    spark.sql("SELECT rint(12.3456)").show() //输出是12.0

    //round 用法：round(expr, d) - 四舍五入将expr精确到d位
    spark.sql("SELECT round(2.5444, 2)").show() //输出是2.54
    spark.sql("SELECT round(2.2, 0)").show() //输出是2

    //shiftleft 用法：shiftleft(base, expr) - 将base按位左移expr位
    spark.sql("SELECT shiftleft(3, 1)").show() //输出是二进制的3按位向左移1位，即6

    //shiftright 用法：shiftright(base, expr) - 将base按位右移expr位
    spark.sql("SELECT shiftright(3, 1)").show() //输出是二进制的3按位向右移1位，即1

    //shiftrightunsigned 用法：shiftrightunsigned(base, expr) - 将base按位无符号右移expr位
    spark.sql("SELECT shiftrightunsigned(3, 1)").show() //输出是二进制的3按位无符号向右移1位，即1

    //signum和sign一样 用法：sign(expr) - 如果expr为0则返回0，如果expr为负数则返回-1，如果expr是正数则返回1
    spark.sql("SELECT sign(40)").show() //输出是1
    spark.sql("SELECT signum(40)").show() //输出是1

    //sin 用法：sin(expr) - Returns the sine of `expr`
    spark.sql("SELECT sin(0)").show() //输出是0

    //sinh 用法：sinh(expr) - Returns the hyperbolic sine of `expr`
    spark.sql("SELECT sinh(0)").show() //输出是0

    //str_to_map 用法：str_to_map(text[, pairDelim[, keyValueDelim]]) -
    // Creates a map after splitting the text into key/value pairs using delimiters. Default delimiters are ',' for `pairDelim` and ':' for `keyValueDelim`.
    spark.sql("SELECT str_to_map('a:1,b:2,c:3', ',', ':')").show() //输出是Map(a -> 1, b -> 2, c -> 3)

    //tan 用法：tan(expr) - Returns the tangent of `expr`
    spark.sql("SELECT tan(0)").show() //输出是0

    //tanh 用法：tanh(expr) - Returns the hyperbolic tangent of `expr`
    spark.sql("SELECT tanh(0)").show() //输出是0

    //+ 用法：expr1 + expr2 - Returns `expr1`+`expr2`
    spark.sql("SELECT 1 + 2").show() //输出是3

    //- 用法：expr1 - expr2 - Returns `expr1`-`expr2`
    spark.sql("SELECT 1 - 2").show() //输出是-1

    //* 用法：expr1 * expr2 - Returns `expr1`*`expr2`
    spark.sql("SELECT 1 * 2").show() //输出是2

    // / 用法：expr1 / expr2 - Returns `expr1`/`expr2`
    spark.sql("SELECT 1 / 2").show() //输出是0.5

    // %(取余数) 用法：expr1 % expr2 - Returns `expr1`%`expr2`
    spark.sql("SELECT 1 % 2").show() //输出是1
  }

}
