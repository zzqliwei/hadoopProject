package com.westar.douban

object MovieDataParser {
  def parseline(line: String): Movie = {
    val fields = line.split("\t")
    val movieId = fields(0)
    val movieName = fields(1)
    val year = fields(2).toInt
    val directors = getItems(fields(3))
    val scriptsWriters = getItems(fields(4))
    val stars = getItems(fields(5)).map(_.split("更多...")(0))
    val category = getItems(fields(6))
    val nations = getItems(fields(7))
    val language = getItems(fields(8))
    val showtime = fields(9).toInt
    val initialReleaseDateMap = getItems(fields(10)).map { dateStr =>
      val index1 = dateStr.indexOf("(")
      val index2 = dateStr.indexOf(")")
      if (index1 > 0 && index2 > 0)
        (dateStr.substring(index1 + 1, index2), dateStr.substring(0, index1))
      else
        ("-", dateStr)
    }.toMap
    val commentNum = fields(11).toInt
    val commentScore = fields(12).toFloat
    val summary = fields(13)
    Movie(movieId, movieName, year, directors, scriptsWriters,
      stars, category, nations, language, showtime, initialReleaseDateMap,
      commentNum, commentScore, summary)
  }

  private def getItems(content: String) :Array[String] = content.split(" / ").map(_.trim)

}

case class Movie(movieId: String, movieName: String, year: Int,  directors: Array[String], scriptsWriters: Array[String],
                 stars: Array[String], category: Array[String], nations: Array[String], language: Array[String],
                 showtime: Int, initialReleaseDateMap: Map[String, String], commentNum: Int, commentScore: Float, summary: String)
