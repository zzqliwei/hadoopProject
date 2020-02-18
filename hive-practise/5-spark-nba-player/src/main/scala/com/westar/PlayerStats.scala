package com.westar

case class PlayerStats(year: Int, name: String, position: String, age: Int, team: String,
                       GP: Int, GS: Int, MP: Double, rawStats: RawStats,
                       zScoreStats: Option[ZScoreStats], totalZScores: Double = 0,
                       nzScoreStats: Option[ZScoreStats], totalNZScore: Double = 0)

case class RawStats(FG: Double, FGA: Double, FGP: Double, threeP: Double,
                    threePA: Double, threePP: Double, twoP: Double,
                    twoPA: Double, twoPP: Double, eFGP: Double, FT: Double,
                    FTA: Double, FTP: Double, ORB: Double, DRB: Double,
                    TRB: Double, AST: Double, STL: Double, BLK: Double,
                    TOV: Double, PF: Double, PTS: Double)

case class ZScoreStats(FGP: Double, FTP: Double, threeP: Double, TRB: Double, AST: Double,
                       STL: Double, BLK: Double, TOV: Double, PTS: Double)

object PlayerStats {
  /**
   * 解析原始数据
   * @param line
   * @return
   */
  def parse(line :String):Option[PlayerStats] = {
    try {
      val fields = line.replaceAll("\"", "").split(",")
      val yearStr = fields(0)
      val name = fields(2)
      val position = fields(3)
      val age = fields(4).toInt
      val team = fields(5)
      val GP = fields(6).toInt
      val GS = fields(7).toInt
      val MP = fields(8).toDouble
      val stats = fields.slice(9, 31).map(x => x.toDouble)
      val rawStats = RawStats(stats(0), stats(1), stats(2), stats(3), stats(4), stats(5), stats(6), stats(7),
        stats(8), stats(9), stats(10), stats(11), stats(12), stats(13), stats(14), stats(15),
        stats(16), stats(17), stats(18), stats(19), stats(20), stats(21))
      Some(PlayerStats(yearStr.toInt, name, position, age, team, GP, GS, MP, rawStats, None, nzScoreStats = None))
    } catch {
      case e: NumberFormatException => {
        Console.err.println(s"error when parse : ${line}")
        None
      }
    }
  }

  /**
   * 计算得分情况
   * @param playerStats
   * @param aggStatsMap
   * @return
   */
  def calculateZScore(playerStats: PlayerStats, aggStatsMap: Map[String, Double]):PlayerStats = {
    val yearStr = playerStats.year.toString
    val rawStats = playerStats.rawStats
    //（投篮命中率  - 总的投篮命中率平均值 ）* 投篮出手次数
    val fgw = (rawStats.FGP - aggStatsMap(yearStr + "_fgp_avg")) * rawStats.FGA
    //罚球命中率
    val ftw = (rawStats.FTP - aggStatsMap(yearStr + "_ftp_avg")) * rawStats.FTA
    //3分球命中
    val zthreeP = (rawStats.threeP - aggStatsMap(yearStr + "_tp_avg")) / aggStatsMap(yearStr + "_tp_stddev")
    //
    val ztrb = (rawStats.TRB - aggStatsMap(yearStr + "_trb_avg")) / aggStatsMap(yearStr + "_trb_stddev")
    //助攻次数
    val zast = (rawStats.AST - aggStatsMap(yearStr + "_ast_avg")) / aggStatsMap(yearStr + "_ast_stddev")
    //抢断次数
    val zstl = (rawStats.STL - aggStatsMap(yearStr + "_stl_avg")) / aggStatsMap(yearStr + "_stl_stddev")
    //盖帽次数
    val zblk = (rawStats.BLK - aggStatsMap(yearStr + "_blk_avg")) / aggStatsMap(yearStr + "_blk_stddev")
    //失误次数
    val ztov = (rawStats.TOV - aggStatsMap(yearStr + "_tov_avg")) / aggStatsMap(yearStr + "_tov_stddev") * (-1)
    //总得分
    val zpts = (rawStats.PTS - aggStatsMap(yearStr + "_pts_avg")) / aggStatsMap(yearStr + "_pts_stddev")

    val zScoreStats = ZScoreStats(fgw, ftw, zthreeP, ztrb, zast, zstl, zblk, ztov, zpts)
    playerStats.copy(zScoreStats = Some(zScoreStats))
  }

  def calculateNZScore(playerStats: PlayerStats, zStats: Map[String, Double]): PlayerStats = {
    val yearStr = playerStats.year.toString
    val zScoreStats = playerStats.zScoreStats.get
    val zfgp = (zScoreStats.FGP - zStats(yearStr + "_fgw_avg")) / zStats(yearStr + "_fgw_stddev")
    val zftp = (zScoreStats.FTP - zStats(yearStr + "_ftw_avg")) / zStats(yearStr + "_ftw_stddev")
    val nzfgp = statNormalize(zfgp,
      (zStats(yearStr + "_fgw_max") - zStats(yearStr + "_fgw_avg")) / zStats(yearStr + "_fgw_stddev"),
      (zStats(yearStr + "_fgw_min") - zStats(yearStr + "_fgw_avg")) / zStats(yearStr + "_fgw_stddev"))
    val nzftp = statNormalize(zftp,
      (zStats(yearStr + "_ftw_max") - zStats(yearStr + "_ftw_avg")) / zStats(yearStr + "_ftw_stddev"),
      (zStats(yearStr + "_ftw_min") - zStats(yearStr + "_ftw_avg")) / zStats(yearStr + "_ftw_stddev"))
    val nzthreeP = statNormalize(zScoreStats.threeP, zStats(yearStr + "_tp_max"),  zStats(yearStr + "_tp_min"))
    val trbN = statNormalize(zScoreStats.TRB, zStats(yearStr + "_trb_max"), zStats(yearStr + "_trb_min"))
    val astN = statNormalize(zScoreStats.AST, zStats(yearStr + "_ast_max"), zStats(yearStr + "_ast_min"))
    val stlN = statNormalize(zScoreStats.STL, zStats(yearStr + "_stl_max"), zStats(yearStr + "_stl_min"))
    val blkN = statNormalize(zScoreStats.BLK, zStats(yearStr + "_blk_max"), zStats(yearStr + "_blk_min"))
    val tovN = statNormalize(zScoreStats.TOV, zStats(yearStr + "_tov_max"), zStats(yearStr + "_tov_min"))
    val ptsN = statNormalize(zScoreStats.PTS, zStats(yearStr + "_pts_max"), zStats(yearStr + "_pts_min"))
    val statsZ = zScoreStats.copy(FGP = zfgp, FTP = zftp)
    val totalZScores = zScoreStats.productIterator.map(_.asInstanceOf[Double]).reduce(_ + _)
    val statsNZ = ZScoreStats(nzfgp, nzftp, nzthreeP, trbN, astN, stlN, blkN, tovN, ptsN)
    val totalNZScore = statsNZ.productIterator.map(_.asInstanceOf[Double]).reduce(_ + _)
    playerStats.copy(zScoreStats = Some(statsZ), totalZScores = totalZScores,
      nzScoreStats = Some(statsNZ), totalNZScore = totalNZScore)
  }

  private def statNormalize(stats: Double, maxValue: Double, minValue: Double) = {
    import Math._
    val absMax = max(abs(maxValue), abs(minValue))
    stats / absMax
  }

}
