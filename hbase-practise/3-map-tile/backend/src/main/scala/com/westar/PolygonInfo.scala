package com.westar
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.{ParseException, WKTReader}

case class PolygonInfo(id: String, layerName: String, height: String, polygon: Geometry)

object PolygonInfo {
  def parse(line:String):Option[PolygonInfo] = {
    val lineRegex = """([-.\d]+),([-.\d]+),([^"]+),([-.\d]+),([-.\d]+),(POLYGON\(\([^"]+\)\)),([-.\d]+),(POLYGON\(\([^"]+\)\))""".r
    line match {
      case lineRegex(_, id, layerName, _, _, polygonStr, height, _) => {
        try {
          val polygon = new WKTReader().read(polygonStr)
          Some(PolygonInfo(id, layerName, height, polygon))
        } catch {
          case e: ParseException =>
            System.err.println(s"error when parse: ${polygonStr}, the exception is : ${e}")
            None
        }
      }
      case _ => {
        System.err.println(s"regex not match : ${line}")
        None
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val line = "5573,60314393,成片房屋,92.295326259,375.9898,POLYGON((512038.92 3345986.92,512032.47 3345994.99,512032.45 3346014.16,512042.32 3346022.17,512047.86 3346015.34,512040.32 3346009.22,512050.68 3345996.47,512038.92 3345986.92)),99,POLYGON((120.125066888744 30.2333271765329,120.124999974843 30.2334000371469,120.124999985841 30.2335729632883,120.125102612338 30.2336451208817,120.125160086915 30.2335834548583,120.125081687335 30.233528323244,120.125189166964 30.2334132071293,120.125066888744 30.2333271765329))"
    val polygonInfo = parse(line)

    println(polygonInfo)
  }

}
