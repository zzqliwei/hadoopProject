package com.westar

object Test {
  def main(args: Array[String]): Unit = {
    import scala.collection.mutable.Map
    "MyyyyDoggggg".foldLeft(Map.empty[Char,Int])((map,c) => map +=(c->(map.getOrElse(c,0) + 1)))

    import scala.collection.mutable.Map
    val result: (Map[Char, Int], Option[Char]) = "MyyyyDoggggg".foldLeft((Map.empty[Char, Int], Option.empty[Char])){
      case ((map,previousCharOpt),currentChar) =>
        if(previousCharOpt.nonEmpty && previousCharOpt.get == currentChar){
          map += (currentChar -> (map.getOrElse(currentChar,1) + 1))
        }else{
          map += (currentChar -> 1)
        }
        (map,Some(currentChar))
    }
    println(result._1)
  }

}
