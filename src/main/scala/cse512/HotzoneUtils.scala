package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    val rectangle = queryRectangle.split(",")
    val lower_x = rectangle(0).toDouble
    val lower_y = rectangle(1).toDouble
    val upper_x = rectangle(2).toDouble
    val upper_y = rectangle(3).toDouble

    val point = pointString.split(",")
    val point_x = point(0).toDouble
    val point_y = point(1).toDouble


    if (lower_x <= point_x & upper_x >= point_x & lower_y <= point_y & upper_y >= point_y){
      return true
    }
    return false
  }



}
