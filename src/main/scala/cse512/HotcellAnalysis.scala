package cse512
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)
  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
  {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    //pickupInfo.show()
    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ",(pickupTime: String)=>((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
    //pickupInfo.show()
    ////println(pickupInfo.count())
    pickupInfo.createOrReplaceTempView("pickupInfo")

    var countTable = spark.sql("SELECT COUNT(*) AS pickups,x,y,z FROM pickupInfo GROUP BY x,y,z")
    countTable.createOrReplaceTempView("countTable")


    var squareTable = spark.sql("SELECT pickups*pickups AS pickupsSquared FROM countTable")
    squareTable.createOrReplaceTempView("squareTable")

    var Sval = spark.sql("SELECT SUM(pickupsSquared) FROM squareTable")


    val Snum = Sval.collect()(0).getLong(0);

    // Define the min and max of x, y, z
    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)
    var allPickups = spark.sql("SELECT COUNT(*) FROM pickupInfo")
    val allPickupsLong = allPickups.collect()(0).getLong(0)
    val allPickupsTotal = allPickupsLong.toDouble

    val X = (allPickupsTotal/numCells)

    val S = Math.sqrt((Snum/numCells)-Math.pow(X,2))


    //val GiDenominator = S*Math.sqrt(((numCells*27)-Math.pow(27,2))/(numCells-1))

    val resultsTable = spark.sql("SELECT countTable.x,countTable.y,countTable.z, count(distinct total.x, total.y, total.z ) as countOf, SUM(total.pickups) AS totalPickups\nFROM countTable \nJOIN countTable AS total  \t\t ON total.x <= (countTable.x +1) AND total.x >= (countTable.x - 1)\n\t\t\t\t\t\t\t\t AND total.y <= (countTable.y +1) AND total.y >= (countTable.y - 1)\n\t\t\t\t\t\t\t\t AND total.z <= (countTable.z +1) AND total.z >= (countTable.z - 1)\n\n\n\n\nGROUP BY\ncountTable.x,\ncountTable.y,\ncountTable.z")

    def edgeCount(x:Double,y:Double,z:Double) : Integer = {
      var edgeCount = 0
      if (x < minX+1) {edgeCount = edgeCount + 1}
      if (x > maxX-1) {edgeCount = edgeCount + 1}
      if (y < minY+1) {edgeCount = edgeCount + 1}
      if (y > maxY-1) {edgeCount = edgeCount + 1}
      if (z < minZ+1) {edgeCount = edgeCount + 1}
      if (z > maxZ-1) {edgeCount = edgeCount + 1}
      if (edgeCount == 0) {return 27}
      if (edgeCount == 1) {return 18}
      if (edgeCount == 2) {return 12}
      return 8
    }

    val myXYZ = udf(edgeCount _)

    var resultsTable2 = resultsTable.withColumn("edgeCount", myXYZ(col("x"),col("y"),col("z")))

    resultsTable2.createOrReplaceTempView("resultsTable2")

    val myTop = udf((s:Double,c:Double) => {((s) - (X*(c)))/(S*Math.sqrt(((numCells*(c)-Math.pow((c),2))/(numCells-1))))})

    resultsTable2 = resultsTable2.withColumn("giScore", myTop( col("totalPickups"),col("edgeCount")))

    resultsTable2.createOrReplaceTempView("resultsTable2")

    val top50 = spark.sql("SELECT x,y,z FROM resultsTable2 ORDER BY giScore DESC limit 50")
    top50.show(10000)

    ////println(minX)
    //val S =
    ////println(S)
    ////println(minX)
    ////println(maxX)
    //for ( a  <- minX.toInt to maxX.toInt){
    //}
    // YOU NEED TO CHANGE THIS PART
    return top50 // YOU NEED TO CHANGE THIS PART
  }



}