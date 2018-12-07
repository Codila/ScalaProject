import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.min

/** Find the day with the most precipitatoon in year 1800*/
object MostPrecDay {
  
  def parseLine(line:String)= {
    val fields = line.split(",")
    val day       = fields(1).takeRight(2)
    val entryType = fields(2)
    val prec = fields(3)
    (day, entryType, prec)  // returns a tuple of 3 values
  }
    /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MostPrecDay")
    
    // Read each line of input data
    val lines = sc.textFile("../1800.csv")
    
    // Convert to (DAY, entryType, PREC) tuples
    val parsedLines = lines.map(parseLine)
    
    // Filter out all but Prec entries
    val maxPRCP = parsedLines.filter(x => x._2 == "PRCP")// Filter in the 2nd field of the passed tuple
    
    // Convert to (DAY, PRCP)
    val DayPRCP = maxPRCP.map(x => (x._1.toInt, x._3.toInt))// now we have a key value rdd
    
    // Reduce by DayID retaining the max PRCP found
    val maxPRCPByDay = DayPRCP.reduceByKey( (x,y) => math.max(x,y))
    
    // Collect, format, and print the results
    val results = maxPRCPByDay.collect()
    
    for (result <- results.sorted) {
       val Day = result._1
       val PRCP = result._2
       println(s"$Day max precipitation: $PRCP") 
    }
      
  }
}
