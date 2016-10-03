package LBSNAnalysis
import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

/**
 * Created by MAamir on 21-03-2016.
 */
class CheckinsAnalysis {
  def stringToDate(dateString: String): Date = {
    val updateDate = dateString.replaceAll("T", "").replaceAll("Z", "")
    val formatter: SimpleDateFormat = new SimpleDateFormat("yyyy-mm-ddhh:mm:ss")
    val date: Date = formatter.parse(updateDate)
    return date
  }

  def getCheckInAnalysis(checkinFilePath: String, writeFileUser: String, writeFileLocation: String): Unit = {
    val lines = scala.io.Source.fromFile(checkinFilePath).getLines().map(t => t.split("\t"))
      .map(t => (t(0).toLong, stringToDate(t(1)), t(2).toDouble, t(3).toDouble, t(4), t(5).toLong, t(1))).toList.distinct //.toArray
    val writerOrderUser = new PrintWriter(new File(writeFileUser))
    val writerOrderLocation = new PrintWriter(new File(writeFileLocation))
    writerOrderLocation.println("Location,TotalVisitors,DistinctVisitors")
    writerOrderUser.println("User,TotalVisitedLocations,DistinctVisitedLocations")


    /** For Users */
    val groupByUsers = lines.groupBy(t => t._1)
    groupByUsers.par.foreach { t =>
      val uniqueLocsSize = t._2.map(u => u._6).distinct
      //println("unique Locations are ::"+uniqueLocsSize)
      val totalLocSize: Int = t._2.size
      //println("location,TotalUsers,DistinctUsers"+t._1,totalLocSize,uniqueLocsSize)//
      writerOrderUser.println(t._1 + "," + totalLocSize + "," + uniqueLocsSize.size)
    }


    /** For Locations */
    val groupByLocations = lines.groupBy(t => t._6)
    groupByLocations.par.foreach { t =>
      val uniqueUsersSize: Int = t._2.map(u => u._1).distinct.size
      val totalSize: Int = t._2.size
      //println("location,TotalUsers,DistinctUsers"+t._1,totalSize,uniqueUsersSize)//
      writerOrderLocation.println(t._1 + "," + totalSize + "," + uniqueUsersSize)
    }
    writerOrderLocation.close()
    writerOrderUser.close()


  }

}
