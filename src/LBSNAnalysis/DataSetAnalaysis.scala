package LBSNAnalysis

import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable.ListBuffer

/**
 * Created by XXX on 11-04-XXX.
 */
class DataSetAnalaysis {
  def stringToDate(dateString: String): Date = {
    val updateDate = dateString.replaceAll("T", "").replaceAll("Z", "")
    val formatter: SimpleDateFormat = new SimpleDateFormat("yyyy-mm-ddHH:mm:ss")
    val date: Date = formatter.parse(updateDate)
    return date
  }

  def getAnalysis(checkinFile: String, friendsFile: String): Unit = {
    val ckLines = scala.io.Source.fromFile(checkinFile).getLines().map(t => t.split("\t"))
      .map(t => (t(0).toLong, stringToDate(t(1)), t(2).toDouble, t(3).toDouble, t(4), t(5).toLong, t(1))).toList.distinct
    val visitors = ckLines.map(t => t._1).distinct
    val locations = ckLines.map(t => t._6).distinct
    val filteredLocs = ckLines.groupBy(t => t._6).filter(t => t._2.size >= 50).map(t => t._1).toList.distinct
    println("filtered locs are ::" + filteredLocs.size)
    println("total Checkins::" + ckLines.size)
    println("total visitors::" + visitors.size)
    println("total locs::" + locations.size)

    val friendslines = scala.io.Source.fromFile(friendsFile).getLines().map(t => t.split("\t"))
      .map(t => (t(0).toLong, t(1).toLong)).toList.distinct //Get friends
    val users: ListBuffer[Long] = new ListBuffer[Long]()
    friendslines.foreach { fl =>
      users += fl._1
      users += fl._2
    }
    println("friend links::" + friendslines.size)
    println("users size::" + users.distinct.size)
  }

}
