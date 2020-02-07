package LBSNAnalysis

import java.text.SimpleDateFormat
import java.util.Date

/**
 * Created by XXX on 22-XXX-XXX.
 */
class visitorsCorrelation {
  def stringToDate(dateString: String): Date = {
    val updateDate = dateString.replaceAll("T", "").replaceAll("Z", "")
    val formatter: SimpleDateFormat = new SimpleDateFormat("yyyy-mm-ddHH:mm:ss")
    val date: Date = formatter.parse(updateDate)
    return date
  }

  def computeCorrelation(friendsFile: String, checkinsFile: String): Unit = {
    val ckLines = scala.io.Source.fromFile(checkinsFile).getLines().map(t => t.split("\t"))
      .map(t => (t(0).toLong, stringToDate(t(1)), t(2).toDouble, t(3).toDouble, t(4), t(5).toLong, t(1))).toList.distinct //.toArray
    //val locationVisitors=ckLines.groupBy(t=> t._6).map(t=> (t._1,t._2.map(it=> it._1)))
    val visitorsLocations = ckLines.groupBy(t => t._1).map(t => (t._1, t._2.map(it => it._6)))
    //locationVisitors.toList.foreach(t=> println(t))

    var firstUser: Long = 0L
    var secondUser: Long = 0L
    val friendsLines = scala.io.Source.fromFile(friendsFile).getLines().map(t => t.split("\t"))
      .map { t =>
      firstUser = t(0).toLong
      secondUser = t(1).toLong
      if (firstUser < secondUser) (firstUser, secondUser)
      else (secondUser, firstUser)
    }.toList.distinct

    var corr: Double = 0.0
    var firstVisits: List[Long] = List()
    var secondVisits: List[Long] = List()
    val friendsCorr = friendsLines.map { t =>
      firstVisits = visitorsLocations.getOrElse(t._1, List())
      //println("user, first visits"+firstVisits)
      secondVisits = visitorsLocations.getOrElse(t._2, List())
      corr = firstVisits.intersect(secondVisits).size.toDouble / (firstVisits.size + secondVisits.size).toDouble
      (t._1, t._2, corr)
      //println("u1,u2,corr"+t._1,t._2,corr)
    }
    //friendsCorr.foreach(t=> println(t))
    friendsCorr.sortBy(t => -t._3).take(10).foreach(t => println(t))

    /*friendsLines.sortBy(t=> t._1).foreach{t=>
      println(t)
    }*/
  }

}
