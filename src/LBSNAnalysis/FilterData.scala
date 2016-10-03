package LBSNAnalysis

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

/**
 * Created by MAamir on 23-03-2016.
 */
class FilterData {
  def countLocations(checkinFilePath: String): Unit = {
    val ckLines = scala.io.Source.fromFile(checkinFilePath).getLines().map(t => t.split("\t"))
      .map(t => (t(5).toLong)).toList.distinct
    println("total location of the dataset are:" + ckLines.size)

  }

  def countSpatialGraphEdges(saptialEdgesPath: String): Unit = {
    val SELines = scala.io.Source.fromFile(saptialEdgesPath).getLines().map(t => t.split("\t"))
      .map(t => (t(0).toLong, t(1).toLong)).toList.map { t =>
      if (t._1 <= t._2) (t._1, t._2)
      else (t._2, t._1)
    }.filter(t => t._1 != t._2)
    println("Count of edges are ::" + SELines.size)

  }

  def stringToDate(dateString: String): Date = {
    val updateDate = dateString.replaceAll("T", "").replaceAll("Z", "")
    val formatter: SimpleDateFormat = new SimpleDateFormat("yyyy-mm-ddhh:mm:ss")
    val date: Date = formatter.parse(updateDate)
    return date
  }

  def filterLocations(checkinFilePath: String): Unit = {
    val minVisitorsSize: Long = 20L
    val lines = scala.io.Source.fromFile(checkinFilePath).getLines().map(t => t.split("\t"))
      .map(t => (t(0).toLong, stringToDate(t(1)), t(2).toDouble, t(3).toDouble, t(4), t(5).toLong, t(1))).toList.distinct //.toArray
    val allowedLocsGroup = lines.groupBy(t => t._6).map(t => (t._1, t._2.map(it => it._1).distinct.size))
        .filter(t => t._2 > minVisitorsSize).map(t => t._1)
    /*allowedLocsGroup.foreach{loc=>
      println(loc._1,loc._2)
    }*/
  }

  def filterUsers(checkinFilePath: String): Unit = {
    val minVLocSize: Long = 20L
    //val minVisitorsSize:Long=20L
    val lines = scala.io.Source.fromFile(checkinFilePath).getLines().map(t => t.split("\t"))
      .map(t => (t(0).toLong, stringToDate(t(1)), t(2).toDouble, t(3).toDouble, t(4), t(5).toLong, t(1))).toList.distinct //.toArray

    val allowedUsers = lines.groupBy(t => t._1).map(t => (t._1, t._2.map(it => it._6).distinct.size))
      .filter(t => t._2 > minVLocSize).map(t => t._1)
    /*allowedUsers.foreach{u=>
      println(u)
    }*/
  }

  def filterLBSNData(friendsFilePath: String, checkinFilePath: String, //minUserCk= minimum check-ins of users
                     minUserCk: Long, maxUserCk: Long, minUserLocs: Long, minLocCk: Long, maxLocCk: Long, minLocVisitors: Long)
  : (List[(Long, Long)], List[(Long, Date, Double, Double, String, Long, String)]) = {
    /** Location check-ins */
    val ckLines = scala.io.Source.fromFile(checkinFilePath).getLines().map(t => t.split("\t"))
      .map(t => (t(0).toLong, stringToDate(t(1)), t(2).toDouble, t(3).toDouble, t(4), t(5).toLong, t(1))).toList.distinct
    val allowedUsers = ckLines.groupBy(t => t._1).map(t => (t._1, t._2.map(it => it._6))) // add distinct for unique locations not check-ins
      .filter(t => t._2.size > minUserCk && t._2.size < maxUserCk && t._2.distinct.size > minUserLocs).map(t => t._1).toList
    val allowedLocs = ckLines.groupBy(t => t._6).map(t => (t._1, t._2.map(it => it._1)))
      .filter(t => t._2.size > minLocCk && t._2.size < maxLocCk && t._2.distinct.size > minLocVisitors).map(t => t._1).toList
    val filteredCkLines = ckLines.filter(t => allowedUsers.contains(t._1) && allowedLocs.contains(t._6))
    //result doesn't guarantee the tuple with the given threshold, it only excludes the tuple that doesn't satisfies these threshold
    //while excluding check-ins and users also miss some tuple
    /** Friends */
    val friendslines = scala.io.Source.fromFile(friendsFilePath).getLines().map(t => t.split("\t"))
      .map(t => (t(0).toLong, t(1).toLong)).toList.distinct
    val filteredFriendsLines: List[(Long, Long)] = friendslines.filter(t => allowedUsers.contains(t._1) && allowedUsers.contains(t._2))
    val filteredDataLines = (filteredFriendsLines, filteredCkLines)
    return filteredDataLines
  }

  def findCorrelation(friendsFilePath: String, checkinFilePath: String, //minUserCk= minimum check-ins of users
                      minUserCk_FD: Long, maxUserCk_FD: Long, minUserLocs_FD: Long, minLocCk_FD: Long, maxLocCk_FD: Long, minLocVisitors_FD: Long, writeFile: String): Unit = {
    var usersFriends: Map[(Long), List[(Long)]] = Map()
    var allowedUsers: List[Long] = List()
    val writerOrder = new PrintWriter(new File(writeFile))
    writerOrder.println("user1,user2,friends,correlation")
    //val minVLocSize:Long=20L
    //val minVisitorsSize:Long=20L
    // friends original
    val filteredDataTuple = filterLBSNData(friendsFilePath, checkinFilePath,
      minUserCk_FD, maxUserCk_FD, minUserLocs_FD, minLocCk_FD, maxLocCk_FD, minLocVisitors_FD)
    val filteredCKLines = filteredDataTuple._2
    /*val friendslines=scala.io.Source.fromFile(friendsFilePath).getLines().map(t=> t.split("\t"))
      .map(t=> (t(0).toLong,t(1).toLong)).toList.distinct//
    usersFriends=friendslines.groupBy(t=> t._1).map(t=> (t._1,t._2.map(it=> it._2).distinct))*/
    val filteredFriendsLines = filteredDataTuple._1
    usersFriends = filteredFriendsLines.groupBy(t => t._1).map(t => (t._1, t._2.map(it => it._2).distinct))
    //original checkins
    /*val ckLines=scala.io.Source.fromFile(checkinFilePath).getLines().map(t=> t.split("\t"))
      .map(t=> (t(0).toLong, stringToDate(t(1)),t(2).toDouble,t(3).toDouble,t(4),t(5).toLong,t(1))).toList.distinct//.toArray
    //original visitors checkins
    //val visitorsLocations=ckLines.groupBy(t=> t._1).map(t=> (t._1,t._2.map(it=> it._6)))
    var allowedUsers=ckLines.groupBy(t=> t._1).map(t=> (t._1,t._2.map(it=> it._6).size))// add distinct for unique locations not checkins
        .filter(t=> t._2 >minVLocSize).map(t=> t._1).toList

    // allowed locations
    val allowedLocs=ckLines.groupBy(t=> t._6).map(t=> (t._1,t._2.map(it=> it._1).size))
      .filter(t=> t._2 >minVisitorsSize).map(t=> t._1).toList
    println("before total Lines"+ckLines.size)
    // to be used check-ins
    val filteredLines=ckLines.filter(t=> allowedUsers.contains(t._1) && allowedLocs.contains(t._6))
    println("after lines::"+filteredLines.size)*/

    val visitorsLocations = filteredCKLines.groupBy(t => t._1).map(t => (t._1, t._2.map(it => it._6)))
    println("visitors locations size ::" + visitorsLocations.size)
    //val au=filteredLines.groupBy(t=> t._1).map(t=> (t._1,t._2.map(it=> it._6).distinct.size))
    allowedUsers = filteredCKLines.groupBy(t => t._1).map(t => (t._1, t._2.map(it => it._6).size))
      .filter(t => t._2 > 30).map(t => t._1).toList
    println("initially users size::" + allowedUsers.size)
    val totalSize = allowedUsers.size
    //val usersPair:List[(Long,Long,Boolean)]=List()
    allowedUsers = allowedUsers.sortBy(t => t)
    var corr: Double = 0.0
    //var u1=0L
    //var u2=0L
    var firstVisits: List[Long] = List()
    var secondVisits: List[Long] = List()
    //val friendsPair=ListBuffer[(Long,Long,Boolean)]()
    for (i <- 0 until allowedUsers.size) {
      println("count,total is " + i, totalSize)
      //u1=allowedUsers(i)
      firstVisits = visitorsLocations.getOrElse(allowedUsers(i), List())
      for (j <- i + 1 until allowedUsers.size) {
        secondVisits = visitorsLocations.getOrElse(allowedUsers(j), List())
        var bool = false
        //friendsPair :+ (allowedUsers(i),allowedUsers(j))
        if (usersFriends.getOrElse(allowedUsers(i), List()).contains(allowedUsers(j))) {
          // ||
          //usersFriends.getOrElse(allowedUsers(j),List()).contains(allowedUsers(i)) ){
          bool = true
          //friendsPair :+ (allowedUsers(i),allowedUsers(j),true)
        } else {
          bool = false
          //friendsPair :+ (allowedUsers(i),allowedUsers(j),false)
        }
        corr = firstVisits.intersect(secondVisits).size.toDouble / (firstVisits.size + secondVisits.size).toDouble
        writerOrder.println(allowedUsers(i) + "," + allowedUsers(j) + "," + bool + "," + corr)
        //friendsPair += ((allowedUsers(i),allowedUsers(j),bool))
        //println("u1,u2::"+allowedUsers(i),allowedUsers(j),bool)
      }
    }
    /*println("friends paris size::"+friendsPair.size)
    /*var corr:Double=0.0
    var firstVisits:List[Long]=List()
    var secondVisits:List[Long]=List()*/
    val friendsCorr=friendsPair.map{t=>
      firstVisits=visitorsLocations.getOrElse(t._1,List())
      //println("user, first visits"+firstVisits)
      secondVisits=visitorsLocations.getOrElse(t._2,List())
      corr=firstVisits.intersect(secondVisits).size.toDouble/(firstVisits.size + secondVisits.size).toDouble
      writerOrder.println(t._1+","+t._2+","+t._3+","+corr)
      //println("u1,u2,corr"+t._1,t._2,t._3,corr)
      (t._1,t._2,t._3,corr)


    }*/
    writerOrder.close()



    //val allowedLocs=filterLocations(lines,minVisitorsSize)

  }

}
