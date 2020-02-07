package LBSNAnalysis

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable.ListBuffer

/**
 * Created by XXX on 17-XXX-XXX.
 */
/** Find the probability of a user that he/check-ins at a location if at least "filterSize" number of his/her
  * friends have checked-in at the location before.
  * We find probability of each user
  * */
class ProbabilityToFollowFriends {
  var filterSize = 1
  var windowSize: Long = 5
  //in days
  var windowSizeList = List(30, 180, 365, 965)
  //days
  var filterSizeList = List(1, 5, 10)
  // friend size
  var Users: List[Long] = null
  var Location: List[Long] = null
  var LocationCheckins: Map[(Long), List[(Long, Date)]] = Map()
  var usersFriends: Map[(Long), List[(Long)]] = Map()
  var usersVisitedLocations: Map[(Long), List[Long]] = Map()

  def getFriends(friendsLines: List[(Long, Long)]): Unit = {
    usersFriends = friendsLines.groupBy(t => t._1).map { t =>
      var f = ListBuffer[Long]()
      t._2.foreach { ftuple =>
        f += ftuple._2
      }
      (t._1, f.toList)
    }
  }

  def stringToDate(dateString: String): Date = {
    val updateDate = dateString.replaceAll("T", "").replaceAll("Z", "")
    val formatter: SimpleDateFormat = new SimpleDateFormat("yyyy-mm-ddhh:mm:ss")
    val date: Date = formatter.parse(updateDate)
    return date
  }

  def getUsersLocations(users: List[Long]): List[Long] = {
    var usersLocations: List[Long] = List()
    users.foreach { u =>
      usersLocations = usersLocations ::: usersVisitedLocations.getOrElse(u, List())
    }
    val locationFrequency = usersLocations.groupBy(l => l).map(l => (l._1, l._2.size))
    val filteredLocations = locationFrequency.filter(t => t._2 > filterSize).map(t => t._1).toList
    return filteredLocations
  }

  def getMaxIntervalList(list: List[(Long, Date)], element: Long): Array[Long] = {
    var maxIntervalArray: Array[List[Long]] = new Array(windowSizeList.size)
    var maxIACount: Array[Long] = new Array(windowSizeList.size)
    maxIntervalArray = maxIntervalArray.map(t => List())
    var temp: Array[List[(Long, Date)]] = new Array(windowSizeList.size)
    temp = temp.map(t => List())
    list.foreach { e =>
      if (e._1 != element) {
        for (a <- 0 until (windowSizeList.size)) {
          temp(a) = temp(a) :+ e
        }
      }
      else if (e._1 == element) {
        for (a <- 0 until windowSizeList.size) {
          temp(a) = temp(a).filter { t => (e._2.getTime - t._2.getTime).toDouble / (1000 * 60 * 60 * 24) <= windowSizeList(a)}
          val tempUsers = temp(a).map(t => t._1).distinct.intersect(usersFriends.getOrElse(element, List()))
          if (tempUsers.size > maxIntervalArray(a).size) {
            maxIntervalArray(a) = tempUsers
            temp(a) = List()
          }
          else temp(a) = List()
        }
      }
    }
    for (i <- 0 until maxIntervalArray.size) {
      maxIACount(i) = maxIntervalArray(i).size
    }
    return maxIACount
  }

  def computeProbabilityToFollowFriends(friendsPath: String, checkinFilePath: String, writePath: String, //minUserCk= minimum check-ins of users
                                        minUserCk: Long, maxUserCk: Long, minUserLocs: Long, minLocCk: Long, maxLocCk: Long, minLocVisitors: Long): Unit = {
    val fd = new FilterData()
    val filterData = fd.filterLBSNData(friendsPath, checkinFilePath,
      minUserCk, maxUserCk, minUserLocs, minLocCk, maxLocCk, minLocVisitors) // get the filtered data with given thresholds
    val writerOrder = new PrintWriter(new File(writePath))
    Users = filterData._2.map(t => t._1).toList.distinct //initialize users with the filtered visitors
    getFriends(filterData._1) // initialize the friends of each users, global variable

    usersVisitedLocations = filterData._2.groupBy(t => t._1).map { t =>
      (t._1, t._2.map(l => l._6).distinct) // get all the visited locations for each user - UN-ORDERED !!
    }

    /** get visitors of each location ordered by their check-in time
     there can be duplicate users in the list because user may check-in twice on same location
      */
    LocationCheckins = filterData._2.groupBy(t => t._6).map { l =>
      (l._1, l._2.sortBy(u => u._2).map(inUser => (inUser._1, inUser._2))) //(l._1,orderedUsers)
    }
    val allowedUsers = filterData._2.groupBy(t => t._1).map(t => (t._1, t._2.map(it => it._6)))
      .filter(t => t._2.size > 1 && t._2.distinct.size > 1).map(t => t._1).toList.distinct
    Users = allowedUsers
    var count = 0
    val totalCount = Users.size
    /** Main Computation */
    var userLocations: List[Long] = List()
    var locVisitors: List[(Long, Array[Long])] = List()
    var visitors: List[(Long, Date)] = List()
    var previousVisitorsCount: Array[Long] = new Array(windowSizeList.size)
    writerOrder.println("user" + "\t" + "windowSize" + "\t" + "FriendSize" + "\t" + "Probability")
    Users.foreach { u =>
      count = count + 1
      println("count,totalCount" + count, totalCount)
      userLocations = usersVisitedLocations.getOrElse(u, List()).distinct //get all distinct locations of the user
      locVisitors = userLocations.map { t =>
        visitors = LocationCheckins.getOrElse(t, List()) //get all the visitors of the locations that is visited by this user
        previousVisitorsCount = new Array(windowSizeList.size)
        for (i <- 0 until windowSizeList.size) {
          previousVisitorsCount(i) = 0
        }
        if (visitors.size > 0) {
          //get max sequence i.e., where user has followed maximum number of locations that are visited by users/friends
          previousVisitorsCount = getMaxIntervalList(visitors, u)
        }
        (t, previousVisitorsCount)
      }
      for (b <- 0 until filterSizeList.size) {
        for (a <- 0 until windowSizeList.size) {
          val filtLocs = locVisitors.filter(t => t._2(a) >= filterSizeList(b))
          val probability = filtLocs.size.toDouble / userLocations.distinct.size.toDouble
          writerOrder.println(u + "\t" + windowSizeList(a) + "\t" + filterSizeList(b) + "\t" + probability)
        }
      }
    }
    println("pff Finished !!")
    writerOrder.close()
  }
}
