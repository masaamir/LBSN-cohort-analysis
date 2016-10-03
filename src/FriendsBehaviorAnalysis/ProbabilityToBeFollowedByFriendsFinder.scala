package FriendsBehaviorAnalysis

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date
import LBSNAnalysis.FilterData
import scala.collection.mutable.ListBuffer

/**
 * Created by MAamir on 4/27/2016.
 */
/** Find out the probability of users that if he/check-ins at a particular places at least #friendSize
  * number of his/her friends will check-in at that place within time window of #windowSize
  * */
class ProbabilityToBeFollowedByFriendsFinder {
  //var friendSize = 1
  var windowSizeList = List(1,7,30,180,365)// in days
  var friendsSizeList = List(1,2,3) // friends
  var usersFriends: Map[(Long), List[(Long)]] = Map()

  def getFriends(FriendsLines: List[(Long, Long)]): Unit = {
    //pathFriends:String,
    usersFriends = FriendsLines.groupBy(t => t._1).map { t =>
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

  def returnMaxArray(temp: Array[List[(Long, Date)]], max: Array[List[Long]]): Unit = {
    var updateCheck = false
    for (i <- 0 until max.size) {
      val tempUsers = temp(i).map(t => t._1).distinct
      if (tempUsers.size > max(0).size) {
        updateCheck = true
      }
    }
    if (updateCheck) {
      // if at least one of index item is more update
      for (i <- 0 until max.size) {
        val tempUsers = temp(i).map(t => t._1).distinct
        max(i) = tempUsers
      }
    }
  }

  def getMaxIntervalList(list: List[(Long, Date)], element: Long): Array[Long] = {
    var maxIntervalArray: Array[List[Long]] = new Array(windowSizeList.size)
    val maxIACount: Array[Long] = new Array(windowSizeList.size)
    maxIntervalArray = maxIntervalArray.map(t => List())
    var temp: Array[List[(Long, Date)]] = new Array(windowSizeList.size)
    temp = temp.map(t => List())
    var userFoundCount = 0 // ignore the users that have checked-in first before the user
    var consideredElement: (Long, Date) = null
    var consideredElementFriends: List[Long] = List()
    list.foreach { e =>
      if (userFoundCount != 0) {
        if (e._1 != element) {
          if (consideredElementFriends.contains(e._1)) {
            for (a <- 0 until (windowSizeList.size)) {
              if ((e._2.getTime - consideredElement._2.getTime).toDouble / (1000 * 60 * 60 * 24) <= windowSizeList(a))
                temp(a) = temp(a) :+ e
            }
          }
        }
        else if (e._1 == element) {
          userFoundCount += 1
          consideredElement = e
          consideredElementFriends = usersFriends.getOrElse(consideredElement._1, List())
          for (a <- 0 until windowSizeList.size) {
            val tempUsers = temp(a).map(t => t._1).distinct
            if (tempUsers.size > maxIntervalArray(a).size) {
              maxIntervalArray(a) = tempUsers
              temp(a) = List()
            }
            else temp(a) = List()
          }
        }
      }
      else if (e._1 == element && userFoundCount == 0) {
        userFoundCount += 1
        consideredElement = e
        consideredElementFriends = usersFriends.getOrElse(consideredElement._1, List())
      }
    }
    var updateCheck = false
    for (i <- 0 until maxIntervalArray.size) {
      val tempUsers = temp(i).map(t => t._1).distinct
      if (tempUsers.size > maxIntervalArray(i).size) {
        maxIntervalArray(i) = tempUsers
        updateCheck = true
      }
    }
    for (i <- 0 until maxIntervalArray.size) {
      maxIACount(i) = maxIntervalArray(i).size

    }
    return maxIACount // just return count
  }

  def computeProbabilityToBeFollowed(friends: List[(Long, Long)], checkins: List[(Long, Date, Double, Double, String, Long, String)], writePath: String): Unit = {
    var userLocFolMapArray: Map[(Long, Long), Array[Long]] = Map()
    val writerOrder = new PrintWriter(new File(writePath))
    val lines = checkins
    val LocationsGroup = lines.groupBy(t => t._6)
    val totalSize = LocationsGroup.size
    getFriends(friends)
    var count = 0
    var follCount: Array[Long] = new Array[Long](windowSizeList.size)//before friendsSizeList.size
    LocationsGroup.foreach { t =>
      count = count + 1
      println("(count, total) is ::" + count, totalSize)
      val UserGroupPerLoc = t._2.sortBy(tup => tup._2) //
      follCount = new Array[Long](windowSizeList.size)//before friendlistSize
      for (a <- 0 until follCount.size) { // initialize
        follCount(a) = 0
      }
      val uniqueUsersPerLoc = UserGroupPerLoc.map(intup => intup._1).distinct
      if (uniqueUsersPerLoc.size == 1) {
        userLocFolMapArray += ((t._1, uniqueUsersPerLoc(0)) -> follCount)
      } else {
        val UserWithTimePerLoc = UserGroupPerLoc.map(uwt => (uwt._1, uwt._2))
        uniqueUsersPerLoc.foreach { tuple =>
          follCount = getMaxIntervalList(UserWithTimePerLoc, tuple)
          userLocFolMapArray += ((t._1, tuple) -> follCount)
        }
      }
    }
    val LocUserListArray = userLocFolMapArray.toList
    val groupFollowerUserArray = LocUserListArray.groupBy(i =>  i._1._2)
    writerOrder.println("user" + "\t" + "windowSize" + "\t" + "FriendSize" + "\t" + "Probability")
    for (b <- 0 until friendsSizeList.size) {
      val probArray = groupFollowerUserArray.map { p => //p.
        for (a <- 0 until windowSizeList.size) {
          //println("array length::"+p._2.head._2.size)
          val filtLocs = p._2.filter(fin => fin._2(a) >= friendsSizeList(b))
          val probability = filtLocs.size.toDouble /  p._2.size.toDouble
          writerOrder.println(p._1 + "\t" + windowSizeList(a) + "\t" + friendsSizeList(b) + "\t" + probability)
        }
      }
    }
    println("Fip: Finished!!")
    writerOrder.close()

  }

}