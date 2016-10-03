package FriendsBehaviorAnalysis

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import LBSNAnalysis.FilterData

import scala.collection.mutable.ListBuffer
import scala.reflect.io
import scala.util.control.Breaks

/**
 * Created by MAamir on 4/26/2016.
 */
/** Find the probability of a user that he/check-ins at a location if at least "filterSize" number of his/her
  * friends have checked-in at the location before.
  * We find probability of each user
  * */
class ProbabilityToFollowFriendsFinder {
  var windowSizeList = List(1,7,30,180, 365)// in days
  var friendSizeList = List(1, 2, 3,4) // friends
  // friend size//1, 3, 5
  var Users: List[Long] = null
  var Location: List[Long] = null
  var LocationCheckins: Map[(Long), List[(Long, Date)]] = Map()
  var usersFriends: Map[(Long), List[(Long)]] = Map()
  var usersVisitedLocations: Map[(Long), List[Long]] = Map()
  var friendsSizeListVisitCount: Array[Long] = new Array[Long](3)

  def getFriends(friendsLines: List[(Long, Long)]): Unit = {
    usersFriends = friendsLines.groupBy(t => t._1).map { t =>
      var f = ListBuffer[Long]()
      t._2.foreach { ftuple =>
        f += ftuple._2
      }
      (t._1, f.toList)
    }
  }

   def getMaxIntervalList(list: List[(Long, Date)], element: Long): Array[Long] = {
    friendsSizeListVisitCount = friendsSizeListVisitCount.map(t => 0L)
    //val totalIntervalSize:Array[Boolean]=new Array[Boolean](3)
    var maxIntervalArray: Array[List[Long]] = new Array(windowSizeList.size)
    val maxIACount: Array[Long] = new Array(windowSizeList.size)
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
          //increase the fix number of friends visited for probability
          if (tempUsers.size > 0) {
            if (tempUsers.size > maxIntervalArray(a).size) {
              maxIntervalArray(a) = tempUsers
              temp(a) = List()
            }
            else temp(a) = List()
          }
        }
      }
    }
    for (i <- 0 until maxIntervalArray.size) {
      maxIACount(i) = maxIntervalArray(i).size
    }
    return maxIACount
  }

  def getCountFriendsVisitStatus(list: List[(Long, Date)], user: Long): Array[Array[Boolean]] = {
    var filteredList: List[(Long, Date)] = List[(Long, Date)]()
    var mdArray: Array[Array[Boolean]] = Array.ofDim[Boolean](windowSizeList.size, friendSizeList.size)
    if (list.size > 0) {
      filteredList = list.sortBy(t => t._2)
      val q: scala.collection.mutable.Queue[(Long, Date)] = scala.collection.mutable.Queue[(Long, Date)]()
      mdArray = mdArray.map(t => t.map(it => false)) //initialize multi-dimensional array
      for (i <- 0 until windowSizeList.size) {
        val loop = new Breaks
        loop.breakable {
          for (j <- 0 until friendSizeList.size) {
            var loopcheck = true
            filteredList.foreach { t =>
              if (loopcheck) {
                if (t._1 == user) {
                  q.clear()
                }
                else {
                  var qUnique = q.map(t => t._1).distinct
                  if (qUnique.size < friendSizeList(j)) {
                    // can b greater than friendSizeList(j) due to repetition
                    q += t
                  }
                  qUnique = q.map(t => t._1).distinct
                  while (qUnique.size == friendSizeList(j) && loopcheck == true) {
                    val temp = q.map(t => t._1).distinct
                    val tempList = q.toList
                    val head = tempList.head
                    val tail = tempList.last
                    if ((head._2.getTime - tail._2.getTime).toDouble / (1000 * 60 * 60 * 24) <= windowSizeList(i)) {
                      mdArray(i)(j) = true
                      loopcheck = false
                      q.clear()
                    } else {
                      q.dequeue()
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    return mdArray

  }

  def computeProbabilityToFollowFriends(friends: List[(Long, Long)], checkins: List[(Long, Date, Double, Double, String, Long, String)], writePathFolder: String): Unit = {
    val writerOrder = new PrintWriter(new File(writePathFolder + ".txt"))
    Users = checkins.map(t => t._1).toList.distinct //initialize users with the filtered visitors
    getFriends(friends) // initialize the friends of each users, global variable
    usersVisitedLocations = checkins.groupBy(t => t._1).map { t =>
      (t._1, t._2.map(l => l._6).distinct) // get all the visited locations for each user - UN-ORDERED !!
    }
    /** get visitors of each location ordered by their check-in time
     there can be duplicate users in the list because user may check-in twice on same location
      */
    LocationCheckins = checkins.groupBy(t => t._6).map { l =>
      (l._1, l._2.sortBy(u => u._2).map(inUser => (inUser._1, inUser._2))) //(l._1,orderedUsers)
    }
    var count = 0
    val totalCount = Users.size
    /** Main Computation */
    var userLocations: List[Long] = List()
    var locVisitors: List[(Long, Array[Long], Array[Array[Boolean]])] = List()
    var visitors: List[(Long, Date)] = List()
    var previousVisitorsCount: Array[Long] = new Array(windowSizeList.size)
    var mdArr: Array[Array[Boolean]] = Array.ofDim[Boolean](windowSizeList.size, friendSizeList.size)
    writerOrder.println("user" + "\t" + "windowSize" + "\t" + "FriendSize" + "\t" + "Probability")
    Users.foreach { u =>
      count = count + 1
      println("count,totalCount::" + count, totalCount)
      userLocations = usersVisitedLocations.getOrElse(u, List()).distinct //get all distinct locations of the user
      locVisitors = userLocations.map { t =>
        visitors = LocationCheckins.getOrElse(t, List()) //get all the visitors of the locations that is visited by this user
        previousVisitorsCount = new Array(windowSizeList.size)
        for (i <- 0 until windowSizeList.size) {
          previousVisitorsCount(i) = 0
        }
        val uFriend = usersFriends.getOrElse(u, List())
        //visitors = visitors.filter(t => uFriend.contains(t._1) || t._1 == u)//friends
        //visitors = visitors.filter(t => !uFriend.contains( t._1) || t._1 == u)//non friends
        val friendVisitorsSize = visitors.map(t => t._1).distinct.filter(t => t != u).size // only visitors it should be friend visitors to optimize
        if (visitors.size > 0 && friendVisitorsSize > 0) {
          //get max sequence i.e., where user has followed maximum number of locations that are visited by users/friends
          previousVisitorsCount = getMaxIntervalList(visitors, u) // should be max not min interval because condition is of at least friends
          //*println("returned::"+previousVisitorsCount.mkString(","))
          mdArr = getCountFriendsVisitStatus(visitors, u) //get probability array for visits of friends, i.e., how many of these locations are visited by the threshold friends
          //this information is used as denominator to compute probability
        }
        (t, previousVisitorsCount, mdArr)
      }
      var resultantArray: Array[Array[Double]] = Array.ofDim[Double](friendSizeList.size, windowSizeList.size)
      for (b <- 0 until friendSizeList.size) {
        for (a <- 0 until windowSizeList.size) {
          val filtLocs = locVisitors.filter(t => t._2(a) >= friendSizeList(b))
          val denom = locVisitors.filter(t => t._3(a)(b) == true)
          if (filtLocs.size > denom.size) {
            println("Error !!")
          }
          val probability = filtLocs.size.toDouble / denom.size.toDouble //userLocations.distinct.size.toDouble
          resultantArray(b)(a) = probability
        }
      }
      // filter only those probabilities which are number and not Infinity or Not a Number (NAN)
      resultantArray = resultantArray.filter { t =>
        t.forall(it => it >= 0)
      }
      if (resultantArray.size == friendSizeList.size) {
        // keep the result of only those user that have probabilities for all friends
        /** Print Writers: Separate pw for each window,friend pair */
        for (outer <- 0 until resultantArray.size) {
          for (inner <- 0 until resultantArray(outer).size) {
            //for separate file for each friend, window pair
            //pwMda(outer)(inner).println(u + "\t" + windowSizeList(inner) + "\t" + friendSizeList(outer) + "\t" + resultantArray(outer)(inner))
            writerOrder.println(u + "\t" + windowSizeList(inner) + "\t" + friendSizeList(outer) + "\t" + resultantArray(outer)(inner))
          }
        }
      }
    }
    //closePrintWriters(friendSizeList.size,windowSizeList.size,pwMda)
    println("pff Finished !!")
    writerOrder.close()
  }

  def closePrintWriters(outerLoopSize: Int, innerLoopSize: Int, pwMda: Array[Array[PrintWriter]]): Unit = {
    for (outer <- 0 until outerLoopSize) {
      for (inner <- 0 until innerLoopSize) {
        pwMda(outer)(inner).close()
      }
    }

  }

  def createPrintWriterArray(outerLoopSize: Int, innerLoopSize: Int, filePath: String): Array[Array[PrintWriter]] = {
    val pwMda = Array.ofDim[PrintWriter](outerLoopSize, innerLoopSize)
    for (outer <- 0 until outerLoopSize) {
      for (inner <- 0 until innerLoopSize) {
        val file = new File(filePath + "\\" + friendSizeList(outer) + "_" + windowSizeList(inner) + ".txt")
        file.getParentFile.mkdirs()
        pwMda(outer)(inner) = new PrintWriter(file)
      }
    }
    return pwMda

  }
}
