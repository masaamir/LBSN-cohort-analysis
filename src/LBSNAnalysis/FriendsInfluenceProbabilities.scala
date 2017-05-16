package LBSNAnalysis

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable.ListBuffer

/**
 * Created by MAamir on 15-03-2016.
 */
/** Find out the probability of users that if he/check-ins at a particular places at least #friendSize
  * number of his/her friends will check-in at that place within time window of #windowSize
  * */
class FriendsInfluenceProbabilities {
  var friendSize = 1
  // to check probability
  var windowSizeList = List(30, 180, 365, 965)
  //days (1,30,180,365)
  var filterSizeList = List(1, 5, 10)
  // friend size (1,5,10,20)
  var usersFriends: Map[(Long), List[(Long)]] = Map()

  def getFriends(FriendsLines: List[(Long, Long)]): Unit = {
    //pathFriends:String,
    /*val lines=scala.io.Source.fromFile(pathFriends).getLines().map(t=> t.split("\t"))
      .map(t=> (t(0).toLong,t(1).toLong)).toList*/

    usersFriends = FriendsLines.groupBy(t => t._1).map { t =>
      var f = ListBuffer[Long]()
      t._2.foreach { ftuple =>
        f += ftuple._2
      }
      //println("user, Friends",t._1,f.toList)
      (t._1, f.toList)
    }
    //friends.foreach(println)
  }

  def stringToDate(dateString: String): Date = {
    val updateDate = dateString.replaceAll("T", "").replaceAll("Z", "")
    val formatter: SimpleDateFormat = new SimpleDateFormat("yyyy-mm-ddHH:mm:ss")
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
    var maxIACount: Array[Long] = new Array(windowSizeList.size)
    maxIntervalArray = maxIntervalArray.map(t => List())
    //val maxInterval:List[(Long,Date)]=List()
    //val maxIntervalUniqueSize=0
    var temp: Array[List[(Long, Date)]] = new Array(windowSizeList.size)
    temp = temp.map(t => List())
    //list.sortBy(t=> t._2)
    //println("list is::"+list)
    // filter visitors starting from the user !!
    //list=list.slice(a+1,UserGroupPerLoc.size).map(chk=> chk._1).distinct
    var userFoundCount = 0 // ignore the users that have checked-in first before the user
    var consideredElement: (Long, Date) = null
    var consideredElementFriends: List[Long] = List()
    list.foreach { e =>
      if (userFoundCount != 0) {
        if (e._1 != element) {
          //var a=0
          //println("value a, total ::"+a,windowSizeList.size-1)
          //println("userCount, element")
          if (consideredElementFriends.contains(e._1)) {

            for (a <- 0 until (windowSizeList.size)) {
              //println("temp::"+temp(a))
              if ((e._2.getTime - consideredElement._2.getTime).toDouble / (1000 * 60 * 60 * 24) <= windowSizeList(a))
                temp(a) = temp(a) :+ e
              //println("userCount, element, Array position, Array"+userFoundCount,e._1,a, temp(a))
              //println(temp(a))
            }
          }
          //temp = temp :+ e
        }
        else if (e._1 == element) {
          userFoundCount += 1
          consideredElement = e
          consideredElementFriends = usersFriends.getOrElse(consideredElement._1, List())
          //returnMaxArray(temp,maxIntervalArray)
          //println("max,temp size is::"+maxInterval,temp)
          //var tempSize=temp.map(t=> t._1).distinct.size
          //temp=temp.reverse
          //println("max,temp size is::"+maxInterval,temp)
          //println("element ",e)
          //println("previous size::"+temp)
          /*temp.foreach{t=>
          println("test",e._2.getTime,t._2.getTime,(e._2.getTime - t._2.getTime),windowSize)
        }*/
          //val tempSize:Array[Int]=new Array(windowSizeList.size)
          for (a <- 0 until windowSizeList.size) {

            //println("before filter::"+temp(a))

            //temp(a) = temp(a).filter { t => (e._2.getTime - t._2.getTime).toDouble / (1000 * 60 * 60 * 24) <= windowSizeList(a)}
            //println("after filter, window::"+temp(a),windowSizeList(a))

            /*println("previous list, element and window "+temp(a),e,windowSizeList(a))
          temp(a).foreach{t=>
            println("element, arrElement, difference"+e._2.getTime,t._2.getTime,((e._2.getTime - t._2.getTime)))
          }*/
            //temp(a) = temp(a).filter { t => (e._2.getTime - t._2.getTime) <= windowSizeList(a)}
            //println("after filter list "+temp(a))
            //tempSize(a)=temp(a).map(t=> t._1).distinct.size
            val tempUsers = temp(a).map(t => t._1).distinct
            //val tempUsers=temp(a).map(t=> t._1).distinct.intersect(usersFriends.getOrElse(e._1,List()))

            if (tempUsers.size > maxIntervalArray(a).size) {
              maxIntervalArray(a) = tempUsers
              //maxIntervalUniqueSize=tempSize(a)
              temp(a) = List()
            }
            else temp(a) = List()
          }
          //temp=temp.filter{t=> (e._2.getTime - t._2.getTime).toDouble/(1000*60*60*24) <= windowSize }

          //println("after size::"+temp)
          //println("filtered temp "+temp+" where element is::"+e)
          //val tempSize=temp.map(t=> t._1).distinct.size

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

    //if(updateCheck) { // if at least one of index item is more update
    /*for (i <- 0 until maxIntervalArray.size){

      val tempUsers = temp(i).map(t => t._1).distinct
      maxIntervalArray(i)=tempUsers
    }*/
    //}
    //println("final max size::"+maxInterval)
    //maxIntervalArray.foreach( println)
    for (i <- 0 until maxIntervalArray.size) {
      maxIACount(i) = maxIntervalArray(i).size

    }
    return maxIACount // just return count
    //return maxIntervalArray
  }


  //def formatDataSet(readPath:String,writePath:String,friendsPath:String): Unit ={
  def formatDataSet(friendsPath: String, checkinFilePath: String, writePath: String, //minUserCk= minimum check-ins of users
                    minUserCk: Long, maxUserCk: Long, minUserLocs: Long, minLocCk: Long, maxLocCk: Long, minLocVisitors: Long): Unit = {
    val fd = new FilterData()
    val filterData = fd.filterLBSNData(friendsPath, checkinFilePath,
      minUserCk, maxUserCk, minUserLocs, minLocCk, maxLocCk, minLocVisitors)

    //var userLocFolMap:Map[(Long,Long),List[Long]]=Map()
    var userLocFolMapArray: Map[(Long, Long), Array[Long]] = Map()
    //val userLocFollowers=List[(Long,Long,Array[Long])]()
    val writerOrder = new PrintWriter(new File(writePath))
    //val lines=scala.io.Source.fromFile(readPath).getLines().map(t=> t.split("\t"))
    //.map(t=> (t(0).toLong,stringToDate(t(1)),t(2).toDouble,t(3).toDouble,t(4),t(5).toLong,t(1))).toList.distinct//.toArray
    //user, time(date), latitude, longitude, Loc(string),loc(long),time(string)
    val lines = filterData._2
    val allowedUsers = filterData._2.groupBy(t => t._1).map(t => (t._1, t._2.map(it => it._6)))
      .filter(t => t._2.size > 30 && t._2.distinct.size > 20).map(t => t._1).toList.distinct


    val LocationsGroup = lines.groupBy(t => t._6)
    val totalSize = LocationsGroup.size
    getFriends(filterData._1)
    var count = 0
    //var foll: Array[List[Long]] = new Array[List[Long]](filterSizeList.size)
    var follCount: Array[Long] = new Array[Long](filterSizeList.size)
    LocationsGroup.foreach { t =>
      count = count + 1
      println("(count, total) is ::" + count, totalSize)
      val UserGroupPerLoc = t._2.sortBy(tup => tup._2) //.filter(t=> allowedUsers.contains(t._1))


      //println("-----------------------------------------")
      /*UserGroupPerLoc.foreach{checkin=>
        //writerOrder.println(checkin._1+"\t"+checkin._7+"\t"+checkin._3+"\t"+checkin._4+"\t"+checkin._5+"\t"+checkin._6)
        //writerOrder.
          println(checkin._1+"\t"+checkin._7+"\t"+checkin._3+"\t"+checkin._4+"\t"+checkin._5+"\t"+checkin._6)
      }*/
      //var a = 0
      follCount = new Array[Long](filterSizeList.size)
      for (a <- 0 until follCount.size) {
        follCount(a) = 0
      }
      val uniqueUsersPerLoc = UserGroupPerLoc.map(intup => intup._1).distinct
      if (uniqueUsersPerLoc.size == 1) {

        userLocFolMapArray += ((t._1, uniqueUsersPerLoc(0)) -> follCount)

      } else {

        val UserWithTimePerLoc = UserGroupPerLoc.map(uwt => (uwt._1, uwt._2))
        uniqueUsersPerLoc.foreach { tuple =>
          //val userC = tuple
          //val userCFriends: Set[Long] = usersFriends.getOrElse(userC, List()).toSet
          //println("userFriends::"+userCFriends.size)
          //println("sent user,List::" + userC, UserWithTimePerLoc)
          // send one by one window

          follCount = getMaxIntervalList(UserWithTimePerLoc, tuple)
          //println("foll::" + foll(0).size)
          if (follCount(0) > 1) {
            //println("check prob of users ::"+userC,foll.toList)
          }
          //var follFriends:Array
          // now foll = followerfriends //not required!! because already done in getMaxIntervalList


          /*
          for (a <- 0 until windowSizeList.size) {
            //println("before::"+foll(a))
            //println("users, friends are ::"+userC,userCFriends)
            foll(a) = foll(a).toSet.intersect(userCFriends).toList
            //println("after::"+foll(a))
          }*/
          userLocFolMapArray += ((t._1, tuple) -> follCount)

          //to continue here

        }
      }
      /*
            for(a<- 0 to UserGroupPerLoc.size-1){
              val u=UserGroupPerLoc(a)._1
              var followers:List[Long]=UserGroupPerLoc.slice(a+1,UserGroupPerLoc.size).map(chk=> chk._1).distinct
              followers = followers.filter(f=> f!=u)
              val friend:Set[Long]=friends(u).toSet
              val friendsFollowers:Set[Long]=followers.toSet.intersect(friend)
              userLocFolMap += ((t._1,u)->friendsFollowers.toList)
              /*if(followers.size>0){
                println("*********************************")
                println(t._1,u,followers)
              }*/

            }*/
      //writerOrder.println(t._2.foreach())
      //val users=UserGroupPerLoc.map(t=> (t._1)).distinct
      //println(t._2)
    }
    //userLocFolMap.foreach(t=> println(t))
    //old--val LocUserList=userLocFolMap.toList
    val LocUserListArray = userLocFolMapArray.toList
    /*LocUserListArray.foreach{ll=>
      println(ll._1,ll._2.toList)
    }*/

    //old--val groupFollowerUser=LocUserList.groupBy(t=> t._1._2)
    val groupFollowerUserArray = LocUserListArray.groupBy(i => i._1._2)
    //val avergUser=groupFollowerUser.map(t=> ())

    /*
    groupFollowerUserArray.foreach{pt=>
      println("u,(u,l),"+pt._1,pt._2.foreach(t=> print(t._1,t._2.toList)))
    }*/
    /*
    for (b <- 0 until filterSizeList.size) {

      for (a <- 0 until windowSizeList.size) {
        val filtLocs = locVisitors.filter(t => t._2(0).size > filterSizeList(b))
        val probab = filtLocs.size.toDouble / userLocations.distinct.size.toDouble
        println("probability of user " + u + " for window size " + windowSizeList(a) +
          " with filter size::" + filterSizeList(b) + " is::" + probab)

      }
    }*/
    writerOrder.println("user" + "\t" + "windowSize" + "\t" + "FriendSize" + "\t" + "Probability")
    for (b <- 0 until filterSizeList.size - 1) {
      val probArry = groupFollowerUserArray.map { p =>
        //println("p is ::"+p)
        for (a <- 0 until windowSizeList.size - 1) {
          //println("before"+p._2.foreach(tt=> println(tt._1,tt._2.toList)))
          val filtLocs = p._2.filter(fin => fin._2(a) >= filterSizeList(b))
          //println("after"+filtLocs.foreach(tt=> println(tt._1,tt._2.toList)))
          val probability = filtLocs.size.toDouble / p._2.size.toDouble
          //println("Probability of user ::" + p._1 + " for windowSize::" + windowSizeList(a) +
          //" with friendSize::" + filterSizeList(b) + " is " + probability)
          writerOrder.println(p._1 + "\t" + windowSizeList(a) + "\t" + filterSizeList(b) + "\t" + probability)
        }
      }
    }
    println("Fip: Finished!!")
    /*

        val prob=groupFollowerUser.map{t=>
          var sum=0
          t._2.foreach{inside=>
            if(inside._2.size>=friendSize){// to check probability that a user will have followers greater than 1
              sum  += 1
            }
            //sum+=inside._2.size // average number of followers
          }
          //println("sum is:: "+sum+" and size is:: "+t._2.size)
          val avg:Double=sum.toDouble/t._2.size.toDouble
          println("average is ::"+avg)
          writerOrder.println(t._1+"\t"+avg)
          (t._1,avg)

        }*/
    //prob.foreach(t=> println(t))
    /*
        //userLocFolMap.gro
        val overallProbability=prob.toList.filter(t=> t._2>0)
        println("probability that a user will be followed by at least "+friendSize+" friends is::"+overallProbability.size.toDouble/prob.size.toDouble)
    */
    writerOrder.close()

  }

}
