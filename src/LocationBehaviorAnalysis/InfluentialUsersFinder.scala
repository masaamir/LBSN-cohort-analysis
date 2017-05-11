package TaskRunner

import java.util.Date

import scala.collection.mutable.{HashSet, ListBuffer}
import FormatData.fileReaderLBSN



/**
  * Created by maaamir on 1/2/17.
  */
class InfluentialUsersFinder {

  var userCheckinMap:Map[Long,List[(Long, Date, Double, Double, String, Long)]]=Map()
  var userActivitiesCount:Map[Long,Long]=Map()
  var pairActivityCount:Map[(Long,Long),Long]=Map()
  var parents:Set[Long]=Set()
  var followersMap:Map[(Long,Long),Long]=Map()
  var followersMapWithTime:Map[(Long,Long,Long,Long),Long]=Map()// (source, destination, location, source Act time),dst Act Time
  var tauFollowersMap:Map[(Long,Long),(Long,Long,Double)]=Map()
  var creditPairAction:Map[(Long,Long,Long),Double]=Map() // action, source, destination
  var srcEdges:Map[Long,List[(Long,Double)]]=Map()


  /**
    * */
  def sortFriendsPair(first:Long,second:Long): (Long,Long) ={
    if(first<second) {
      return (first,second)
    }  else {
      return (second,first)
    }
  }

  /** Map contains the total activities/visits performed by a visitor
    * */
  def incrementUserActs(user:Long): Unit ={
    var activityCount:Long=0
    if(userActivitiesCount.contains(user)){
      activityCount=userActivitiesCount.getOrElse(user,0)
      userActivitiesCount += (user-> (activityCount+1))
    }else{
      userActivitiesCount += (user -> 1L)
    }
  }

  /**Map contains a pair(src, dst) ant the number of activities of src that are followed by the dest node.
    * */
  def incrementFollowerCount(srcUser:Long,dstUser:Long): Unit ={
    if(followersMap.contains((srcUser,dstUser))){
      val followedCount:Long=followersMap.getOrElse((srcUser,dstUser),0)
      followersMap += ((srcUser,dstUser)-> (followedCount + 1L))
    }else{
      followersMap += ((srcUser,dstUser)-> 1L) //v._1,u._1
    }
  }

  /**Tau contains an average time difference that dst take to follow src's activities between a pair(src,dst)
    * */
  def updateTau(src:(Long,Date,Double,Double,String,Long),dst:(Long,Date,Double,Double,String,Long)): Unit ={
    if(tauFollowersMap.contains((src._1,dst._1))){
      val countNSumNMean=tauFollowersMap.getOrElse((src._1,dst._1),null)
      val newCount=countNSumNMean._1 + 1
      val sumDiff=countNSumNMean._2+ (dst._2.getTime - src._2.getTime )
      tauFollowersMap += ((src._1,dst._1)->(newCount,sumDiff,sumDiff.toDouble/newCount.toDouble))
    }else{
      tauFollowersMap += ((src._1,dst._1)->(1,(dst._2.getTime -src._2.getTime ),(dst._2.getTime -src._2.getTime ).toDouble))
    }
  }
  /** count of number of activities of a pair
    * */
  def incrementPairActCount(srcUser:Long,dstUser:Long): Unit ={
    if(pairActivityCount.contains(sortFriendsPair(srcUser,dstUser))){
      val pairActCount:Long=pairActivityCount.getOrElse(sortFriendsPair(srcUser,dstUser),0)
      pairActivityCount += (sortFriendsPair(srcUser,dstUser)->(pairActCount +1 ))
    }else{
      pairActivityCount += (sortFriendsPair(srcUser,dstUser)->1)
    }
  }

  def findPotentialBV(bv:HashSet[Long],minInf:Double)
  : Set[(Long)] = {

    val influencedUsers: ListBuffer[(Long, Double)] = new ListBuffer[(Long, Double)]()
    bv.foreach { t =>
      if (srcEdges.contains(t)) {
        influencedUsers ++= srcEdges.getOrElse(t, List())
      }
    }
    //println(" total size of influenced users are ::" + influencedUsers.size)
    //println("adding values for each users")
    val potentialBV=influencedUsers.groupBy(_._1).mapValues(_.map(_._2).sum)
      .filter(_._2 >=minInf).toList
    //potentialBV.foreach(println)
    return potentialBV.map(t=> t._1).toSet // return only list of users but not (user, influencedScore)
  }

  /**main function - Algorithm 1 : Paper: "learning influence probabilities in Social Networks"
    * */
  /*
    def learningPhase1Old(friendsMap:Map[(Long,Long),Int],inCheckins:List[(Long, Date, Double, Double, String, Long)], timeWindow:Long): Unit ={
      var currentTable:ListBuffer[(Long, Date, Double, Double, String, Long)]=new ListBuffer[(Long, Date, Double, Double, String, Long)]()
      val trainingSet=inCheckins.groupBy(t=> t._6).map(t=> (t._1,t._2.sortBy(it=> it._2))) //Actions sorted by time
      var followerTimeDiff:Long=0
      val timeThreshold:Long=timeWindow* 1000 *60 *60 // convert it into milliseconds
      trainingSet.foreach{ at=> //for each action
        currentTable= new ListBuffer[(Long, Date, Double, Double, String, Long)]()
        at._2.foreach{ u=> // for each tuple
          incrementUserActs(u._1) //increment A_u (line 4)
            parents=new ListBuffer[Long]()
            currentTable.foreach{v=>
              if(u._1!= v._1 && friendsMap.contains(sortFriendsPair(u._1,v._1))){
                followerTimeDiff= u._2.getTime - v._2.getTime
                if(followerTimeDiff > 0 && followerTimeDiff <= timeThreshold){
                  incrementFollowerCount(v._1,u._1) // increment A_v2u (line 8)
                  updateTau(v,u) //update T_v,u (line 9)
                  parents += v._1 //insert v in parents
                }
                incrementPairActCount(v._1,u._1)//increment Av&u
              }
            }
            // parents
            parents.foreach{p=>
              creditPairAction += ((u._6,p,u._1)-> (1.toDouble/parents.size.toDouble))//update credit v,u doubt on it divide by the parents
            }
            currentTable += u
        }
      }
      var creditPairScore:Double=0.0
      var totalActsSrc:Long=0
      val creditAllActions=creditPairAction.groupBy(t=> (t._1._2,t._1._3)).toList
        .map{t=>
          creditPairScore=t._2.toList.map(it=> it._2).sum
          if(userCheckinMap.contains(t._1._1))
          totalActsSrc=userCheckinMap.getOrElse(t._1._1,List()).size
          (t._1,creditPairScore/totalActsSrc.toDouble)
        }
      srcEdges=creditAllActions.groupBy(t=> t._1._1)
        .map(t=> (t._1,(t._2.map(it=> (it._1._2,it._2))))) // populate map with the value of influenced vertices for each user
    }
  */
  def learningPhase1(friendsMap:Map[(Long,Long),Int],inCheckins:List[(Long, Date, Double, Double, String, Long)], timeWindow:Long): Unit ={
    var currentTable:Set[(Long, Date, Double, Double, String, Long)]=Set() // changed to set modified
    val trainingSet=inCheckins.groupBy(t=> t._6).map(t=> (t._1,t._2.sortBy(it=> it._2))) //Actions sorted by time
    var followerTimeDiff:Long=0
    var considerTuple=true
    val timeThreshold:Long=timeWindow* 1000 *60 *60// convert it into milliseconds
    trainingSet.foreach{ at=> //for each action
      followersMapWithTime=Map()
      currentTable= Set()
      at._2.foreach{ u=> // for each tuple
        incrementUserActs(u._1) //increment A_u (line 4)
        parents=Set()
        currentTable.foreach{v=>
          if(u._1!= v._1 && friendsMap.contains(sortFriendsPair(u._1,v._1))){
            considerTuple=true
            if(! followersMapWithTime.contains(v._1,u._1,v._6,v._2.getTime)) { // if not exist

              followerTimeDiff = u._2.getTime - v._2.getTime
              if (followerTimeDiff > 0 && followerTimeDiff <= timeThreshold) {
                followersMapWithTime += ((v._1,u._1,v._6,v._2.getTime)-> u._2.getTime)
                incrementFollowerCount(v._1, u._1) // increment A_v2u (line 8)
                updateTau(v, u) //update T_v,u (line 9)
                parents += v._1 //insert v in parents // should be "Set" despite of ListBuffer /--new keep only distinct parents and avoid repeated entries of same node
              }
              incrementPairActCount(v._1, u._1) //increment Av&u
            }
          }
        }
        // parents
        parents.foreach{p=>
          creditPairAction += ((u._6,p,u._1)-> (1.toDouble/parents.size.toDouble))//update credit v,u doubt on it divide by the parents
        }
        currentTable += u
      }
    }
    var creditPairScore:Double=0.0
    var totalActsSrc:Long=0
    val creditAllActions=creditPairAction.groupBy(t=> (t._1._2,t._1._3)).toList
      .map{t=>
        creditPairScore=t._2.toList.map(it=> it._2).sum
        if(userCheckinMap.contains(t._1._1))
          totalActsSrc=userCheckinMap.getOrElse(t._1._1,List()).size
        (t._1,creditPairScore/totalActsSrc.toDouble)
      }
    srcEdges=creditAllActions.groupBy(t=> t._1._1)
      .map(t=> (t._1,(t._2.map(it=> (it._1._2,it._2))))) // populate map with the value of influenced vertices for each user
    /*
    /**testing*/
    val bv:List[Long]=List(1919,14116,1871)
    val potBV=findPotentialBV(srcEdges,bv,0.01)

    //srcEdges.toList.sortBy(t=> -t._2.size).take(10).foreach(t=> println(t))

    // for a user
    /*
    val creditScoreUser=creditAllActions.groupBy(t=> t._1._1).toList
      .map(t=> (t._1,t._2.map(it=> it._2).sum))

    println("top scorers are ::")
    creditScoreUser.sortBy(t=> -t._2).take(10).foreach(println)*/
    */
  }

  def runner(fileFriends:String,fileCheckin:String, timeWindow:Long): Unit ={
    val fr=new fileReaderLBSN
    val friendsMap=fr.readFriendsFile(fileFriends)
      .map(t=> if(t._1< t._2) t else (t._2,t._1)).distinct // remove duplicate edges in friendship graph
      .map(t=> ((t._1,t._2),1)).toMap //check if friends
    val checkins=fr.readCheckinFileNew(fileCheckin)
    userCheckinMap=checkins.groupBy(t=> t._1) // group activities for each user
    learningPhase1(friendsMap,checkins,timeWindow)
  }
}
