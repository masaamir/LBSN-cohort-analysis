package FriendsBehaviorAnalysis

import java.io.{File, PrintWriter}
import java.util.Date

/**
 * Created by MXXX on 5/2/XXX.
 */
class correlationFinder {
  def computeCorrelationFriends(friends: List[(Long, Long)], checkins: List[(Long, Date, Double, Double, String, Long, String)]): Unit ={
    val visitorsLocations=checkins.groupBy(t=> t._1).map(t=> (t._1,t._2.map(it=> it._6)))
    var firstUser:Long=0L
    var secondUser:Long=0L
    val friendsLines=friends
      .map{t=>
      firstUser=t._1.toLong
      secondUser=t._2.toLong
      if(firstUser < secondUser) (firstUser,secondUser)
      else (secondUser,firstUser)
    }.toList.distinct

    var corr:Double=0.0
    var firstVisits:List[Long]=List()
    var secondVisits:List[Long]=List()
    val friendsCorr=friendsLines.map{t=>
      firstVisits=visitorsLocations.getOrElse(t._1,List())
      secondVisits=visitorsLocations.getOrElse(t._2,List())
      corr=firstVisits.toSet.intersect(secondVisits.toSet).size.toDouble/(firstVisits.size + secondVisits.size).toDouble
      (t._1,t._2,corr)
    }
    friendsCorr.sortBy(t=> -t._3).take(10).foreach(t=> println(t))
  }
  def findCorrelationFNF(friends: List[(Long, Long)], checkins: List[(Long, Date, Double, Double, String, Long, String)], writeFile:String): Unit ={
    var usersFriends:Map[(Long),List[(Long)]]=Map()
    var allowedUsers:List[Long]=List()
    val writerOrder=new PrintWriter(new File(writeFile))
    writerOrder.println("user1,user2,friends,correlation")
    usersFriends=friends.groupBy(t=> t._1).map(t=> (t._1,t._2.map(it=> it._2).distinct))
    val usersFriendsMap:Map[Long,Map[Long,Int]]=usersFriends.map(t=> (t._1,t._2.map(it=>(it,1)).toMap)).toMap
    val visitorsLocations=checkins.groupBy(t=> t._1).map(t=> (t._1,t._2.map(it=> it._6)))
    allowedUsers=checkins.map(t=> t._1).distinct
    val totalSize=allowedUsers.size
    allowedUsers=allowedUsers.sortBy(t=> t)
    var corr:Double=0.0
    var firstVisits:List[Long]=List()
    var secondVisits:List[Long]=List()
    for(i<-0 until allowedUsers.size){
      println("count,total is "+i,totalSize)
      firstVisits=visitorsLocations.getOrElse(allowedUsers(i),List())
      for(j<-i+1 until allowedUsers.size){
        secondVisits=visitorsLocations.getOrElse(allowedUsers(j),List())
        var bool=false
        val containedUserFriends:Map[Long,Int]=usersFriendsMap
          .getOrElse(allowedUsers(i),Map[Long,Int]())//.ge
        val inList=containedUserFriends.getOrElse(allowedUsers(j),null)
        //if(usersFriends.getOrElse(allowedUsers(i),List()).contains(allowedUsers(j))){
        if(inList!=null){
          bool=true
        }else{
          bool=false
        }
        corr=firstVisits.intersect(secondVisits).size.toDouble/(firstVisits.size + secondVisits.size).toDouble
        writerOrder.println(allowedUsers(i)+","+allowedUsers(j)+","+bool+","+corr)
      }
    }
    writerOrder.close()
  }


}