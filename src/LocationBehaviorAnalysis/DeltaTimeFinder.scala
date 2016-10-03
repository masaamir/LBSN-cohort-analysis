package LocationBehaviorAnalysis

import java.io.{File, PrintWriter}
import java.util.Date

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.reflect.io

/**
 * Created by MAamir on 5/2/2016.
 */
class DeltaTimeFinder {
  def findDistance(lat1:Double,lon1:Double,lat2:Double,lon2:Double): Double ={
    val earthRadius = 6371000; //meters
    val dLat = Math.toRadians(lat2-lat1);
    val dLng = Math.toRadians(lon2-lon1)
    val a = Math.sin(dLat/2) * Math.sin(dLat/2) +
      Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
        Math.sin(dLng/2) * Math.sin(dLng/2)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a))
    val dist =  (earthRadius * c)

    return dist
    //return math.sqrt(math.pow(lat1-lat2,2)+ math.pow(lon1-lon2,2) )
  }
  def computeDeltaTnDeltaDWrong(checkins: List[(Long, Date, Double, Double, String, Long, String)], fileWrite:String): Unit ={
    val writer=new PrintWriter(new File(fileWrite))
    //val locations=scala.io.Source.fromFile(path).getLines().map(t=> t.split("\t"))
    //.map(t=> (t(5).toLong,t(2).toDouble,t(3).toDouble)).toList.distinct
    var lines=checkins
      .map(t=> (t._1,t._2,t._6,t._3,t._4)).toList.distinct.toArray // user, time, loc id, lat,lon
    lines=lines.sortBy(t=> t._2)
    //lines.take(10).foreach(println)

    writer.println("user,t1,loc1,lat1,lon1,t2,loc2,lat2,lon2,deltaT,deltaD")
    for(a <- 1 until lines.length-1){
      //println(lines(a))
      val first=lines(a)
      val second=lines(a+1)

      if(second._1==first._1 && second._3!=first._3) {// same users and different locations
        val deltaT = second._2.getTime - first._2.getTime

        val deltaHours=(deltaT/(1000*60*60))
        if(deltaT>0) {
          val deltaD = findDistance(first._4, first._5, second._4, second._5) / 1000 //in kilometers
          //println(first._1, deltaT,deltaD)
          writer.println(first._1 + "," + second._1 + "," + first._2 + "," + first._3 + "," + first._4 + "," + first._5 + "," +
            second._2 + "," + second._3 + "," + second._4 + "," + second._5 + "," + deltaHours + "," + deltaD)
          //writer.println()
          //user,t1,loc1,lat1,lon1,t2,loc2,lat2,lon2,deltaT,deltaD

        }
        else if(deltaT<0){
          println("error !!!")
          println("first is ::"+first)
          println("second is ::"+second)
        }
      }
    }
    writer.close()
  }

  def computeDeltaTRaw(checkins: List[(Long, Date, Double, Double, String, Long, String)],timeT:Double, fileWrite:String): Unit ={
    val timeThreshold:Double=timeT// in hours
    val timeConversion=1000*60*60 // in hours mSec*60(min)*60(hrs)*24(days)
    val fileWriter=new PrintWriter(new File(fileWrite+"_TimeWindow_"+timeThreshold+".txt"))
    //val l2LMap: scala.collection.mutable.Map[(Long, Long), (Double,Double,Long)] = scala.collection.mutable.Map()
    val l2lArray:ArrayBuffer[(Long,Long,Long,Double)]=new ArrayBuffer[(Long, Long, Long, Double)]()//L1,L2,U,time
    val locsByUsers=checkins.groupBy(t=> t._1).map(t=> (t._1,t._2.sortBy(it=> it._2))).toArray
    fileWriter.println("Location1"+"\t"+"Location2"+"\t"+"User"+"\t"+"Delta Time")
    val totalSize=locsByUsers.size
    for(i<-0 until locsByUsers.size){
      println("current,total::"+i,totalSize)
      //println("for user ::"+locsByUsers(i)._1)
      val usersCheckin=locsByUsers(i)._2
      for(j<-0 until usersCheckin.size-1){ //for check-ins of each users
      val first=usersCheckin(j)
        val second=usersCheckin(j+1)
        val timeDiff=(second._2.getTime-first._2.getTime).toDouble/timeConversion.toDouble //mSec. to given
        if(timeDiff<= timeThreshold){
          if(second._6!=first._6){ // if locations are not equal
            l2lArray  += ((first._6,second._6,first._1,timeDiff))
            /*
          val temp=l2LMap.getOrElse((second._6,first._6),null)
            val distance = findDistance(second._3, second._4, first._3, first._4)
            if(temp!=null) { //if entry already exists
            //println("Already existed")
            val updatedTime = temp._1 + timeDiff
              val updatedDistance = temp._2 + distance
              val updatedCount = temp._3 + 1
              l2LMap += ((second._6, first._6) ->(updatedTime, updatedDistance, updatedCount))
            }
            else{//if entry doesn't exist
              l2LMap += ((second._6, first._6) ->(timeDiff,distance,1L))
            }
            */
          }//else ignore
        }//else ignore
      }
    }//all users done
    l2lArray.toList.foreach{t=>
      fileWriter.println(t._1+"\t"+t._2+"\t"+t._3+"\t"+t._4) // actual time, l1, l2, user, time
    }

    /*
    l2LMap.toList.map(t=> (t._1,(t._2._1/t._2._3.toDouble,t._2._2/t._2._3.toDouble,t._2._3)))//computing average
      .foreach{t=>
      fileWriter.println(t._1._1+"\t"+t._1._2+"\t"+t._2._1+"\t"+t._2._2+"\t"+t._2._3)
    }
    */
    fileWriter.close()
    //.filter(t=> t._2._3>5).take(10).foreach(t=> println(t))
    println("total size::"+l2lArray.size)

  }

  def computeDeltaTnDeltaD(checkins: List[(Long, Date, Double, Double, String, Long, String)],timeT:Double, fileWrite:String): Unit ={
    val timeThreshold:Double=timeT// in hours
    val timeConversion=1000 *60*60*24 // in hours
    val fileWriter=new PrintWriter(new File(fileWrite+"_TimeWindow_"+timeThreshold+".txt"))
    val l2LMap: scala.collection.mutable.Map[(Long, Long), (Double,Double,Long)] = scala.collection.mutable.Map()
    val locsByUsers=checkins.groupBy(t=> t._1).map(t=> (t._1,t._2.sortBy(it=> it._2))).toArray
    fileWriter.println("Location1"+"\t"+"Location2"+"\t"+"Delta Time"+"\t"+"Delta Distance"+"\t"+"Count")
    val totalSize=locsByUsers.size
    for(i<-0 until locsByUsers.size){
      println("current,total::"+i,totalSize)
      //println("for user ::"+locsByUsers(i)._1)
      val usersCheckin=locsByUsers(i)._2
      for(j<-0 until usersCheckin.size-1){ //for check-ins of each users
        val first=usersCheckin(j)
        val second=usersCheckin(j+1)
        val timeDiff=(second._2.getTime-first._2.getTime).toDouble/timeConversion.toDouble
        if(timeDiff<= timeThreshold){
          if(second._6!=first._6){ // if locations are not equal
            val temp=l2LMap.getOrElse((second._6,first._6),null)
            val distance = findDistance(second._3, second._4, first._3, first._4)
            if(temp!=null) { //if entry already exists
              //println("Already existed")
              val updatedTime = temp._1 + timeDiff
              val updatedDistance = temp._2 + distance
              val updatedCount = temp._3 + 1
              l2LMap += ((second._6, first._6) ->(updatedTime, updatedDistance, updatedCount))
            }
            else{//if entry doesn't exist
              l2LMap += ((second._6, first._6) ->(timeDiff,distance,1L))
            }
          }//else ignore
        }//else ignore
      }
    }//all users done
    l2LMap.toList.map(t=> (t._1,(t._2._1/t._2._3.toDouble,t._2._2/t._2._3.toDouble,t._2._3)))
      .foreach{t=>
      fileWriter.println(t._1._1+"\t"+t._1._2+"\t"+t._2._1+"\t"+t._2._2+"\t"+t._2._3)
    }
    fileWriter.close()
      //.filter(t=> t._2._3>5).take(10).foreach(t=> println(t))
    println("total size::"+l2LMap.size)

  }

  def accumulateFriends(users:List[Long],usersByFriends:Map[Long,List[Long]]): ArrayBuffer[Long] ={
    var accUsers:ArrayBuffer[Long]=new ArrayBuffer[Long]()
    accUsers ++= users
    //println("initial users ::"+accUsers)
    users.foreach{u=>
      //println("user ::"+u)
      val friends=usersByFriends.getOrElse(u,null)
      //println("friends "+friends)
      if(friends!=null){
        accUsers ++= friends
      }
      else println("Error !!")
    }
    //println("final users::"+accUsers)
    return accUsers.distinct
  }

  def computeDeltaUsersUpdated(friends:List[(Long,Long)],checkins: List[(Long, Date, Double, Double, String, Long, String)],timeT:Double, fileWrite:String): Unit ={
    val timeThreshold:Double=timeT// in hours
    val conversionTime=1000 *60*60 // in hours
    val fileWriter=new PrintWriter(new File(fileWrite+"_TimeWindow_"+timeThreshold+".txt"))
    val l2LMap: scala.collection.mutable.Map[(Long, Long), ArrayBuffer[Long]] = scala.collection.mutable.Map()

    val vistorsByLocs=checkins.groupBy(t=> t._6).map(t=> (t._1,t._2.map(it=> it._1).distinct))
    //println("locations ")
    //vistorsByLocs.toList.sortBy(t=> -t._2.size).take(10).foreach(println)
    val friendsByUsers=friends.groupBy(t=> t._1).map(t=> (t._1,t._2.map(it=> it._2)))
    //println("users")
    //friendsByUsers.toList.sortBy(t=> -t._2.size).take(10).foreach(println)
    //friendsByUsers.toList.take(2).map(t=> t._1).foreach(println)

    //accumulateFriends(List(14221,2163),friendsByUsers)




    val locsByUsers=checkins.groupBy(t=> t._1).map(t=> (t._1,t._2.sortBy(it=> it._2))).toList
    fileWriter.println("Location1"+"\t"+"Location2"+"\t"+"Abs"+"\t"+"Relt"+"\t"+"AbsFriend"+"\t"+"ReltFriends")
    val totalSize=locsByUsers.size
    for(i<-0 until locsByUsers.size){
      val user:Long=locsByUsers(i)._1
      println("current,total::"+i,totalSize)
      //println("for user ::"+locsByUsers(i)._1)
      val usersCheckin=locsByUsers(i)._2
      for(j<-0 until usersCheckin.size-1){ //for check-ins of each users
      val first=usersCheckin(j)
        val second=usersCheckin(j+1)
        val timeDiff=(second._2.getTime-first._2.getTime).toDouble/conversionTime.toDouble
        if(timeDiff<= timeThreshold){
          if(second._6!=first._6){ // if locations are not equal
          val temp:ArrayBuffer[Long]=l2LMap.getOrElse((second._6,first._6),null)
            //val distance = findDistance(second._3, second._4, first._3, first._4)
            if(temp!=null) { //if entry already exists
            val gotList=temp += user
              l2LMap += ((second._6, first._6) ->gotList)
            }
            else{//if entry doesn't exist
              l2LMap += ((second._6, first._6) -> ArrayBuffer(user))
            }
          }//else ignore
        }//else ignore
      }
    }//all users done
    println("processing output")
    //val cc=0

    val result=l2LMap.toList.map(t=> (t._1,t._2.distinct)).filter(t=> t._2.size>1) //ignore one common visitors
        .map{t=> // l1,l2,#user
        val abs=t._2.size
        val totalVisitors=vistorsByLocs.getOrElse(t._1._2,null)
        var relt:Double=0.0
        if(totalVisitors!=null) {
          relt=abs.toDouble/totalVisitors.size
        } else{
          println("Error !!")
        }
        val absF=accumulateFriends(t._2.toList,friendsByUsers).size
        val totalVisitorsFriends=accumulateFriends(vistorsByLocs.getOrElse(t._1._2,null),friendsByUsers)
        val reltF=absF.toDouble/totalVisitorsFriends.size.toDouble

       /*if(abs==null || relt==null ||absF==null || reltF==null){
         println("error")
       }else println(t._1,t._2,abs,relt,absF,reltF)*/
        (t._1._1,t._1._2,abs,relt,absF,reltF)
      }

    result.foreach{t=>
      if(t._3>1){
        fileWriter.println(t._1+"\t"+t._2+"\t"+t._3+"\t"+t._4+"\t"+t._5+"\t"+t._6)//l1,l2,abs,relt,absF,reltF
      }
    }



    /*result
      .foreach{t=>
      if(t._3>1)
        fileWriter.println(t._1._1+"\t"+t._1._2+"\t"+t._3)
    }*/
    fileWriter.close()
    //.filter(t=> t._2._3>5).take(10).foreach(t=> println(t))
    println("total size::"+l2LMap.size)


  }

  def computeDeltaUsers(checkins: List[(Long, Date, Double, Double, String, Long, String)],timeT:Double, fileWrite:String): Unit ={
    val timeThreshold:Double=timeT// in hours
    val conversionTime=1000 *60*60 // in hours
    val fileWriter=new PrintWriter(new File(fileWrite+"_TimeWindow_"+timeThreshold+".txt"))
    val l2LMap: scala.collection.mutable.Map[(Long, Long), scala.collection.mutable.ListBuffer[Long]] = scala.collection.mutable.Map()
    val locsByUsers=checkins.groupBy(t=> t._1).map(t=> (t._1,t._2.sortBy(it=> it._2))).toList
    fileWriter.println("Location1"+"\t"+"Location2"+"\t"+"UsersSize")
    val totalSize=locsByUsers.size
    for(i<-0 until locsByUsers.size){
      val user:Long=locsByUsers(i)._1
      println("current,total::"+i,totalSize)
      //println("for user ::"+locsByUsers(i)._1)
      val usersCheckin=locsByUsers(i)._2
      for(j<-0 until usersCheckin.size-1){ //for check-ins of each users
      val first=usersCheckin(j)
        val second=usersCheckin(j+1)
        val timeDiff=(second._2.getTime-first._2.getTime).toDouble/conversionTime.toDouble
        if(timeDiff<= timeThreshold){
          if(second._6!=first._6){ // if locations are not equal
          val temp:ListBuffer[Long]=l2LMap.getOrElse((second._6,first._6),null)
            //val distance = findDistance(second._3, second._4, first._3, first._4)
            if(temp!=null) { //if entry already exists
            //println("Already existed")
            //val updatedTime = temp._1 + timeDiff
              //val updatedDistance = temp._2 + distance
              //*if(temp.distinct.size>1)
              //*println("returned list is ::"+temp)
              val gotList=temp += user
              //val updatedCount = temp._3 + 1
              l2LMap += ((second._6, first._6) ->gotList)
            }
            else{//if entry doesn't exist
              l2LMap += ((second._6, first._6) -> ListBuffer(user))
            }
          }//else ignore
        }//else ignore
      }
    }//all users done
    val result=l2LMap.toList.map(t=> (t._1,t._2.distinct)).map(t=> (t._1,t._2,t._2.size))
    result
      .foreach{t=>
      if(t._3>1)
      fileWriter.println(t._1._1+"\t"+t._1._2+"\t"+t._3)
    }
    fileWriter.close()
    //.filter(t=> t._2._3>5).take(10).foreach(t=> println(t))
    println("total size::"+l2LMap.size)

  }

}
