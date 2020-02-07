package VisotorsPrediction

import java.io.{File, PrintWriter}
import java.util.Date

import scala.collection.mutable.ListBuffer
import FormatData.fileReaderLBSN

/**
  * Created by XXX on XXX.
  */
class VPDataPreprator {

  val dirCheckinsCOld="XXX"
  val dirCheckinsCats="XXX"
  val dirVPData="XXX"

  def filterCheckinsOnLocsOld(checkinsFile:String, minVisits:Int)
  : List[(Long, Date, Double, Double, String, Long, String)] ={
    val fr=new fileReaderLBSN
    val checkins=fr.readCheckinFile(checkinsFile)
    val locs=checkins.groupBy(t=> t._6 ).filter(t=> t._2.size>=minVisits)
      .map(t=> (t._1,1))
    val filtCheckins=checkins.filter(t=> locs.contains(t._6))
    println(" filtered checkin on locations having min visits:"+minVisits+" are::"+filtCheckins.size)

    return filtCheckins

  }
  def filterCheckinsCatsOnLocs(checkinsFile:String, minVisits:Int)
  : List[(Long, Date, Double, Double, String, Long, String)] ={
    val fr=new fileReaderLBSN
    val checkins=fr.readCheckinsWithCats(checkinsFile)
    val locs=checkins.groupBy(t=> t._6 ).filter(t=> t._2.size>=minVisits)
      .map(t=> (t._1,1))
    val filtCheckins=checkins.filter(t=> locs.contains(t._6))
    println(" filtered checkin on locatios having min visits:"+minVisits+" are::"+filtCheckins.size)

    return filtCheckins

  }


  def getU2LMatrixData(): Unit ={
    /**User to location matrix*/
      val datasetList=List("WeeFull.txt")
    val minVisits=10
    val writer=new PrintWriter(new File(dirVPData+"Ul2L.txt_FilteredOn_"+minVisits))

    datasetList.foreach{ds=>
      val fr=new fileReaderLBSN
      val checkins=filterCheckinsCatsOnLocs(dirCheckinsCats+ds,minVisits)//fr.readCheckinFile(dirCheckinsC+ds)
      val locations=checkins.map(t=> t._6).distinct.sortBy(t=> t)
      val locMap=locations.map(t=> (t,0)).toMap
      writer.println("User: {frequency at Locs}")
      writer.println("User,"+locations.mkString(","))
      println("Total locations are::"+locations.size)
      var perUserLocs:Map[Long,Int]=Map()
      var userLocsRow:ListBuffer[Int]=new ListBuffer[Int]()
      //var row:ListBuffer[(Long,ListBuffer[Int])]=new ListBuffer[(Long, ListBuffer[Int])]()//user-loc=frequency
      val usersLocs=checkins.groupBy(t=> t._1)
      val totalUsers=usersLocs.map(t=> t._1  ).toList.distinct.size
      var countUsers=0
      //var forUser1:ListBuffer[Int]=new ListBuffer[Int]()
      //var forUser2:ListBuffer[Int]=new ListBuffer[Int]()
      //var check=false
      usersLocs.foreach{user=>
        countUsers += 1
        println("user id is::"+user._1)
        println("Current, total::"+countUsers,totalUsers)
        ///val userLocalMap=locMap.clone()
        perUserLocs=user._2.groupBy(t=> t._6).map(t=> (t._1,t._2.size))
        userLocsRow=new ListBuffer[Int]()
        val userLocalMap=locMap ++ perUserLocs
        /*if(countUsers==1){
          forUser1=userLocalMap.values.to[ListBuffer].sortBy(t=> t)
        }else if (countUsers==2){
          forUser2=userLocalMap.values.to[ListBuffer].sortBy(t=> t)
          check=true
        }
        if(check==true){
          if(forUser1==forUser2){
            println("Both maps are same")
          }else{
            println("No they are different")
          }
        }*/
        /*locations.foreach{l=>
          if(perUserLocs.contains(l)){
            val countLoc=perUserLocs.getOrElse(l,0)
            userLocsRow :+= countLoc
          }else{
            userLocsRow :+= 0
          }
        }*/
        //println("user locs row"+user._1,userLocsRow,userLocsRow.size)
        //println(userLocalMap.values.toList.sortBy(t=> t).size)
        writer.println(user._1+","+userLocalMap.toList.sortBy(t=> t._1).map(t=> t._2).mkString(","))
      }
      writer.close()
      println("U2U file written")
      //println("size of check-ins::"+checkins.size)
    }
  }

  def getU2CatMatrixData(): Unit ={
    /**User to location matrix*/
    val datasetList=List("WeeFull.txt")
    val minVisits=10
    val writer=new PrintWriter(new File(dirVPData+"U2Cat.txt_filteredOn_"+minVisits))//_filteredOn_"+minVisits
    datasetList.foreach { ds =>
      val fr = new fileReaderLBSN
      val checkins = filterCheckinsCatsOnLocs(dirCheckinsCats + ds,minVisits)//fr.readCheckinsWithCats(dirCheckinsCats + ds)
      var checkinCats:ListBuffer[(Long, Date, Double, Double, String, Long, String,String)]=new ListBuffer[(Long, Date, Double, Double, String, Long, String, String)]()
      checkins.foreach{ck=>
        ck._7.split(",").foreach{cat=>
          //if(cat!="n\\a"){
            checkinCats += ((ck._1,ck._2,ck._3,ck._4,ck._5,ck._6,ck._7,cat))
          //}
        }
      }
      val cats=checkinCats.map(t=> t._8).distinct.sortBy(t=> t)
      val catsMap=cats.map(t=> (t,0)).toMap
      println("new Checkin size::"+checkinCats.size)
      writer.println("User: {frequency at Categories}")
      writer.println("User,"+cats.mkString(","))
      println("Total categories are::"+cats.size)
      var perUserCats:Map[String,Int]=Map()
      var userCatsRow:ListBuffer[Int]=new ListBuffer[Int]()
      //var row:ListBuffer[(Long,ListBuffer[Int])]=new ListBuffer[(Long, ListBuffer[Int])]()//user-loc=frequency
      val usersCats=checkinCats.groupBy(t=> t._1)
      val totalUsers=usersCats.map(t=> t._1).toList.distinct.size
      var countUsers=0
      usersCats.foreach{user=>
        countUsers += 1
        println("Current, total::"+countUsers,totalUsers)
        ///val userLocalMap=locMap.clone()
        perUserCats=user._2.groupBy(t=> t._8).map(t=> (t._1,t._2.size))
        userCatsRow=new ListBuffer[Int]()
        val userLocalMap=catsMap ++ perUserCats
        //println("size:",userLocalMap.toList.sortBy(t=> t._1).map(t=> t._2).size)
        writer.println(user._1+","+userLocalMap.toList.sortBy(t=> t._1).map(t=> t._2).mkString(","))
      }
      writer.close()
      println("U2Cat file written")


    }
  }

  def getU2TimeMatrixData(): Unit ={
    /**User to location matrix*/
    val datasetList=List("WeeFull.txt")
    val minVisits=10
    val writer=new PrintWriter(new File(dirVPData+"U2Time.txt_filteredOn_"+minVisits))
    datasetList.foreach { ds =>
      val fr = new fileReaderLBSN
      val checkins = filterCheckinsCatsOnLocs(dirCheckinsCats + ds,minVisits)// fr.readCheckinsWithCats(dirCheckinsCats + ds)
      val checkinsTime=checkins.map{ck=>
        (ck._1,ck._2,ck._3,ck._4,ck._5,ck._6,ck._7,ck._2.getHours.toString )
      }
      var hours=checkinsTime.map(t=> t._8).distinct.sortBy(t=> t)
      var hoursMap=hours.map(t=> (t,0)).toMap
      println("new Checkin size::"+checkinsTime.size)
      writer.println("User: {frequency at hour of Day}")
      writer.println("User,"+hours.mkString(","))
      println("Total parts of days are::"+hours.size)
      var perUserHours:Map[String,Int]=Map()
      var userHoursRow:ListBuffer[Int]=new ListBuffer[Int]()
      //var row:ListBuffer[(Long,ListBuffer[Int])]=new ListBuffer[(Long, ListBuffer[Int])]()//user-loc=frequency
      val usersHours=checkinsTime.groupBy(t=> t._1)
      val totalUsers=usersHours.map(t=> t._1).toList.distinct.size
      var countUsers=0
      usersHours.foreach{user=>
        countUsers += 1
        println("Current, total::"+countUsers,totalUsers)
        ///val userLocalMap=locMap.clone()
        perUserHours=user._2.groupBy(t=> t._8).map(t=> (t._1,t._2.size))
        userHoursRow=new ListBuffer[Int]()
        val userLocalMap=hoursMap ++ perUserHours
        //println(userLocalMap.toList.sortBy(t=> t._1).map(t=> t._2))
        writer.println(user._1+","+userLocalMap.toList.sortBy(t=> t._1).map(t=> t._2).mkString(","))
      }
      writer.close()
      println("U2time file written")


    }
  }

}
