package ConvoysLBSN

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable.ListBuffer


/**
 * Created by XXX on 30-XXX-XXX.
 */
class ConvoyAnalysisFromCheckins {
  def stringToDate(dateString: String): Date = {
    val updateDate = dateString.replaceAll("T", "").replaceAll("Z", "")
    val formatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss")
    val date: Date = formatter.parse(updateDate)
    return date
  }
  //test

  def removeConsecutiveLocs(inconvoys: ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[(Double, Double)])]):
  ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[(Double, Double)])] = {
    //println("new one ::")
    val distConsLocs = inconvoys.map { t =>
      val actLocs: ListBuffer[Long] = t._2
      val actTS = t._3
      var newLocs: ListBuffer[Long] = new ListBuffer()
      var newTS: ListBuffer[(Double, Double)] = new ListBuffer()
      var tempTS: (Double, Double) = null
      var tempLoc: Long = 0L
      //println("before is ::"+actLocs)
      if (actLocs.size == 1) {
        newLocs = actLocs
        newTS = actTS
      }
      for (i <- 1 until actLocs.size) {
        //println("count::"+i)
        val preLoc = actLocs(i - 1)
        val currLoc = actLocs(i)
        val preTS = actTS(i - 1)
        val currTS = actTS(i)
        if (tempLoc == 0L) {
          // println("new tem")
          tempLoc = preLoc
          tempTS = preTS
        }
        //println("previous,current, tempLoc"+preLoc,currLoc,tempLoc)
        if (tempLoc == currLoc) {
          tempTS = (tempTS._1, currTS._2)
        } else if (tempLoc != currLoc) {
          newLocs += tempLoc
          newTS += tempTS
          //println("inserted"+tempLoc)
          tempLoc = 0L
          tempTS = null
        }
        if (i == actLocs.size - 1) {
          if (tempLoc != 0) {
            newLocs += tempLoc
            newTS += tempTS
          }
          //println("inserted"+tempLoc)
          //tempLoc=0L
          //tempTS=null
        }
      }
      //println("before::"+actLocs)
      //println("after::"+newLocs)
      if (newLocs.contains(0)) println("ERROR !!!!")
      (t._1, newLocs, newTS)

    }
    return distConsLocs
  }

  def timeStampToConvoy(CC: ListBuffer[(ListBuffer[Long], Long, Long)]): ListBuffer[Convoy] = {
    var V: ListBuffer[Convoy] = new ListBuffer()
    CC.foreach { c =>
      //println("originial c::"+c)
      var v: Convoy = new Convoy()
      v = v.createConvoy((c._1, ListBuffer(c._2), ListBuffer(c._3), c._3,c._3))
      //println("created convoy, users, loc, tid"+v.getUsers(),v.getLocations(),v.startTime,v.endTime)
      V += v
    }
    return V
  }

  def updateVnext(Vnext: ListBuffer[Convoy], vnew: Convoy): ListBuffer[Convoy] = {
    var added: Boolean = false
    //println("Vnext call::")
    //println("new potential insertion::"+vnew.getUsers(),vnew.getLocations(),vnew.getStartTime(),vnew.endTime)
    //println("intial Vnext::")
    //testing github
    //Vnext.foreach(t=> println("con:"+t.getUsers(),t.getLocations(),t.getStartTime(),t.endTime))
    Vnext.foreach { v: Convoy =>
      //if(v.getUsers().equals(vnew.getUsers()) && v.hasSameUsers(vnew)==false)
        //println("ERROR ____________________________________________--------------")
      if (v.hasSameUsers(vnew)) {
        if (v.getStartTime() > vnew.getStartTime()) {
          //v is a subconvoy of vnew
          Vnext -= v;
          Vnext += vnew;
          added = true;
        }
        else if (vnew.getStartTime() > v.getStartTime()) {
          //vnew is a subconvoy of v *****different from vcoda
          added = true;
        }
      }
    }
    if (added == false) {
      Vnext += vnew;
    }
    //println("final Vnext::")
    //Vnext.foreach(t=> println("con:"+t.getUsers(),t.getLocations(),t.getStartTime(),t.endTime))
    return Vnext;
  }

  /** Get all the convoys in the LBSN */
  def getConvoys(id:Int,checkins: List[(Long, Date, Double, Double, String, Long, String)],
                 deltaTS: Long): ((Int, Long), ListBuffer[Convoy]) = {
    //val formatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss")
    val timeUnit = (1000 * 60 ) // in minutes now //*24
    val TSMap = getDataSnapShots(checkins, deltaTS * timeUnit) //Divide the data set into SnapShots on the basis of given time unit
    println("Time Stamp Completed")
    println("Convoy mining started.")
    //val writeFile = new PrintWriter(new File(writeFilePath))
    //val writerValuesFile=new PrintWriter(new File(writeFileValues))
    val m = 2 // min. no. of users a convoy should have
    val k = 2 //min. no. of locs a convoy should have visited
    //val minConvoyDistinctLocSize = 2

    //writeFile.println("TSMAP size" + TSMap.size) //TSMAP=Array[(t1,t2),Hash[user,location]]
    //var convoys: ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[(Double, Double)])] = new ListBuffer() // all convoys
    //var currentConvoys: ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[(Double, Double)])] = new ListBuffer() // convoys that are currently progressing
    //Convoy=List[(Users,VisitedLocations,TimeStamps)]
    //var loc: Long = 0L
    //val start = 0 //3390//0
    //val end = TSMap.size
    // initialize all data structures here to save memory
    //var addConvoys: ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[(Double, Double)])] = new ListBuffer() // all convoys
    //var delConvoys: ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[(Double, Double)])] = new ListBuffer()
    //var filteredConvoys: ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[(Double, Double)])] = new ListBuffer()
    var CC: ListBuffer[(ListBuffer[Long], Long, Long)] = new ListBuffer()
    // new algorithm
    var Vnext: ListBuffer[Convoy] = new ListBuffer[Convoy]() // temp
    var V: ListBuffer[Convoy] = new ListBuffer[Convoy]() // next Convoys
    var Vpcc: ListBuffer[Convoy] = new ListBuffer[Convoy]() // final Convoys
    var C: ListBuffer[Convoy] = new ListBuffer[Convoy]() // current convoy
    var newTimeStamp:ListBuffer[Long]=new ListBuffer()
    var newLocs:ListBuffer[Long]=new ListBuffer()
    var time: Long = -1
    var loopCheck = true
    val total=TSMap.size
    var count=0
    var startTimeId:Long=TSMap(0)._3
    var endTimeId:Long=TSMap(TSMap.size-1)._3
    println("Time Ids Strat,End::"+startTimeId,endTimeId)
    TSMap.foreach { ts => // time stamp: (startTime,EndTime), Hash(user, locs), TimeStampID
      count= count +1
      if(count%1==0)
        //println(count,total) // can be commented off to track

      loopCheck = true
      CC = new ListBuffer()
      CC = ts._2.groupBy(t => t._2).map(t => (t._2.map(it => it._1).to[ListBuffer], t._1, ts._3)).to[ListBuffer]
      //CC=CC.filter(t=> t._1.contains(11) && t._1.contains(0))
      //println("Time stamp groups::"+CC)
      time = ts._3

      Vnext = new ListBuffer[Convoy]()
      if (CC != null && CC.size > 0) {
        C = timeStampToConvoy(CC)
        //println("Converted convoy::")
        //C.foreach(t=> println(t.getUsers(),t.getLocations()))
      } else {
        V.foreach { v: Convoy =>
          if (v.lifeTime() >= k && v.getLocations().size >=k) {
            Vpcc += v
          }
        }
        V = new ListBuffer[Convoy]()
        //CONTINUE: --------------missing statement
        loopCheck = false
      }
      if (loopCheck) {
        C.foreach { c: Convoy =>
          c.setMatched(false)
          c.setAbsorbed(false)
          //System.out.println(c)
        }
        V.foreach { v: Convoy =>
          v.setExtended(false)
          v.setAbsorbed(false)
          C.foreach { c:Convoy =>
            if (c.getUsers().size >= m && v.intersection(c).size >= m) {
              v.setExtended(true)
              c.setMatched(true)
              var vext: Convoy = new Convoy()
              //println("going to insert new convoy::"+v.intersection(c), c.getLocations(),v.getStartTime(), time)
              newLocs= if(v.getLocations().last == c.getLocations().last)v.getLocations() else v.getLocations()++c.getLocations()
              newTimeStamp=v.getTimeStamps() ++c.getTimeStamps()
              vext = vext.createConvoy(v.intersection(c), newLocs,newTimeStamp,v.getStartTime(), time)
              Vnext = updateVnext(Vnext, vext) /** update new one !! with more locations*/
              //println("testing v subset c")
              //println("v,c::"+v.getUsers(),c.getUsers())
              //println("v.getusers.cntains(c.getuser)"+v.getUsers().toSet.subsetOf(c.getUsers().toSet))
              if (v.isSubset(c)) {
                //println("true in loop")
                v.setAbsorbed(true)
              }
              if (c.isSubset(v)) {
                c.setAbsorbed(true)
              }
            }
          }
          if (!v.isAbsorbed()) {
            if (v.lifeTime() >= k && v.getLocations().size >=k) {
              Vpcc += v
            }
          }
        }
        C.foreach { c: Convoy =>
          if (!c.isAbsorbed()) {
            Vnext = updateVnext(Vnext, c)
          }
        }
        V = Vnext
        //++ time
      } //loop check
    }
    //println("V size is ::"+V.size)
    V.foreach { v:Convoy =>
      //println("convoy, life time::"+v.getUsers(),v.getLocations(),v.lifeTime())
      if (v.lifeTime() >= k && v.getLocations().size >=k) {
        //println("inside life time")
        Vpcc += v
      }

    }

    /*println("size is::"+Vpcc.size)
    Vpcc.foreach{t=>
      println("convoys "+t.getUsers(),t.getLocations(),t.getTimeStamps())
    }*/
    println("size is::"+Vpcc.size)
    val resultConvoys:ListBuffer[(ListBuffer[Long],ListBuffer[Long],ListBuffer[Long])]=new ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[Long])]()
    Vpcc.foreach{c=>
      //writeFile.println(c.getUsers(),c.getLocations(),c.getTimeStamps())
      //writeFile.println(c.getUsers().mkString(",")+"\t"+c.getLocations().mkString(",")+"\t"+c.getTimeStamps().mkString(","))
      resultConvoys += ((c.getUsers(),c.getLocations(),c.getTimeStamps()))
    }
    //writeFile.close()

    if(resultConvoys.size>0)
    println("result convoys size are ******************** ::"+resultConvoys.size)
    else println("size is 0")
    return  ((id,endTimeId),Vpcc)
  }
  //testing

  /** Remove conseuctive duplicates from the list */
  def compress[T](values: List[T]): List[T] =
    compressTail(Nil, values)

  def compressTail[T](seen: List[T], remaining: List[T]): List[T] =
    remaining match {
      case Nil => seen
      case x :: y :: xs if (x == y) => compressTail(seen, y :: xs)
      case x :: xs => compressTail(seen ::: List(x), xs)
    }

  /** Divide the LBSN into timeStamps on the basis of given time unit */
  def getDataSnapShots(checkins:List[(Long, Date, Double, Double, String, Long, String)], deltaTS: Long)
  : Array[((Double, Double), scala.collection.mutable.HashMap[Long, Long], Long)] = {
    //val df:DateFormat=new SimpleDateFormat("yyyy-mm-dd:hh:mm:ss")
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    // read check-ins from file

    val ckLines =checkins.map(t=> (t._1,t._2,t._3,t._4,t._5,t._6, t._2.toString,t._2.toString))
      .distinct
    /*
    scala.io.Source.fromFile(checkinsFile).getLines()//.take(10000)
      .map(t => t.split("\t"))
      .map(t => (t(0).toLong, stringToDate(t(1)), t(2).toDouble, t(3).toDouble, t(4), t(5).toLong, t(1), t(1))).toList.distinct //.toArray
    */
    println("check-ins size::"+ckLines.size)
    //ckLines = ckLines.filter(t => t._1 <= 3 && t._1 >0) // consider only these users
    //time unit to convert the visited time from millisecond to given unit
    //ckLines.sortBy(t=> t._2).foreach(t=> println(t._1,t._2,t._6))
    //val timeUnit = convertMSecTo //(1000 * 60 *60 ) // in hours now //*24
    val sortedByTime = ckLines.sortBy(t => t._2) //sort check-ins on the basis of time
    //println("minimum time::" + sortedByTime.head._8)
    //println("maximum time::" + sortedByTime.last._8)
    val minT = sortedByTime.head._2.getTime.toDouble // earliest check-in time
    val maxT = sortedByTime.last._2.getTime.toDouble //last check-in time
    //println("time is::"+deltaTS)
    val tsCount = Math.ceil((maxT - minT + 1) / (deltaTS)).toInt //total number of timeStamps
    //println("Total time stamps::" + tsCount)
    println("Start Time :: " + sdf.format(sortedByTime.head._2.getTime))
    println("End Time :: " + sdf.format(sortedByTime.last._2.getTime))
    println("array size is ::"+tsCount)

    val timeStampsMap: Array[((Double, Double), scala.collection.mutable.HashMap[Long, Long], Long)]
    = new Array[((Double, Double), scala.collection.mutable.HashMap[Long, Long], Long)](tsCount)

    // startTime,endTime, List[(user,location)]
    var startTime = minT
    var endTime = 0.0
    for (i <- 0 until timeStampsMap.size) {
      //find time ranges for each timeStamp
      endTime = startTime + deltaTS
      timeStampsMap(i) = ((startTime, endTime), scala.collection.mutable.HashMap(), i)
      //println("start Time::"+sdf.format(startTime))
      //println("end Time::"+sdf.format(endTime))
      startTime = endTime
    }

    // get all visited locations for each user

    val usersLocs = ckLines.groupBy(t => t._1)
      .map(t => (t._1, t._2.map(it => (it._6, it._2.getTime)).sortBy(iit => iit._2))) // u-> {(l,t),..}
    //usersLocs.toList.foreach(t=> println(t._1,t._2))
    val totatlUsers = usersLocs.size //total number of users
    println("total number of users::" + totatlUsers)
    var usersCount = 0
    usersLocs.foreach { t => //u,{(l,t)..}
      usersCount += 1
      if(usersCount%1000==0)
      println("current, total Users::" + usersCount, totatlUsers)
      //val total=t._2.size-2
      /** the last location of a user with-in a timestamp is considered its location throughout that time stamp
      this location will be repeated for all upcoming time stamps until user make a new check-in */
      var i = -1
      var lastTSCount = 0
      while (i < t._2.size - 2) {
        i = i + 1 //first time zero
        var current: (Long, Long) = null
        var next: (Long, Long) = null
        var ind1, ind2 = 0
        var countLoop = 0
        do {
          countLoop += 1
          if (countLoop != 1) {
            i = i + 1 // for first time, in do-while loop not incremented
          }
          current = t._2(i)
          next = t._2(i + 1)
          ind1 = Math.floor(((current._2 - (minT - 1)) / deltaTS)).toInt // timestamp of current check-in
          //println("current time, start, end::"+sdf.format(current._2),sdf.format(timeStampsMap(ind1)._1._1),sdf.format(timeStampsMap(ind1)._1._2))

          ind2 = Math.floor(((next._2 - (minT - 1)) / deltaTS)).toInt // time stamp of next check-in
        } while ((ind1 == ind2) && (i < t._2.size - 2)) //consider only last location of a user in a time stamp
        if (i < t._2.size - 2) {
          //add the same location to all timestamps until user make new check-in
          timeStampsMap(ind1)._2 += (t._1 -> current._1)
          for (a <- ind1 until ind2) {
            timeStampsMap(a)._2 += (t._1 -> current._1)
            //location on last/final time stamp is not set yet
          }
        }
        else if (i == t._2.size - 2 && ind1 == ind2) {
          //in second-last timestamp if current and next check-in at same loc
          timeStampsMap(ind2)._2 += (t._1 -> next._1)
          lastTSCount = 0
          for (lastTSCount <- (ind2 + 1) until tsCount) {
            timeStampsMap(lastTSCount)._2 += (t._1 -> next._1)
          }
        }
        else if (i == t._2.size - 2 && ind1 != ind2) {
          //in second-last timestamp if current and next check-in at same loc
          timeStampsMap(ind1)._2 += (t._1 -> current._1)
          timeStampsMap(ind2)._2 += (t._1 -> next._1)
          lastTSCount = 0
          for (lastTSCount <- (ind2 + 1) until tsCount) {
            timeStampsMap(lastTSCount)._2 += (t._1 -> next._1)
          }
        }
      }
    }
    //.filter(t=> t._2.toList.size>0)
    //timeStampsMap.
    /*timeStampsMap.sortBy(t => t._1._1).foreach { t =>
      //if(t._2._1)
      if(timeStampsMap.size<1)
        println("Empty")
      println(sdf.format(t._1._1),sdf.format(t._1._2),t._2.getOrElse(0,0L))
    }*/
    //timeStampsMap.take(1).foreach(t=> println((sdf.format(t._1._1),sdf.format(t._1._2)),t._2,t._3))
    //println(timeStampsMap(0)._1)
    //timeStampsMap=timeStampsMap.sortBy(t=> -t._1._1).take(3)
    //timeStampsMap=timeStampsMap.sortBy(t=> t._1._1).take(3)
    //timeStampsMap.foreach(t=> println(("check-ins::"+sdf.format(t._1._1),sdf.format(t._1._2)),t._2,t._3))
    //timeStampsMap=timeStampsMap.filter(t=> t._3>=32383 && t._3<=32529)
    return timeStampsMap // ={startTime,EndTime, Hash(user, locs), TimeStampID}
  }

  def findUserActivitiesTS(checkins: List[(Long, Date, Double, Double, String, Long, String)], deltaTS: Long, writeFile:String)
  : Unit ={
    val writer=new PrintWriter(new File(writeFile))
    val minGroupSize=2
    val groupActivities:ListBuffer[(Long,Long,Long)]= new ListBuffer() //user, location, time
    val timeUnit = (1000 * 60 )
    val timeSnaps=getDataSnapShots(checkins,deltaTS * timeUnit)
    timeSnaps.foreach{ts=>
      //val timeStampActsA = ts._2.groupBy(t => t._1) //group by user .. each user has only one location
        //.map(t =>  (t._1,t._2.map(it => it._2).to[ListBuffer], ts._3)).to[ListBuffer]
      val timeStampActs= ts._2.toList.map(it=> (it._1,it._2,ts._3))
        //.filter(t=> t._1.size>=minGroupSize)
      //.map(t=> t.)
      groupActivities ++= timeStampActs
    }
    println("total size is ::"+groupActivities.size)
    val newGroups:ListBuffer[(Long,Long,ListBuffer[Long])]=new ListBuffer()
    var timeList:ListBuffer[Long]=new ListBuffer()
    var outerCount=0
    groupActivities.groupBy(t=> t._1).foreach{gc=>
      outerCount+=1
      if(outerCount%100==0){
        println("outerCount::"+outerCount)
      }
      //println("new one !! started")
      timeList=new ListBuffer()
      for(i:Int<-1 until gc._2.size){
        if(i%10000==0){
          println("inner count:: i, total::"+i,gc._2.size)
        }

        val previous=gc._2(i-1)
        //println("previous::"+previous)
        val newOne=gc._2(i)
        //println("new one:;"+newOne)
        if(newOne._2==previous._2 && newOne._1==previous._1 && newOne._3==(previous._3+1)){ //loc=loc, user=user,time=time+1
          timeList += previous._3
          //println("not inserted")
        }else{
          timeList +=previous._3
          newGroups += ((previous._1,previous._2,timeList))
          //println("inserted ::"+previous)
          //println("with time stamps::"+timeList)
          timeList=new ListBuffer()
        }
      }
    }
    println("new user check-ins size::"+newGroups.size)
    newGroups.foreach{g=>
      writer.println(g._1+"\t"+g._2+"\t"+g._3.mkString(","))
    }
    writer.close()

  }

  def findGroupActivities(checkins: List[(Long, Date, Double, Double, String, Long, String)], deltaTS: Long, writeFile:String)
  : Unit ={
    val writer=new PrintWriter(new File(writeFile))
    val minGroupSize=2
    val groupActivities:ListBuffer[(ListBuffer[Long],Long,Long)]= new ListBuffer()
    val timeUnit = (1000 * 60 )
    val timeSnaps=getDataSnapShots(checkins,deltaTS * timeUnit)
    timeSnaps.foreach{ts=>
      val timeStampGroups = ts._2.groupBy(t => t._2)
        .map(t =>  (t._2.map(it => it._1).to[ListBuffer], t._1, ts._3)).to[ListBuffer]
        .filter(t=> t._1.size>=minGroupSize)
        //.map(t=> t.)
      groupActivities ++= timeStampGroups
    }
    println("total size is ::"+groupActivities.size)
    val newGroups:ListBuffer[(ListBuffer[Long],Long,ListBuffer[Long])]=new ListBuffer()
    var timeList:ListBuffer[Long]=new ListBuffer()
    groupActivities.groupBy(t=> t._1).foreach{gc=>
      //println("new one !! started")
      timeList=new ListBuffer()
      for(i:Int<-1 until gc._2.size){

        val previous=gc._2(i-1)
        //println("previous::"+previous)
        val newOne=gc._2(i)
        //println("new one:;"+newOne)
        if(newOne._2==previous._2 && newOne._1==previous._1 && newOne._3==(previous._3+1)){
          timeList += previous._3
          //println("not inserted")
        }else{
          timeList +=previous._3
          newGroups += ((previous._1,previous._2,timeList))
          //println("inserted ::"+previous)
          //println("with time stamps::"+timeList)
          timeList=new ListBuffer()
        }
    }
    }
    println("new Group size::"+newGroups.size)
    newGroups.foreach{g=>
      writer.println(g._1.mkString(",")+"\t"+g._2+"\t"+g._3.mkString(","))
    }
    writer.close()

  }
}
