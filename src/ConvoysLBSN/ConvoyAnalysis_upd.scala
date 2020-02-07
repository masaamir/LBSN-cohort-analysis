package ConvoysLBSN

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable.ListBuffer


/**
 * Created by XXX on 30-XXX-XXX.
 */
class ConvoyAnalysis_upd {
  def stringToDate(dateString: String): Date = {
    val updateDate = dateString.replaceAll("T", "").replaceAll("Z", "")
    val formatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss")
    val date: Date = formatter.parse(updateDate)
    return date
  }
  def removeConsecutiveLocs(inconvoys: ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[(Double, Double)])]):
  ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[(Double, Double)])] ={
    val distConsLocs=inconvoys.map{t=>
      val actLocs:ListBuffer[Long]=t._2
      val actTS=t._3
      var newLocs:ListBuffer[Long]=new ListBuffer()
      var newTS:ListBuffer[(Double,Double)]=new ListBuffer()
      var tempTS:(Double,Double)=null
      var tempLoc:Long=0L
      if(actLocs.size==1){
        newLocs=actLocs
        newTS=actTS
      }
      for(i<-1 until actLocs.size){
        val preLoc=actLocs(i-1)
        val currLoc=actLocs(i)
        val preTS=actTS(i-1)
        val currTS=actTS(i)
        if(tempLoc==0L){
          tempLoc=preLoc
          tempTS=preTS
        }
        if(tempLoc==currLoc){
          tempTS=(tempTS._1,currTS._2)
        }else if (tempLoc !=currLoc ){
          newLocs+=tempLoc
          newTS+=tempTS
          tempLoc=0L
          tempTS=null
        }
        if(i==actLocs.size-1){
          if(tempLoc!=0 ){
          newLocs+=tempLoc
          newTS+=tempTS
          }
        }
      }
      if(newLocs.contains(0))println("ERROR !!!!")
      (t._1,newLocs,newTS)

    }
    return distConsLocs
  }

  /** Get all the convoys in the LBSN */
  def getConvoys(checkinsFile: String, friendsFile: String, deltaTS: Long, writeFilePath: String, writeFileValues: String): Unit = {
    val formatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss")
    val timeUnit = (1000 * 60 * 60) // in hours now //*24
    val TSMap = getDataSnapShots(checkinsFile, friendsFile, deltaTS * timeUnit, timeUnit) //Divide the data set into SnapShots on the basis of given time unit
    val writeFile=new PrintWriter(new File(writeFilePath))
    //val writerValuesFile=new PrintWriter(new File(writeFileValues))
    val minConvoySize = 2 // min. no. of users a convoy should have
    val minConvoyLocSize = 2 //min. no. of locs a convoy should have visited
    val minConvoyDistinctLocSize = 2


    writeFile.println("TSMAP size"+TSMap.size) //TSMAP=Array[(t1,t2),Hash[user,location]]
    var convoys: ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[(Double, Double)])] = new ListBuffer() // all convoys
    var currentConvoys: ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[(Double, Double)])] = new ListBuffer() // convoys that are currently progressing
    //Convoy=List[(Users,VisitedLocations,TimeStamps)]
    var loc: Long = 0L
    val start = 0//3390//0
    val end = TSMap.size
    // initialize all data structures here to save memory

    var addConvoys: ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[(Double, Double)])] = new ListBuffer() // all convoys
    var delConvoys: ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[(Double, Double)])] = new ListBuffer()
    var filteredConvoys: ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[(Double, Double)])] = new ListBuffer()
    var TimeStampGroups:ListBuffer[(ListBuffer[Long],ListBuffer[Long],ListBuffer[(Double,Double)])]=new ListBuffer()
    for (i <- start until end) {
      // group time stamped check-ins on the basis of visited location

      if(i%100==0)
      println("count, total ::"+i,end)
      TimeStampGroups = TSMap(i)._2.groupBy(t => t._2).map(t => (t._2.map(it => it._1).to[ListBuffer],
        ListBuffer(t._1), ListBuffer(TSMap(i)._1))).to[ListBuffer]
      TimeStampGroups=TimeStampGroups.filter(t => t._1.size >= minConvoySize && !t._2.contains(0L))
      addConvoys= new ListBuffer() // all convoys
      delConvoys= new ListBuffer() // all convoys
      currentConvoys = currentConvoys.filter(t => t._1.size >= minConvoySize) // filter current convoys

      writeFile.println("count::"+i)
      writeFile.println("convoys::"+convoys.map(t=> (t._1,t._2,t._3.map(it=> (formatter.format(it._1),formatter.format(it._2))))))
      writeFile.println("current Convoys::"+currentConvoys.map(t=> (t._1,t._2,t._3.map(it=> (formatter.format(it._1),formatter.format(it._2))))))
      writeFile.println("TimeStamp::"+TimeStampGroups.map(t=> (t._1,t._2,t._3.map(it=> (formatter.format(it._1),formatter.format(it._2))))))


      currentConvoys.foreach { cc =>
        writeFile.println("current convoy::"+cc)
        delConvoys += cc
        TimeStampGroups.foreach { g =>
          //if( g._2.contains(0))println("ERROR >>>>>>>>>>>>>>>>>>>>>")
          //println("inside convoy")
          writeFile.println("cc,g::"+cc,g)
          if (cc._1 == g._1) {
            //writeFile.println("equal users")
            // if continuous convoy, i.e, same users with new location ( or same loc)
            addConvoys += ((cc._1, cc._2.++(g._2), cc._3.++(Traversable(TSMap(i)._1))))
            //delConvoys += cc
            writeFile.println("add convoys::"+((cc._1, cc._2.++(g._2), cc._3.++(Traversable(TSMap(i)._1)))))
          }
          else if (cc._1.toSet.intersect(g._1.toSet).size >= minConvoySize) {
            // if convoy break then make new one
            writeFile.println("intersect bigger")
            addConvoys += ((cc._1.intersect(g._1), cc._2.++(g._2), cc._3.++(Traversable(TSMap(i)._1))))
            //delConvoys += cc
            if(cc._2.size>=minConvoyLocSize)// because the convoy broke, so old should be considered as a convoy
            convoys += cc
            writeFile.println("size bigger so inserted in Per Convoy::"+cc)
            if (g._1.size >= minConvoySize) {
              addConvoys += ((g._1, g._2, g._3))
              writeFile.println("new convoy since size is bigger")
            }
          }
          else  { //if (cc._1 != g._1)
            // if group users not matched with existing convoy, they always should be new one
            writeFile.println("else condition")
            addConvoys += ((g._1, g._2, ListBuffer(TSMap(i)._1)))
            writeFile.println("add cc::"+((g._1, g._2, cc._3.++(Traversable(TSMap(i)._1)))))
            if(cc._2.size>=minConvoyLocSize && !convoys.contains(cc)) {
              // contain is expensive can be remove from hashmap
              writeFile.println("loc size::" + cc._2.size, cc._2)
              convoys += cc // if doesn't already exist
              writeFile.println("add to perm Convoy::" + cc)
            }
          }
        }
        //currentConvoys=currentConvoys.distinct
        //println
      }
      writeFile.println("current convoy before delt before addition::"+currentConvoys)
      currentConvoys --= delConvoys
      writeFile.println("delete convoys::"+delConvoys)
      writeFile.println("current convoy after delt before addition::"+currentConvoys)
      currentConvoys ++= addConvoys
      writeFile.println("current convoy after delt after addition::"+currentConvoys)
      if (currentConvoys.size < 1) {
        // whenever current convoys is empty initialize it with group stamped
        writeFile.println("first addition")
        currentConvoys ++= TimeStampGroups
      }
      if(i==end-1){
        writeFile.println("last one")
        convoys ++= currentConvoys.filter(t => t._2.size >= minConvoyLocSize)
      }
      currentConvoys=currentConvoys.distinct
      convoys=convoys.filter(t=> t._2.size>minConvoyDistinctLocSize).distinct
      //println( "sizes ::"+currentConvoys.size, convoys.size)

    } // end timeStamped snapshots
    //println("before,after distinct size::"+convoys.size,convoys.distinct.size)
    convoys=removeConsecutiveLocs(convoys)
    convoys=convoys.distinct
    //println("before,after distinct size::"+convoys.size,convoys.distinct.size)


    //convoys=convoys.map(t=> (t._1.distinct,t._2.distinct,t._3.distinct))
    println("No. of convoys size of users >="+minConvoySize+"| and minLocs >="+minConvoyLocSize+" minDistincLocs >="+minConvoyDistinctLocSize+
      ":: " + convoys.size, convoys.filter(t => compress(t._2.toList).size >= minConvoyDistinctLocSize).size)
    convoys = convoys.filter(t => compress(t._2.toList).size >= minConvoyDistinctLocSize) // filter convoys that with minimum distinct locations

    filteredConvoys = new ListBuffer()
    for (i <- 0 until convoys.size) {
      // maintain only maximal convoys
      //println(i)
      val current = convoys(i)
      var contained = false
      for (j <- 0 until convoys.size) {
        if (i != j) {
          val itConvoy = convoys(j)
          if (current._1.forall(itConvoy._1.contains) && current._2.forall(itConvoy._2.contains) && current._3.forall(itConvoy._3.contains)) {
            contained = true
          }
        }
      }
      if (contained == false) {
        filteredConvoys += current

      }
    }

    println("filtered before,after distinct size::"+filteredConvoys.size,filteredConvoys.distinct.size,filteredConvoys.toSet.size)

    filteredConvoys=filteredConvoys.distinct
    filteredConvoys.foreach(t=> println(t._1,t._2,t._3.map(it=> (formatter.format(it._1),formatter.format(it._2)))))
    //writeFile.println("#Users,#UniqueLocations,#Locations,#Times")
    // convoys with of only users, without considering friends
    filteredConvoys.foreach{c=>
      writeFile.println(c._1,c._2.distinct,c._2,c._3)
    }


    writeFile.close()
    /*
      //Filter convoys that have at least minimum number of users
      currentConvoys = currentConvoys.filter(t => t._1.size >= minConvoySize) //if users are greater than limit
      val tempCurrentConvoy = currentConvoys.clone()
      tempCurrentConvoy.foreach { c =>
        val usersLoc: ListBuffer[(Long, Long)] = c._1
          .map { u => // find user's locations
          loc = TSMap(i)._2.getOrElse(u, 0L)
          (u, loc)
          }
        val newLocGroup = usersLoc.groupBy(t => t._2)
        //newLocGroup.foreach(t=> println("location group::"+t))
        val filteredLocGroup = newLocGroup.filter(t => t._2.size >= minConvoySize && t._1 != 0L) // filter groups that have at least min users and the location is not NULL.
        //filteredLocGroup.foreach(t=> println("location group::"+t))
        if (c._2.size >= minConvoyLocSize && newLocGroup.size >= 1) {//update to 1 from minConvoyDistinctLocSize
          // check if size of convoy have been to multiple locations and broke at this point
          convoys += c
        } else if (c._2.size >= minConvoyLocSize && newLocGroup.size == 1 && newLocGroup.contains(0L)) {
          // add convoy if its users have no next location
          convoys += c
        }
        filteredLocGroup.foreach { t => // update current convoys that may have some part in next timestamp
          currentConvoys += ((t._2.map(it => it._1), c._2.++(Traversable(t._1)), c._3.++(Traversable(TSMap(i)._1)))) //(t._2.map(it=> it._1),c._2+=t._1,c._3+=TSMap(i)._1)
        }
        currentConvoys = currentConvoys - c // delete the considered convoy, after considering its next parts
      }
      if (i == end - 1) {
        //In last timeStamp current convoys are added to convoys
        convoys ++= currentConvoys.filter(t => t._2.size >= minConvoyLocSize)
      }
    }
    // remove two consecutive same locations from the visit history, which happens due to limited time stamp

    //convoys=removeConsecutiveLocs(convoys)




    //convoys=convoys.map(t=> (t._1,co))
    println("No. of convoys size of users >="+minConvoySize+"| and minLocs >="+minConvoyLocSize+" minDistincLocs >="+minConvoyDistinctLocSize+
      ":: " + convoys.size, convoys.filter(t => compress(t._2.toList).size >= minConvoyDistinctLocSize).size)
    convoys = convoys.filter(t => compress(t._2.toList).size >= minConvoyDistinctLocSize) // filter convoys that with minimum distinct locations
    var filteredConvoys: ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[(Double, Double)])] = new ListBuffer()
    for (i <- 0 until convoys.size) {
      // maintain only maximal convoys
      println(i)
      val current = convoys(i)
      var contained = false
      for (j <- 0 until convoys.size) {
        if (i != j) {
          val itConvoy = convoys(j)
          if (current._1.forall(itConvoy._1.contains) && current._2.forall(itConvoy._2.contains) && current._3.forall(itConvoy._3.contains)) {
            contained = true
          }
        }
      }
      if (contained == false) {
        filteredConvoys += current
      }
    }




    /** At this point we have convoys (group of users that share consecutively share the locations)
      * based on minimum users, total locations and distinct locations */
    /** These convoys can further be processed on to find cliques or connected Components */
    filteredConvoys.foreach(t=> println(t._1,t._2,t._3.map(it=> (formatter.format(it._1),formatter.format(it._2)))))



    //distConsLocs.foreach(t=> println(t._1,t._2,t._3.map(it=> (formatter.format(it._1),formatter.format(it._2)))))
    println("filtered convoys size::"+filteredConvoys.size)
    writeFile.println("#Users,#UniqueLocations,#Locations,#Times")
    // convoys with of only users, without considering friends
    filteredConvoys.foreach{c=>
      writeFile.println(c._1,c._2.distinct,c._2,c._3)
    }
    writeFile.close()

*/

    /** */
    /*val convoysWithCliques=getConvoyWithCliques(filteredConvoys,friendsFile)
    convoysWithCliques.foreach{c=>
      writeFile.println(c._1,c._2.distinct,c._2,c._3)
    }*/
    /*
    val cc=new ConnectedComponentFinder()
    val convoysWithCC=cc.getConvoyWithConnectedComponents(filteredConvoys,friendsFile,minConvoySize)
    convoysWithCC.foreach{c=>
      //writeFile.println(c._1,c._2.distinct,c._2,c._3)
      writeFile.println(c._1.mkString(",")+"\t"+compress(c._2.toList).mkString(",")+"\t"+c._2.mkString(",")+"\t"+c._3.mkString(","))
      //writeFile.println(c._1.mkString(",")+"\t"+c._2.distinct.mkString(",")+"\t"+c._2.mkString(",")+"\t"+c._3.mkString(","))
      writerValuesFile.println(c._1.size+"\t"+compress(c._2.toList).size+"\t"+c._2.size+"\t"+c._3.size)
      //(1,2,3)
      //t=> t._6
      /*writeFile.println{
        c._1.foreach(t=> writeFile.print(t+","))+"\t"+c._2.distinct.
          foreach(t=> writeFile.print(t+","))+"\t"+c._2.foreach(t=> writeFile.print(t+","))+"\t"+c._3.foreach(t=> writeFile.print(t+","))
      }*/
    }
    writeFile.close()
    writerValuesFile.close()
    */

  }

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
  def getDataSnapShots(checkinsFile: String, friendsFile: String, deltaTS: Long, convertMSecTo: Long)
  : Array[((Double, Double), scala.collection.mutable.HashMap[Long, Long])] = {
    //val df:DateFormat=new SimpleDateFormat("yyyy-mm-dd:hh:mm:ss")
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    // read check-ins from file

    var ckLines = scala.io.Source.fromFile(checkinsFile).getLines().map(t => t.split("\t"))
      .map(t => (t(0).toLong, stringToDate(t(1)), t(2).toDouble, t(3).toDouble, t(4), t(5).toLong, t(1), t(1))).toList.distinct //.toArray
    ckLines=ckLines.filter(t=> t._1<=5) // consider only these users
    //time unit to convert the visited time from millisecond to given unit
    //ckLines.sortBy(t=> t._2).foreach(t=> println(t._1,t._2,t._6))
    val timeUnit = convertMSecTo //(1000 * 60 *60 ) // in hours now //*24
    val sortedByTime = ckLines.sortBy(t => t._2) //sort check-ins on the basis of time
    //println("minimum time::" + sortedByTime.head._8)
    //println("maximum time::" + sortedByTime.last._8)
    val minT = sortedByTime.head._2.getTime.toDouble // earliest check-in time
    val maxT = sortedByTime.last._2.getTime.toDouble //last check-in time
    //println("time is::"+deltaTS)
    val tsCount = Math.ceil((maxT - minT + 1) / (deltaTS)).toInt //total number of timeStamps
    //println("Total time stamps::" + tsCount)
    println("Time conversion:: " + sdf.format(sortedByTime.head._2.getTime))
    println("Time conversion:: " + sdf.format(sortedByTime.last._2.getTime))

    val timeStampsMap: Array[((Double, Double), scala.collection.mutable.HashMap[Long, Long])]
    = new Array[((Double, Double), scala.collection.mutable.HashMap[Long, Long])](tsCount)
    // startTime,endTime, List[(user,location)]
    var startTime = minT
    var endTime = 0.0
    for (i <- 0 until timeStampsMap.size) {
      //find time ranges for each timeStamp
      endTime = startTime + deltaTS
      timeStampsMap(i) = ((startTime, endTime), scala.collection.mutable.HashMap())
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
      println("current, total Users::" + usersCount, totatlUsers)
      //val total=t._2.size-2
      /** the last location of a user with-in a timestamp is considered its location throughout that time stamp
      this location will be repeated for all upcoming time stamps until user make a new check-in */
      var i = -1
      var lastTSCount=0
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
            //location on lastest/final time stamp is not set yet
          }
        }
        else if (i == t._2.size - 2 && ind1 == ind2) {
          //in second-last timestamp if current and next check-in at same loc
          timeStampsMap(ind2)._2 += (t._1 -> next._1)
          lastTSCount=0
          for(lastTSCount<-(ind2+1) until tsCount){
            timeStampsMap(lastTSCount)._2 += (t._1 -> next._1)
          }
        }
        else if (i == t._2.size - 2 && ind1 != ind2) {
          //in second-last timestamp if current and next check-in at same loc
          timeStampsMap(ind1)._2 += (t._1 -> current._1)
          timeStampsMap(ind2)._2 += (t._1 -> next._1)
          lastTSCount=0
          for(lastTSCount<-(ind2+1) until tsCount){
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
    //timeStampsMap.foreach(t=> println((sdf.format(t._1._1),sdf.format(t._1._2)),t._2))
    return timeStampsMap // =startTime,EndTime, Hash(user, locs)
  }
}
