package LBSNAnalysis

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import org.jgrapht.graph.{DefaultEdge, SimpleGraph}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
 * Created by MXXX on 30-XXX-XXX.
 */
class ConveyAnalysis_org {
  def stringToDate(dateString: String): Date = {
    val updateDate = dateString.replaceAll("T", "").replaceAll("Z", "")
    val formatter: SimpleDateFormat = new SimpleDateFormat("yyyy-mm-ddhh:mm:ss")
    val date: Date = formatter.parse(updateDate)
    return date
  }

  /** Get all the convoys in the LBSN */
  def getConvoys(checkinsFile: String, friendsFile: String, deltaTS: Long, writeFilePath: String, writeFileValues: String): Unit = {
    val timeUnit = (1000 * 60 * 60) // in hours now //*24
    val TSMap = getDataSnapShots(checkinsFile, friendsFile, deltaTS, timeUnit) //Divide the data set into SnapShots on the basis of given time unit
    val writeFile = new PrintWriter(new File(writeFilePath))
    val writerValuesFile = new PrintWriter(new File(writeFileValues))
    val minConvoySize = 2 // min. no. of users a convoy should have
    val minConvoyLocSize = 2 //min. no. of locs a convoy should have visited
    val minConvoyDistinctLocSize = 2
    //writeFile.println("TSMAP size"+TSMap.size) //TSMAP=Array[(t1,t2),Hash[user,location]]
    var convoys: ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[(Double, Double)])] = new ListBuffer() // all convoys
    var currentConvoys: ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[(Double, Double)])] = new ListBuffer() // convoys that are currently progressing
    //Convoy=List[(Users,VisitedLocations,TimeStamps)]
    var loc: Long = 0L
    val start = 1
    val end = TSMap.size
    for (i <- start until end) {
      //find convoys
      println("time stamp, total::" + i, end)
      //TSMap(i).
      // group time stamped check-ins on the basis of visited location
      val TimeStampGroups = TSMap(i - 1)._2.groupBy(t => t._2).map(t => (t._2.map(it => it._1).to[ListBuffer],
        ListBuffer(t._1), ListBuffer(TSMap(i - 1)._1))).to[ListBuffer].filter(t => t._1.size >= minConvoySize)
      //{users},loc,{startTime,endTime}
      TimeStampGroups.foreach { g => // only keep the maximal convoys in current convoy

        var containCheck = false
        currentConvoys.foreach { cc =>
          if (g._1.forall(cc._1.contains)) {
            containCheck = true // will be covered in next iteration
          }
        }
        if (containCheck == false) {
          currentConvoys += g
        }
      }
      //Filter convoys that have at least minimum number of users
      currentConvoys = currentConvoys.filter(t => t._1.size >= minConvoySize) //if users are greater than limit
      val tempCurrentConvoy = currentConvoys.clone()
      tempCurrentConvoy.foreach { c =>
        val usersLoc: ListBuffer[(Long, Long)] = c._1.map { u => // find user's locations
          loc = TSMap(i)._2.getOrElse(u, 0L)
          (u, loc)
        }
        val newLocGroup = usersLoc.groupBy(t => t._2)
        val filteredLocGroup = newLocGroup.filter(t => t._2.size >= minConvoySize && t._1 != 0L) // filter groups that have at least min users and the location is not NULL.
        if (c._2.size >= minConvoyLocSize && newLocGroup.size >= minConvoyDistinctLocSize) {
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
    //convoys=convoys.map(t=> (t._1,co))
    println("convoys size: total, locs >=2 " + convoys.size, convoys.filter(t => compress(t._2.toList).size >= minConvoyDistinctLocSize).size)
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
    //filteredConvoys.foreach(t=> println(t))

    //println("filtered convoys size::"+filteredConvoys.size)
    //writeFile.println("#Users,#UniqueLocations,#Locations,#Times")
    /** */
    /*val convoysWithCliques=getConvoyWithCliques(filteredConvoys,friendsFile)
    convoysWithCliques.foreach{c=>
      writeFile.println(c._1,c._2.distinct,c._2,c._3)
    }*/
    val cc = new ConnectedComponentFinder()
    val convoysWithCC = cc.getConvoyWithConnectedComponents(filteredConvoys, friendsFile, minConvoySize)
    convoysWithCC.foreach { c =>
      //writeFile.println(c._1,c._2.distinct,c._2,c._3)
      writeFile.println(c._1.mkString(",") + "\t" + compress(c._2.toList).mkString(",") + "\t" + c._2.mkString(",") + "\t" + c._3.mkString(","))
      //writeFile.println(c._1.mkString(",")+"\t"+c._2.distinct.mkString(",")+"\t"+c._2.mkString(",")+"\t"+c._3.mkString(","))
      writerValuesFile.println(c._1.size + "\t" + compress(c._2.toList).size + "\t" + c._2.size + "\t" + c._3.size)
      //(1,2,3)
      //t=> t._6
      /*writeFile.println{
        c._1.foreach(t=> writeFile.print(t+","))+"\t"+c._2.distinct.
          foreach(t=> writeFile.print(t+","))+"\t"+c._2.foreach(t=> writeFile.print(t+","))+"\t"+c._3.foreach(t=> writeFile.print(t+","))
      }*/
    }
    writeFile.close()
    writerValuesFile.close()
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

  /** Input: vertices and edges, Output: cliques among these vertices */
  def getCliques(vertices: ListBuffer[Long], edges: ListBuffer[(Long, Long)]): Unit = {
    val graph: SimpleGraph[Long, DefaultEdge] = new SimpleGraph[Long, DefaultEdge](classOf[DefaultEdge])
    vertices.foreach(t => graph.addVertex(t)) //add vertices
    edges.foreach(t => graph.addEdge(t._1, t._2)) //add edges
    //Get all the cliques in the graph
    val cf = new CliqueFinder[Long, DefaultEdge](graph)
    val cliques = cf.getAllMaximalCliques()
    println("cliques in the graph are ::" + cliques)
  }

  /** Find the cliques in a set by giving all the maximal cliques in the graph */
  def getCliquesOfSubset(items: ListBuffer[Long], maximalCliques: ListBuffer[Set[Long]]): ListBuffer[Set[Long]] = {

    val resultCliques: ListBuffer[Set[Long]] = new ListBuffer[Set[Long]]()
    val newCliques = maximalCliques.map(t => t.toSet.intersect(items.toSet)).filter(t => t.size > 1).distinct
    for (i <- 0 until newCliques.size) {
      // keep only maximal cliques
      var containedCheck = false
      val outer = newCliques(i)
      for (j <- 0 until newCliques.size) {
        if (i != j) {
          val inner = newCliques(j)
          if (outer.forall(inner.contains)) {
            containedCheck = true
          }
        }
      }
      if (containedCheck == false) {
        resultCliques += outer
      }
    }
    return resultCliques

  }

  /** Get cliques for each user group in convoys */
  def getConvoyWithCliques(filterdConvoy: ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[(Double, Double)])],
                           friendsFilePath: String): ListBuffer[(List[Long], ListBuffer[Long], ListBuffer[(Double, Double)])] = {
    val friendslines = scala.io.Source.fromFile(friendsFilePath).getLines().map(t => t.split("\t"))
      .map(t => (t(0).toLong, t(1).toLong)).toList.distinct //Get friends
    val friends = friendslines.groupBy(t => t._1) //Get each user's friends
    var users: ListBuffer[Long] = new ListBuffer[Long]()
    filterdConvoy.foreach { fc =>
      users ++= fc._1 // Get Vertices: All the users that are involved in convoys called convoyUsers
    }
    users = users.distinct // Filter only unique visitors
    val edges: ListBuffer[(Long, Long)] = new ListBuffer[(Long, Long)]()
    users.foreach { t =>
      val uf = friends.getOrElse(t, null).filter(it => users.contains(it._2)) // only consider those friends that are in convoyUsers
      if (uf != null) {
        edges ++= uf
      }
    }
    val graph: SimpleGraph[Long, DefaultEdge] = new SimpleGraph[Long, DefaultEdge](classOf[DefaultEdge])
    users.foreach(t => graph.addVertex(t)) //add vertices
    edges.foreach(t => graph.addEdge(t._1, t._2)) //add edges

    val cf = new CliqueFinder[Long, DefaultEdge](graph)
    val cliques = cf.getAllMaximalCliques() //Get all the cliques in the graph
    val scalaCliques: ListBuffer[Set[Long]] = new ListBuffer[Set[Long]]()
    cliques.foreach { t =>
      scalaCliques += t.toSet //convert from java to scala collection
    }
    val filteredCliqueConvoys: ListBuffer[(List[Long], ListBuffer[Long], ListBuffer[(Double, Double)])] = new
        ListBuffer[(List[Long], ListBuffer[Long], ListBuffer[(Double, Double)])]()
    filterdConvoy.foreach { fc =>
      val cliqueGroups = getCliquesOfSubset(fc._1, scalaCliques) //get cliques in a particular user group in a convoy
      cliqueGroups.foreach { t =>
        filteredCliqueConvoys += ((t.toList, fc._2, fc._3)) // add in main clique convoy list
      }
    }
    return filteredCliqueConvoys
  }

  /** Divide the LBSN into timeStamps on the basis of given time unit */
  def getDataSnapShots(checkinsFile: String, friendsFile: String, deltaTS: Long, convertMSecTo: Long)
  : Array[((Double, Double), scala.collection.mutable.HashMap[Long, Long])] = {
    // read check-ins from file
    val ckLines = scala.io.Source.fromFile(checkinsFile).getLines().map(t => t.split("\t"))
      .map(t => (t(0).toLong, stringToDate(t(1)), t(2).toDouble, t(3).toDouble, t(4), t(5).toLong, t(1))).toList.distinct //.toArray
    //time unit to convert the visited time from millisecond to given unit
    val timeUnit = convertMSecTo //(1000 * 60 *60 ) // in hours now //*24
    val sortedByTime = ckLines.sortBy(t => t._2) //sort check-ins on the basis of time
    val minT = sortedByTime.head._2.getTime.toDouble / timeUnit.toDouble // earliest check-in time
    val maxT = sortedByTime.last._2.getTime.toDouble / timeUnit.toDouble //last check-in time
    val tsCount = Math.ceil((maxT - minT + 1) / (deltaTS).toDouble).toInt //total number of timeStamps

    val timeStampsMap: Array[((Double, Double), scala.collection.mutable.HashMap[Long, Long])] = new Array[((Double, Double), scala.collection.mutable.HashMap[Long, Long])](tsCount) // startTime,endTime, List[(user,location)]
    var startTime = minT

    var endTime = 0.0
    for (i <- 0 until timeStampsMap.size) {
      //find time ranges for each timeStamp
      endTime = startTime + deltaTS
      timeStampsMap(i) = ((startTime, endTime), scala.collection.mutable.HashMap())
      startTime = endTime
    }
    // get all visited locations for each user
    val usersLocs = ckLines.groupBy(t => t._1)
      .map(t => (t._1, t._2.map(it => (it._6, it._2.getTime.toDouble / timeUnit.toDouble)).sortBy(iit => iit._2))) // u-> {(l,t),..}
    val totatlLocz = usersLocs.size //total number of locations
    var usersCount = 0

    usersLocs.foreach { t => //u,{(l,t)..}
      usersCount += 1
      println("current, total Locs::" + usersCount, totatlLocz)
      //val total=t._2.size-2
      /** the last location of a user with-in a timestamp is considered its location throughout that time stamp
      this location will be repeated for all upcoming time stamps until user make a new check-in */
      var i = -1
      while (i < t._2.size - 2) {
        i = i + 1 //first time zero
        var current: (Long, Double) = null
        var next: (Long, Double) = null
        var ind1, ind2 = 0
        var countLoop = 0
        do {
          countLoop += 1
          if (countLoop != 1) {
            i = i + 1 // for first time, in do-while loop not incremented
          }
          current = t._2(i)
          next = t._2(i + 1)
          ind1 = Math.floor(((current._2 - (minT - 1)) / deltaTS.toDouble)).toInt // timestamp of current check-in
          ind2 = Math.floor(((next._2 - (minT - 1)) / deltaTS.toDouble)).toInt // time stamp of next check-in
        } while ((ind1 == ind2) && (i < t._2.size - 2)) //consider only last location of a user in a time stamp
        if (i < t._2.size - 2) {
          //add the same location to all timestamps until user make new check-in
          timeStampsMap(ind1)._2 += (t._1 -> current._1)
          for (a <- ind1 until ind2) {
            timeStampsMap(a)._2 += (t._1 -> current._1)
          }
        }
        else if (i == t._2.size - 2 && ind1 == ind2) {
          //in second-last timestamp if current and next check-in at same loc
          timeStampsMap(ind2)._2 += (t._1 -> next._1)
        }
        else if (i == t._2.size - 2 && ind1 != ind2) {
          //in second-last timestamp if current and next check-in at same loc
          timeStampsMap(ind1)._2 += (t._1 -> current._1)
          timeStampsMap(ind2)._2 += (t._1 -> next._1)
        }
      }
    }
    return timeStampsMap // =startTime,EndTime, Hash(user, locs)
  }
}
