package LBSNAnalysis

import java.io.{File, PrintWriter}

import net.sf.javaml.clustering.DensityBasedSpatialClustering
import net.sf.javaml.core._
import net.sf.javaml.distance.EuclideanDistance

import scala.collection.mutable.ListBuffer

/**
 * Created by XXX on 4/12/2016.
 */
class DataSetFormatter {
  def formatFSDataSet(venuesFile: String, friendsFile: String, checkinFile: String, friendWrite: String, checkInWrite: String): Unit = {

    val fWriter = new PrintWriter(new File(friendWrite))
    val chkWriter = new PrintWriter(new File(checkInWrite))
    val venues = scala.io.Source.fromFile(venuesFile).getLines().drop(2).map(t => t.split("\\|"))
      .map { t =>
      if (t(1).trim != "" && t(2).trim != "") {
        (t(0).trim.toLong, (t(1).trim.toDouble, t(2).trim.toDouble))
      }
      else (t(0).trim.toLong, (0.0, 0.0))
    }.toMap


    val chkLines = scala.io.Source.fromFile(checkinFile).getLines().drop(2).map(t => t.split("\\|"))
      .map(t => (t(1).trim, t(5).trim.replace(' ', 'T') + "Z", t(3).trim, t(4).trim, "LocStr", t(2).trim)).toList //.take(10) //
    chkLines.foreach { t =>
      val coord = venues.getOrElse(t._6.toLong, null)
      if (coord == null) {
        println("null")
        chkWriter.println(t._1 + "\t" + t._2 + "\t" + t._3 + "\t" + t._4 + "\t" + t._5 + "\t" + t._6)
      }
      else
        chkWriter.println(t._1 + "\t" + t._2 + "\t" + coord._1 + "\t" + coord._2 + "\t" + t._5 + "\t" + t._6)
    }
    chkWriter.close()

    val visitors = chkLines.map(t => t._1).distinct
    val visitorsMap = visitors.map(t => (t, 1)).toMap
    //To be converted Format= users \t time \t lat \t lon \t stringLoc \t LongLoc

    println("first printed...")
    val friendsLines = scala.io.Source.fromFile(friendsFile).getLines().drop(2)
      .map(t => t.split("\\|")).filter(t => t.size == 2)
      .map(t => (t(0).trim, t(1).trim))
      .filter(t => visitorsMap.getOrElse(t._1, null) != null && visitorsMap.getOrElse(t._2, null) != null) //visitors.contains(t(0)) && visitors.contains(t(1)
      .toList //.take(10)
    println("size is ::" + friendsLines.size)
    var count = 0
    friendsLines.foreach { t =>
      count = count + 1
      println(count)
      fWriter.println(t._1 + "\t" + t._2)
    }
    println("second printed..")
    fWriter.close()
  }

  def formatWEEDataSet(friendsFile: String, checkinFile: String, friendWrite: String, checkInWrite: String): Unit = {
    // assign a mapping to users and locations
    val fWriter = new PrintWriter(new File(friendWrite))
    val chkWriter = new PrintWriter(new File(checkInWrite))
    //val chkLines=scala.io.Source.fromFile(checkinFile).getLines().toList.take(10).foreach(println)
    /*.map(t=> t.split("\\|")).map(t=> (t(1).trim,t(5).trim.replace(' ','T')+"Z",t(3).trim,t(4).trim,"LocStr",t(2).trim)).toList//.take(10) //
  chkLines.foreach{t=>
    chkWriter.println(t._1+"\t"+t._2+"\t"+t._3+"\t"+t._4+"\t"+t._5+"\t"+t._6)
  }*/
    //To be converted Format= users \t time \t lat \t lon \t stringLoc \t LongLoc
    /** Create unique ids for each user and map name to respective id */

    var uniqueUsers: ListBuffer[String] = new ListBuffer[String]()
    var usersIdMap: scala.collection.mutable.Map[String, Long] = scala.collection.mutable.Map[String, Long]()
    val friendsLines = scala.io.Source.fromFile(friendsFile).getLines().drop(1).map(t => t.split(",")).toList //.take(10).foreach(println)
    friendsLines.foreach { t =>
      uniqueUsers += t(0)
      uniqueUsers += t(1)
    }
    println("users: total, unique::" + uniqueUsers.size, uniqueUsers.distinct.size)
    uniqueUsers = uniqueUsers.distinct
    var countUser: Long = 0
    uniqueUsers.foreach { uu =>
      countUser += 1
      usersIdMap += (uu -> countUser)
    }
    //usersIdMap.foreach(t=> println(t))
    /** Create unique ids for each location and map location name to respective id */
    var uniqueLocs: ListBuffer[String] = new ListBuffer[String]()
    var uniqueCoords: ListBuffer[(String, String)] = new ListBuffer[(String, String)]()
    var locIdMap: scala.collection.mutable.Map[String, Long] = scala.collection.mutable.Map[String, Long]()
    var coordIdMap: scala.collection.mutable.Map[(String, String), Long] = scala.collection.mutable.Map[(String, String), Long]()
    // Actual is userid,placeid,datetime,lat,lon,city,category
    val chkLines = scala.io.Source.fromFile(checkinFile).getLines().drop(1).map(t => t.split(",")).toList //.take(100)//.map(t=> (t(1)))//.take(10).foreach(println)
    chkLines.foreach { t =>
      if (t.size > 2) {
        uniqueLocs += t(1)
        uniqueCoords += ((t(3), t(4)))
      }
      else {
        println("anomaly in data set::" + t.mkString(","))
      }
    }
    println("locs: total, unique::" + uniqueLocs.size, uniqueLocs.distinct.size)
    println("coords: total, unique::" + uniqueCoords.size, uniqueCoords.distinct.size)
    uniqueLocs = uniqueLocs.distinct
    uniqueCoords = uniqueCoords.distinct
    var countLocs: Long = 0
    uniqueLocs.foreach { ul =>
      countLocs += 1
      locIdMap += (ul -> countLocs)
    }
    var countCoords: Long = 0
    uniqueCoords.foreach { uc =>
      countCoords += 1
      coordIdMap += (((uc._1, uc._2) -> countCoords))
    }

    println("writing files")

    /** Write files */
    //friends
    println("friends size::" + friendsLines.size)
    friendsLines.foreach { fp =>
      val firstUser = usersIdMap.getOrElse(fp(0), null)
      val secondUser = usersIdMap.getOrElse(fp(1), null)
      if (firstUser == null || secondUser == null) {
        println("did something wrong for users!!")
      } else {
        fWriter.println(firstUser + "\t" + secondUser)
      }
    }
    fWriter.close()

    //location
    chkLines.foreach { ck =>
      val user = usersIdMap.getOrElse(ck(0), null)
      val loc = locIdMap.getOrElse(ck(1), null)
      val cord = coordIdMap.getOrElse((ck(3), ck(4)), null)
      if (user == null || loc == null || cord == null) {
        println("string,user,loc" + ck.mkString("::"), user, loc, cord)
        println("did something wrong for checkins")
      } else {
        chkWriter.println(user + "\t" + ck(2) + "Z" + "\t" + ck(3) + "\t" + ck(4) + "\t" + cord + "\t" + loc)
      }
    }
    chkWriter.close()

  }

  def getUniqueLocIdWithCrds(checkinFile: String): Unit = {
    val chkLines = scala.io.Source.fromFile(checkinFile).getLines().map(t => t.split("\t")).toList.map(t => (t(4), (t(2), t(3)))).distinct
    //val locsPerCords= chkLines.map(t=> (t(5),(t(2),t(3))))
    println("Locs per coordinates")
    //val locsPer=chkLines.groupBy(t=> t._1).filter(t=> t._2.size>1).foreach(t=> println(t._1,t._2.size))
    println("Coordinates per location")
    val cordsPerLoc = chkLines.groupBy(t => t._2).filter(t => t._2.size > 1).toList.sortBy(t => -t._2.size).take(100).foreach(t => println(t._1, t._2.size)) //t._2.map(t=> t._1)
    //println("total cordiantes that are belong to multiple locs:"+cordsPerLoc.size)
  }

  def analyzeData(checkinFile: String, chkin: String): Unit = {
    /*
    // Wee Data
    val chkLines=scala.io.Source.fromFile(checkinFile).getLines().map(t=> t.split(",")).map(t=> (t(1),(t(3),t(4)))).toList.distinct
      .groupBy(t=> t._2).filter(t=> t._2.size>1)
    .map(t=> (t._1,t._2.map(it=> it._1))).toList.sortBy(t=> -t._2.size).take(10).foreach(t=> println(t))
*/
    /*
    //Gwowalla
    val chkLines=scala.io.Source.fromFile(checkinFile).getLines().map(t=> t.split(",")).map(t=> (t(0))).toList.distinct//.take(10).foreach(t=> println(t.mkString(",")))
      println("size is ::"+chkLines.size)
    val chk2=scala.io.Source.fromFile(chkin,"latin1").getLines().map(t=> t.split(",")).map(t=> (t(0))).toList.distinct
    println("size second::"+chk2)
    val sum=chk2 ++ chkLines
    println("unique count::"+sum.distinct.size)*/
    //.map(t=> (t(1),(t(3),t(4)))).toList.distinct
    //.groupBy(t=> t._2).filter(t=> t._2.size>1)
    //.map(t=> (t._1,t._2.map(it=> it._1))).toList.sortBy(t=> -t._2.size).take(10).foreach(t=> println(t))


    val chk1 = scala.io.Source.fromFile(checkinFile).getLines().map(t => t.split(",")).drop(1).toList
      .map(t => (t(0), (t(2), t(3)))).distinct
    println("first size ::" + chk1.size)
    val chk2 = scala.io.Source.fromFile(chkin, "latin1").getLines().map(t => t.split(",")).drop(1).toList
      .map(t => (t(0), (t(1), t(2)))).distinct
    println("second size ::" + chk2.size)
    val total = chk1 ++ chk2
    println("total size ::" + total.size)
    //total=total.distinct
    println("total distinct size ::" + total.size)
    /*val coordsPerLoc=total.groupBy(t=> t._1).filter(t=> t._2.size>1).toList.sortBy(t=> -t._2.size)
    println("total number of locations that have more coords are::"+coordsPerLoc.size)
    coordsPerLoc.take(10)
      .foreach { t =>
      println("location::" + t._1)
      t._2.foreach(it => println(it._2._1, it._2._2))
    }*/

    // locations per coordinate
    val LocsPerCoord = total.groupBy(t => t._2).filter(t => t._2.size > 1).toList.sortBy(t => -t._2.size)
    println("total size is::" + LocsPerCoord.size)
  }

  def formatGWDataSet(venue1File: String, venue2File: String, friendsFile: String, checkinFile: String, friendWrite: String, checkInWrite: String): Unit = {
    val fWriter = new PrintWriter(new File(friendWrite))
    val chkWriter = new PrintWriter(new File(checkInWrite))

    val venue1 = scala.io.Source.fromFile(venue1File).getLines().drop(1)
      .map(t => t.split(",")).map(t => (t(0).toLong, (t(2).toDouble, t(3).toDouble))).toMap
    val venue2 = scala.io.Source.fromFile(venue2File, "latin1").getLines().drop(1)
      .map(t => t.split(",")).map(t => (t(0).toLong, (t(1).toDouble, t(2).toDouble))).toMap
    val venues = venue1 ++ venue2
    println("venue size::" + venues.size)


    var notFoundCount = 0

    val chkLines = scala.io.Source.fromFile(checkinFile).getLines().drop(1) //.take(100000)//.take(10).foreach(t=> println(t))
    println("checkins Size::" + chkLines.size)
    /*.map(t=> t.split(","))
    .map{t=>
    val coords=venues.getOrElse(t(1).trim.toLong,null)
    if(coords!=null) {
      (t(0), t(2), coords._1, coords._2, "str", t(1))
    }
    else {//println("notFound")
      notFoundCount += 1
      (t(0), t(2), "0.0", "0.0", "str", t(1))
    }
  }.toList//.take(10) //
  println("checkins size::"+chkLines.size)
  println("notFound count::"+notFoundCount)
  chkLines.foreach{t=>
    chkWriter.println(t._1+"\t"+t._2+"\t"+t._3+"\t"+t._4+"\t"+t._5+"\t"+t._6)
    //println(t._1+"\t"+t._2+"\t"+t._3+"\t"+t._4+"\t"+t._5+"\t"+t._6)
  }
  chkWriter.close()
  //val users=chkLines.map(t=> t._1).distinct
  //To be converted Format= users \t time \t lat \t lon \t stringLoc \t LongLoc

  println("first printed...")

  val friendsLines=scala.io.Source.fromFile(friendsFile).getLines().drop(1)
    .map(t=> t.split(",")).filter(t=> t.size==2).map(t=> (t(0).trim,t(1).trim)).toList//.take(10) //.filter(t=> users.contains(t(0)) && users.contains(t(1)))
  println("size is ::"+friendsLines.size)
  var count=0
  friendsLines.foreach{t=>
    count =count +1
    //println(count)
    fWriter.println(t._1+"\t"+t._2)
  }
  println("second printed..")
  fWriter.close()*/

  }


  def findGPSDistance(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {

    //distance in meters
    val earthRadius: Double = 6371000.0;
    //meters
    val dLat: Double = Math.toRadians(lat2.toDouble - lat1.toDouble);
    val dLng: Double = Math.toRadians(lon2.toDouble - lon1.toDouble);
    val a: Double = Math.sin(dLat.toDouble / 2.0) * Math.sin(dLat.toDouble / 2.0) +
      Math.cos(Math.toRadians(lat1.toDouble)) * Math.cos(Math.toRadians(lat2.toDouble)) *
        Math.sin(dLng.toDouble / 2.0) * Math.sin(dLng / 2.0);
    val c: Double = 2.0 * Math.atan2(Math.sqrt(a.toDouble), Math.sqrt(1.0 - a.toDouble));
    val distance: Double = (earthRadius.toDouble * c.toDouble);
    //println("distance is::"+distance)

    //return dist;
    // eucledian distance
    //val distance=math.sqrt(math.pow((lat1 - lat2), 2) + math.pow((lon1 - lon2), 2))
    //math.sqrt(math.pow((x1 - y1), 2) + math.pow((x2 - y2), 2))
    //if(distance<1 && distance>0)
    //println("distance is::"+distance)
    return distance

  }

  def findLocationIds(filePath: String): Unit = {
    val lines = scala.io.Source.fromFile(filePath).getLines().drop(2).toList
      .map(t => t.split("\\|")).map(t => (t(0).trim, (t(1).trim, t(2).trim)))
    /*val coordsPerLoc=lines.groupBy(t=> t._1).filter(t=> t._2.size>1).toList.sortBy(t=> -t._2.size)
    println("total size is::"+coordsPerLoc.size)
    coordsPerLoc.take(10)
      .foreach{t=>
      println("location::"+t._1)
      t._2.foreach(it=> println(it._2._1,it._2._2))
      */


    // locations per coordinate
    //val LocsPerCoord=lines.groupBy(t=> t._2).filter(t=> t._2.size>1).toList.sortBy(t=> -t._2.size)
    //println("total size is::"+LocsPerCoord.size)
    /*LocsPerCoord//.take(10)
        .foreach{t=> println(t._1, t._2.size)
        //println("coordinates::"+t._1)
        //t._2.foreach(it=> println(it._1))
    }*/
    //val locs=lines.map(t=> t._1).distinct
    //println("total locations ::"+locs.size)
    //val coords=lines.map(t=> t._2).distinct
    //println("total unique coords::"+coords.size)


  }

  def clusterLocations(checkinFile: String, writeFile: String): Unit = {
    val minClusterSize = 1
    val eps = 10 //in meters
    val writer = new PrintWriter(new File(writeFile + "_" + eps + ".txt"))
    val chks = scala.io.Source.fromFile(checkinFile).getLines().toList //.drop(2).toList
    println("total checkins::" + chks.size)
    val locs = chks.map(t => t.split("\t")).map(t => (t(5), (t(2).toDouble, t(3).toDouble))).distinct
    println("total unique locations-coordinate pairs::" + locs.size)
    val locIds = locs.map(t => t._1).distinct
    println("total unique location ids ::" + locs.size)
    val coords = locs.map(t => t._2).distinct
    println("total unique coordinates NEW ::" + coords.size)
    coords.foreach { t =>
      writer.println(t._1 + "," + t._2)
    }


    /** Data set generation*/
    val dataset:Dataset =new DefaultDataset()

    var instance:Instance= null
    coords.foreach{t=>
      instance=new DenseInstance(Array(t._1,t._2))
      dataset.add(instance)
    }
    println("dat set size ::"+dataset.size())
    val dbsc=new DensityBasedSpatialClustering(eps,minClusterSize,new distanceAmongCoords)//distanceAmongCoords

    val clusters=dbsc.cluster(dataset)
    println("number of clusters::"+clusters.size)

    var idCount=0
    //var itr:Iterator[Instance]=null
    var current:Instance=null
    clusters.foreach{t=>
      idCount += 1
        val itr = t.iterator()
        while (itr.hasNext) {
          current = itr.next()
          //println("string is::"+idCount+"\t"+current.get(0)+"\t"+current.get(1))
          writer.println(idCount+"\t"+current.get(0)+"\t"+current.get(1))
        }
        //println("convoys::"+t)
    }
    writer.close()
  }

  def convertGPSToXY(checkinFile: String, xyzFile: String): Unit = {
    val xyzWriter = new PrintWriter(new File(xyzFile))
    val chks = scala.io.Source.fromFile(checkinFile).getLines().toList //.drop(2).toList
    println("total checkins::" + chks.size)
    val locs = chks.map(t => t.split("\t")).map(t => (t(5), (t(2).toDouble, t(3).toDouble))).distinct
    println("total unique locations-coordinate pairs::" + locs.size)
    val locIds = locs.map(t => t._1).distinct
    println("total unique location ids ::" + locs.size)
    val coords = locs.map(t => t._2).distinct
    println("total unique coordinates::" + coords.size)
    val r = 6371000 //meters

    val xyzCoords = coords.map(t => (r * Math.cos(t._1.toRadians) * Math.cos(t._2.toRadians),
      r * Math.cos(t._1.toRadians) * Math.sin(t._2.toRadians), r * Math.sin(t._1.toRadians)))
    println("converted Unique Coordinates(total,unique)::" + xyzCoords.size)
    xyzCoords.foreach { t =>
      xyzWriter.println(t._1 + " " + t._2 + " " + t._3)
    }
    xyzWriter.close()
  }

  def DBSCAN(): Unit = {
    //val lines=scala.io.Source.fromFile(checkinFile).getLines().drop(2).toList
    //.map(t=> t.split("\\|")).map(t=> (t(0).trim,(t(1).trim,t(2).trim)))


    val ds: Dataset = new DefaultDataset()
    val first = Array(25.667713, -100.386301)
    val second = Array(33.900483, -117.890205)
    val third = Array(35.900483, -119.890205)
    val d = findGPSDistance(33.900483, -117.890205, 25.667713, -100.386301)
    println("distance is ::" + d)


    val instance1: Instance = new DenseInstance(first)
    //instance.put(25,-100.386301)
    //instance.put(2,-100.386301)
    ds.add(instance1)

    val instance2: Instance = new DenseInstance(second)
    //instance2.put(1,33.900483)
    //instance.put(2,-117.890205)
    ds.add(instance2)
    val instance3: Instance = new DenseInstance(third)
    ds.add(instance3)
    val d2 = new distanceAmongCoords
    val test2 = d2.measure(instance2, instance3)
    println("distance check:" + test2)
    //ds.
    /** Test density based spatial clustering */
    //new ManhattanDistance
    val clusters = new DensityBasedSpatialClustering(10, 1, new EuclideanDistance)
    val test = clusters.cluster(ds)
    println("size is ::" + test.size)
    test.foreach(t => println(t))

  }
}
