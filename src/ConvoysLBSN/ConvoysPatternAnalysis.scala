package ConvoysLBSN

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date
import FormatData.fileReaderLBSN
import LBSNAnalysis.CliqueFinder
import org.jgrapht.alg.ConnectivityInspector

import org.jgrapht.graph.{DefaultEdge, SimpleGraph}

import scala.collection.JavaConversions._
import Basic._
import scala.collection.mutable.ListBuffer

/**
 * Created by MAamir on 9/23/2016.
 */
class ConvoysPatternAnalysis {
  //github test new
  var filteredFriends: Map[Long, List[(Long, Long)]] = Map()

  def readFile(convoysFile: String): ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[Long])] = {
    //List[(List[Long],List[Long],List[Long])]

    val convoys = scala.io.Source.fromFile(convoysFile).getLines().to[ListBuffer]
      .drop(1)
      .map(t => t.toString)
      .map(t => t.replaceFirst("\\(", "").replaceAll("\\)", "").replaceAll("\\(", ""))
      .map(t => t.split(",ListBuffer"))
      .map(t => (t(0).replaceAll("ListBuffer", ""), t(1), t(2)))
      .map(t => (t._1.split(","), t._2.split(","), t._3.split(",")))
      .map(t => (t._1.map(it => it.trim.toLong).to[ListBuffer], t._2.map(it => it.trim.toLong).to[ListBuffer], t._3.map(it => it.trim.toLong).to[ListBuffer]))
    //convoys.foreach(t=> println("users::"+t._1+" locations::"+t._2+" timestamps::"+t._3))
    return convoys
  }

  def readConvoysFile(convoysFile:String): ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[Long])] ={
    val convoys = scala.io.Source.fromFile(convoysFile).getLines().to[ListBuffer]
      .map(t=> t.split("\t")).map(t=> (t(0).split(","),t(1).split(","),t(2).split(",")))
      .map(t=> (t._1.map(it=> it.trim.toLong).to[ListBuffer],t._2.map(it=> it.trim.toLong).to[ListBuffer],t._3.map(it=> it.trim.toLong).to[ListBuffer]))

    return convoys
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
  def getConvoyWithCliques(filterdConvoy: ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[Long])],
                           friendsFilePath: String): ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[Long])] = {
    val friendslines = scala.io.Source.fromFile(friendsFilePath).getLines().map(t => t.split("\t"))
      .map(t => (t(0).toLong, t(1).toLong)).toList.distinct //Get friends
    //val friends = friendslines.groupBy(t => t._1) //Get each user's friends
    var users: ListBuffer[Long] = new ListBuffer[Long]()
    filterdConvoy.foreach { fc =>
      users ++= fc._1 // Get Vertices: All the users that are involved in convoys called convoyUsers
    }
    users = users.distinct // Filter only unique visitors
    val usersMap=users.map(t=> (t,1)).toMap

    var edges: ListBuffer[(Long, Long)] = new ListBuffer[(Long, Long)]()
    /*users.foreach { t =>
      var uf = friends.getOrElse(t, null)
      if (uf != null)
        uf = uf.filter(it => users.contains(it._2)) // only consider those friends that are in convoyUsers
      if (uf != null) {
        edges ++= uf
      }
    }*/
    //alternate for finding edges

    edges=friendslines.filter{t=>
      usersMap.contains(t._1) && usersMap.contains(t._2)
    }.to[ListBuffer]
    val graph: SimpleGraph[Long, DefaultEdge] = new SimpleGraph[Long, DefaultEdge](classOf[DefaultEdge])
    users.foreach(t => graph.addVertex(t)) //add vertices
    edges.foreach(t => graph.addEdge(t._1, t._2)) //add edges

    val cf = new CliqueFinder[Long, DefaultEdge](graph)
    val cliques = cf.getAllMaximalCliques() //Get all the cliques in the graph
    val scalaCliques: ListBuffer[Set[Long]] = new ListBuffer[Set[Long]]()

    cliques.foreach { t =>
      scalaCliques += t.toSet //convert from java to scala collection
    }
    val filteredCliqueConvoys: ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[Long])] = new
        ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[Long])]()
    filterdConvoy.foreach { fc =>
      val cliqueGroups = getCliquesOfSubset(fc._1, scalaCliques) //get cliques in a particular user group in a convoy
      cliqueGroups.foreach { t =>
        filteredCliqueConvoys += ((t.toList.to[ListBuffer], fc._2, fc._3)) // add in main clique convoy list
      }
    }
    return filteredCliqueConvoys.distinct
  }

  def getConnectedComponents(vertices: ListBuffer[Long], minConvoyUsers: Long): ListBuffer[Set[Long]] = {
    val edges: ListBuffer[(Long, Long)] = ListBuffer[(Long, Long)]()
    val innerGraph: SimpleGraph[Long, DefaultEdge] = new SimpleGraph[Long, DefaultEdge](classOf[DefaultEdge])
    println("vertices are::" + vertices)
    vertices.foreach { v =>
      val vf = filteredFriends.getOrElse(v, null)
      println("actual friends are ::" + vf)
      if (vf != null) {
        edges ++= vf.filter(t => vertices.contains(t._2))
        println("filtered friends are ::" + vf.filter(t => vertices.contains(t._2)))
      }
    }

    vertices.foreach(n => innerGraph.addVertex(n))
    edges.foreach(e => innerGraph.addEdge(e._1, e._2))
    val innerCI: ConnectivityInspector[Long, DefaultEdge] = new ConnectivityInspector(innerGraph)
    val cc = innerCI.connectedSets()
    var scalaCC: ListBuffer[Set[Long]] = new ListBuffer[Set[Long]]()
    cc.foreach { t =>
      scalaCC += t.toSet //convert from java to scala collection
    }
    scalaCC = scalaCC.filter(t => t.size >= minConvoyUsers)
    println("connected components are::" + scalaCC)
    return scalaCC
  }

  def getConvoyWithConnectedComponents(filterdConvoy: ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[Long])],
                                       friendsFilePath: String, minCovoyUsers: Long): ListBuffer[(List[Long], ListBuffer[Long], ListBuffer[Long])] = {

    val friendslines = scala.io.Source.fromFile(friendsFilePath).getLines().map(t => t.split("\t"))
      .map(t => (t(0).toLong, t(1).toLong)).toList.distinct //Get friends
    var filteredFriends: Map[Long, List[(Long, Long)]] = Map()
    val friends = friendslines.groupBy(t => t._1) //Get each user's friends
    var users: ListBuffer[Long] = new ListBuffer[Long]()
    filterdConvoy.foreach { fc =>
      users ++= fc._1 // Get Vertices: All the users that are involved in convoys called convoyUsers
    }
    users = users.distinct // Filter only unique visitors
    println("unique users are::" + users)
    // populate filteredEdges
    val FilteredEdges: ListBuffer[(Long, Long)] = new ListBuffer[(Long, Long)]()
    filteredFriends = friends.filter(t => users.contains(t._1)).map(t => (t._1, t._2.filter(it => users.contains(it._2))))
    users.foreach { t =>
      var uf = friends.getOrElse(t, null) //.filter(it=> users.contains(it._2)) // only consider those friends that are in convoyUsers
      if (uf != null) {
        uf = uf.filter(it => users.contains(it._2))
        FilteredEdges ++= uf
      }
    }
    val filteredCCConvoys: ListBuffer[(List[Long], ListBuffer[Long], ListBuffer[Long])] = new
        ListBuffer[(List[Long], ListBuffer[Long], ListBuffer[Long])]()
    filterdConvoy.foreach { fc =>
      val users = fc._1.distinct //users
    val cc = getConnectedComponents(users, minCovoyUsers)
      cc.foreach { t =>
        filteredCCConvoys += ((t.toList, fc._2, fc._3)) // add in main clique convoy list
      }
    }

    return filteredCCConvoys
  }

 var convoyCount=0
  def findConvoyStats(convoys: ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[Long])], friendsFile: String): Unit = {
    println("Users groups convoys with their corresponding locations")
    val groupsCount = convoys.groupBy(t => t._1).map(t => (t._1, t._2.size)).toList.sortBy(t => -t._2)
    groupsCount.take(10).foreach(t => println(t))
    println("Locations groups with their corresponding convoys")
    val locsCount = convoys.groupBy(t => t._2).map(t => (t._1, t._2.size)).toList.sortBy(t => -t._2)
    locsCount.take(10).foreach(t => println(t))

    val convoyUserGroup=List[Long](3,4,5)
    convoyUserGroup.foreach{t=>
      val count=convoys.filter(it=> it._1.size>=t).size
      println("Convoys users greater than :"+t+" are::"+count)
    }
    val convoyLocGroup=List[Long](3,4,5)
    convoyLocGroup.foreach{t=>
      val count=convoys.filter(it=> it._2.size>=t).size
      println("Convoys locations greater than :"+t+" are::"+count)
    }


    // total users participate in convoys
    var convoyUsers: ListBuffer[Long] = new ListBuffer[Long]()
    convoys.foreach { c =>
      convoyUsers = convoyUsers.union(c._1)
    }
    println("convoys users::" + convoyUsers.distinct.size)
    println("users::" + convoyUsers.distinct)

    // total locations participate in convoys
    var convoyLocs: ListBuffer[Long] = new ListBuffer[Long]()
    convoys.foreach { c =>
      convoyLocs = convoyLocs.union(c._2)
    }
    println("convoys locations::" + convoyLocs.distinct.size)
    println("locations::" + convoyLocs.distinct)
    // users size
    println("group size")
    val groupSize = convoys.map(t => (t._1, t._2, t._1.size)).sortBy(t => -t._3).take(10)
    groupSize.foreach(t => println(t))
    println("locations size")
    // locations size
    val locSize = convoys.map(t => (t._1, t._2, t._2.size)).sortBy(t => -t._3).take(10)
    locSize.foreach(t => println(t))
    println("Actual convoy size::" + convoys.size)
    /** Clique convoys */
    val convoysWithCliques = getConvoyWithCliques(convoys, friendsFile)
    println("filtered convoy size::" + convoysWithCliques.size)

    if(convoyCount==0){
      convoyCount += 1
      println("Now stats of cliques convoys::")
      findConvoyStats(convoysWithCliques,friendsFile)

    }

  }

  def getConvoyTable(convoysFile: String, friendsFile: String, venuesFile: String, fileConvoyTable:String): Unit = {

    val writerConvoyTable=new PrintWriter(new File(fileConvoyTable))
    var convoys: ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[Long])] = readConvoysFile(convoysFile)
    //convoys.foreach(t=> println(t))
    convoys = convoys.map(t => (t._1.sortBy(t => t), t._2.sortBy(t => t), t._3))

    //findConvoyStats(convoys)
    val fr = new fileReaderLBSN
    val friends = fr.readFriendsFile(friendsFile)
    val locations = fr.readVenuesFileWee(venuesFile)
    val locationMap = locations.map(t => (t.lId, t)).toMap
    /** Convoys */
    var convoysTable: ListBuffer[(Long, Long, Long, String, ListBuffer[Long], ListBuffer[Long],ListBuffer[String])] = new ListBuffer()
    //convoyId, userId, locationId,category, User group, location group
    var convoyId: Long = -1
    convoys.foreach { t =>
      convoyId += 1
      t._1.foreach { u => //user
        t._2.foreach { l => //location
          val loc: Location = locationMap.getOrElse(l, null)
          if (loc != null) {
            if (loc.lCategories.size > 0) {
              loc.lCategories.foreach { cat =>
                convoysTable +=((convoyId, u, l,cat, t._1,t._2,loc.lCategories))
              }
            }
            /*else {
              println("else printed"+loc.printInfo())
              convoysTable +=((convoyId, u, l, "", t._1,t._2,ListBuffer()))
            }*/
          }
        }
      }
    }// test
    convoysTable.groupBy(t=> t._2).map(t=> (t._1,t._2.map(it=> it._1).distinct)).toList
      .sortBy(t=> -t._2.size).map(t=> (t._1, t._2.size, t._2)).take(10)
    //.foreach(println)
    /*val user=3943
    var fTravelCompanions:ListBuffer[Long]=new ListBuffer()
    var temp:ListBuffer[ListBuffer[Long]]=new ListBuffer()
    convoysTable=convoysTable.filter(t=> t._2==user)
    //println("convoy size::"+convoysTable.size)
    convoysTable.map(t=> (t._1,t._5,t._6,t._4)).distinct.groupBy(t=> t._4)
      .map{t=>
      temp=t._2.map(it=> it._2)
      temp.foreach{tem=>
        fTravelCompanions ++= tem
      }
      (t._1,temp)}
    //.foreach(println)
    fTravelCompanions= fTravelCompanions.distinct -= user
    println("travel companion: size, users ::"+fTravelCompanions.size, fTravelCompanions)
    var fUser:ListBuffer[Long]=new ListBuffer()
    friends.filter(t=> t._1==user || t._2==user).foreach{t=>
      fUser += t._1
      fUser += t._2
    }
    fUser=fUser.distinct -= user
    println("friends of users: size, friends::"+fUser.size,fUser)
    */
    writerConvoyTable.println("convoyId"+"\t"+"user"+"\t"+"location"+"\t"+"category"+"\t"+"UGroupSize"+"\t"+"LGroupSize"+"\t"+"Category Group Size"+"\t"+"UGroup"+"\t"+"LGroup"+"\t"+"Categories")
    convoysTable.foreach{t=>
      writerConvoyTable.println(t._1+"\t"+t._2+"\t"+t._3+"\t"+t._4+"\t"+t._5.size+"\t"+t._6.size+"\t"+t._7.size+"\t"+t._5.mkString(",")+"\t"+t._6.mkString(",")+"\t"+t._7.mkString(","))
    }
    writerConvoyTable.close()
    /** Clique convoys */
    //val convoysWithCC = getConvoyWithConnectedComponents(convoys, friendsFile,1)
    //println("filtered convoy size::" + convoysWithCC.size)
  }
  def getPerGroup(convoysFile: String, friendsFile: String, venuesFile: String, fileConvoyTable:String): Unit = {

    val writerConvoyTable=new PrintWriter(new File(fileConvoyTable))
    var convoys: ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[Long])] = readFile(convoysFile)
    //convoys.foreach(t=> println(t))
    convoys = convoys.map(t => (t._1.sortBy(t => t), t._2.sortBy(t => t), t._3))

    //findConvoyStats(convoys)
    val fr = new fileReaderLBSN
    val friends = fr.readFriendsFile(friendsFile)
    val locations = fr.readVenuesFile(venuesFile)
    val locationMap = locations.map(t => (t.lId, t)).toMap

    /** Convoys */
    var convoysTable: ListBuffer[(Long, Long, Long, String, ListBuffer[Long], ListBuffer[Long])] = new ListBuffer()
    //convoyId, userId, locationId,category, User group, location group
    var convoyId: Long = -1
    convoys.foreach { t =>
      convoyId += 1
      t._1.foreach { u => //user
        t._2.foreach { l => //location
          val loc: Location = locationMap.getOrElse(l, null)
          if (loc != null) {
            if (loc.lCategories.size > 0) {
              loc.lCategories.foreach { cat =>
                convoysTable +=((convoyId, u, l,cat, t._1,t._2))
              }
            }
            else {
              println("else printed"+loc.printInfo())
              convoysTable +=((convoyId, u, l, "", t._1,t._2))
            }
          }
        }
      }
    }

    convoysTable.groupBy(t=> t._2).map(t=> (t._1,t._2.map(it=> it._1).distinct)).toList
      .sortBy(t=> -t._2.size).map(t=> (t._1, t._2.size, t._2)).take(10)
      //.foreach(println)

    val user=3943
    var fTravelCompanions:ListBuffer[Long]=new ListBuffer()
    var temp:ListBuffer[ListBuffer[Long]]=new ListBuffer()
    convoysTable=convoysTable.filter(t=> t._2==user)
    //println("convoy size::"+convoysTable.size)
    convoysTable.map(t=> (t._1,t._5,t._6,t._4)).distinct.groupBy(t=> t._4)
      .map{t=>

      temp=t._2.map(it=> it._2)
      temp.foreach{tem=>
        fTravelCompanions ++= tem
      }
      (t._1,temp)}
      //.foreach(println)
    fTravelCompanions= fTravelCompanions.distinct -= user
    println("travel companion: size, users ::"+fTravelCompanions.size, fTravelCompanions)
    var fUser:ListBuffer[Long]=new ListBuffer()
    friends.filter(t=> t._1==user || t._2==user).foreach{t=>
      fUser += t._1
      fUser += t._2
    }
    fUser=fUser.distinct -= user
    println("friends of users: size, friends::"+fUser.size,fUser)

    /*
    writerConvoyTable.println("convoyId"+"\t"+"user"+"\t"+"location"+"\t"+"category"+"\t"+"UGroupSize"+"\t"+"LGroupSize"+"\t"+"UGroup"+"\t"+"LGroup")
    convoysTable.foreach{t=>
      writerConvoyTable.println(t._1+"\t"+t._2+"\t"+t._3+"\t"+t._4+"\t"+t._5.size+"\t"+t._6.size+"\t"+t._5.mkString(",")+"\t"+t._6.mkString(","))
    }*/
    writerConvoyTable.close()


    /** Clique convoys */
    //val convoysWithCC = getConvoyWithConnectedComponents(convoys, friendsFile,1)
    //println("filtered convoy size::" + convoysWithCC.size)


  }

}
