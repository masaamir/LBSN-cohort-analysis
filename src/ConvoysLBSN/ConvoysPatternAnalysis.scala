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

  var filteredFriends: Map[Long, List[(Long, Long)]] = Map()

  def sortFriends(inFriendships:ListBuffer[(Long,Long)]): ListBuffer[(Long,Long)] ={
    val newFriendships=inFriendships.map{t=>
      if(t._1>t._2) (t._2,t._1) else t
    }
    return newFriendships.distinct
  }

  def getFriendsForUsers(inUsers:ListBuffer[(Long)],inFriendships:ListBuffer[(Long,Long)]): Map[Long,ListBuffer[Long]] ={
    // No assumption// assumption is: edges are duplicated such that for each user all his friends are present from source - dest id
    val friends=sortFriends(inFriendships)

    val newFriends=inUsers.map{t=>
      val localFriends:ListBuffer[Long]=new ListBuffer[Long]()
      inFriendships.filter(it=> it._1==t || it._2==t).foreach{tup=>
        if(tup._1==t) localFriends+= tup._2 else localFriends+=tup._1
      }
      (t,localFriends.distinct)
    }

    return newFriends.toMap
  }

  def getAttributesFromConvoys(visitor:ListBuffer[Long], pConvoy:ListBuffer[(ListBuffer[(Long)],ListBuffer[(Long)],ListBuffer[String])])
  : (ListBuffer[Long],ListBuffer[Long],ListBuffer[String]) ={
    var companions:ListBuffer[Long]=new ListBuffer()
    var locations:ListBuffer[Long]=new ListBuffer()
    var categories:ListBuffer[String]=new ListBuffer()
    //var companCat:ListBuffer[String]=new ListBuffer()
    //var compCatMap:ListBuffer[(Long,ListBuffer[String])]=new ListBuffer()
    //println("Total convoys are::"+pConvoy)
    pConvoy.foreach{it=>
      companions ++= it._1
      locations ++= it._2
      categories ++= it._3
    }
    //println("before ::"+companions)
    companions= companions.distinct -- visitor
    //println("after"+companions)
    (companions.distinct,locations.distinct,categories.distinct)
  }

  def evaluateCategoryAffect5(fileConvoyTable:String): Unit ={ // user, companions, categories, locs
  val covoyTable=scala.io.Source.fromFile(fileConvoyTable).getLines().drop(1).toList
      .map(t=> t.split("\t")).map(t=> (t(0).toLong,t(1).toLong,t(2).toLong,t(3),t(4).toLong,t(5).toLong,t(6).toLong,t(7),t(8),t(9)))
      //convoyId,user,location,category,UGroupSize,LGroupSize,CategoryGroupSize,UGroup,LGroup,Categories
      .groupBy(t=> t._2).map(t=> (t._1, t._2.map(it=> (it._8,it._9,it._10)).distinct)).toList//group by user : {user - {convoy table tuple with user}}
      .map(t=> (t._1,t._2.map(it=> (it._1.split(",").map(iit=> iit.toLong).to[ListBuffer],it._2.split(",").map(iit=> iit.toLong).to[ListBuffer],it._3.split(",").to[ListBuffer])).to[ListBuffer]))
      .filter(t=> t._1==14812)
      .foreach{t=>
        val cats:ListBuffer[String]=new ListBuffer()
        t._2.foreach{it=>
          if(it._1.contains(10514))
            cats ++= it._3
        }
        println("Contained Categories are ::"+cats.distinct.mkString(","))
      }


  }

  def getCategoryCountInConvoys(cat:ListBuffer[String], pConvoy:ListBuffer[(ListBuffer[(Long)],ListBuffer[(Long)],ListBuffer[String])])
  : ListBuffer[(String,Int)] ={
    val convoys=pConvoy.distinct
    val catCountPair=cat.map{t=>

      val con=convoys.filter{it=>
        it._3.contains(t)}
      println("category::"+t)
      println("convoy::"+con)
      (t, con.size)
    }

    return catCountPair
  }

  def evaluateCategoryAffect4(fileConvoyTable:String, friendsFile:String): Unit ={ // user, companions, categories, locs
  val fr=new fileReaderLBSN
    val friendships=fr.readFriendsFile(friendsFile)
    val covoyTable=scala.io.Source.fromFile(fileConvoyTable).getLines().drop(1).toList
      .map(t=> t.split("\t")).map(t=> (t(0).toLong,t(1).toLong,t(2).toLong,t(3),t(4).toLong,t(5).toLong,t(6).toLong,t(7),t(8),t(9)))
      //convoyId,user,location,category,UGroupSize,LGroupSize,CategoryGroupSize,UGroup,LGroup,Categories
      .groupBy(t=> t._2).map(t=> (t._1, t._2.map(it=> (it._8,it._9,it._10)).distinct)).toList//group by user : {user - {convoy table tuple with user}}
      .map(t=> (t._1,t._2.map(it=> (it._1.split(",").map(iit=> iit.toLong).to[ListBuffer],it._2.split(",").map(iit=> iit.toLong).to[ListBuffer],it._3.split(",").to[ListBuffer])).to[ListBuffer]))
      .sortBy(t=> -t._2.size).take(100)
        //.filter(t=> t._1==1777)
      .map{t=>
        val cVisitor=t._1
        val userConvoys= t._2.distinct
        val minusVisitors=ListBuffer(cVisitor)
        //println("Minus visitor is ::"+minusVisitors)
        val convoyAttribs=getAttributesFromConvoys(minusVisitors,userConvoys)
        val cCampanions:ListBuffer[ConvoyCompanion]=new ListBuffer()


        val comps=convoyAttribs._1
        comps.foreach{c=>
          val filConvoys=userConvoys.filter(cf=> cf._1.contains(c)).distinct
          val innerConvoyAttrib=getAttributesFromConvoys(ListBuffer(cVisitor,c),filConvoys)
          cCampanions += new ConvoyCompanion(c,filConvoys.size.toLong,innerConvoyAttrib._1,innerConvoyAttrib._2,innerConvoyAttrib._3)
        }
        (cVisitor,cCampanions)
        //new ConvoyVisitor(cVisitor,userConvoys.size.toLong,cCampanions,convoyAttribs._2,convoyAttribs._3)
        new ConvoyVisitor(cVisitor,userConvoys.size.toLong,cCampanions,convoyAttribs._2,convoyAttribs._3,userConvoys)
 /*       var locations:ListBuffer[Long]=new ListBuffer()
        var categories:ListBuffer[String]=new ListBuffer()
        var companCat:ListBuffer[String]=new ListBuffer()
        var compCatMap:ListBuffer[(Long,ListBuffer[String])]=new ListBuffer()
        t._2.foreach{it=>
          companions ++= it._1
          locations ++= it._2
          categories ++= it._3
        }
        companions= companions.distinct
        locations=locations.distinct
        categories=categories.distinct
*/
        /*
        //val userConvoys= t._2.distinct
        companions.foreach{c=>
          //companCat._1 += c
          val filtConvoys=userConvoys.filter(cf=> cf._1.contains(c)) // consider only those convoys which have this user
          categories.foreach{ct=>
            val catConvoys=filtConvoys.filter(ffc=> ffc._3.contains(ct)) // consider only those convoys which have this category
            if(catConvoys.size>0){
              companCat += ct
            }
          }
          compCatMap += ((c,companCat.distinct))
        }
        println()
        println()
        println("for user ::"+cVisitor)
        println("All categories are ::"+categories.mkString(","))
        println("All companions are ::"+companions.mkString(","))
        compCatMap.foreach{cM=>
          if(categories.diff(cM._2).size>0) {
            println("with user***" + cM._1)
            println("On categories***" + cM._2.mkString(","))
            println("Not in !!!!!!" + categories.diff(cM._2))
          }
        }

        */
      }
      .foreach{v=>
        println()
        println()
        println("For users ::"+v.vId)
        /**only companion friends*/
        //val friendshipMap=getFriendsForUsers(ListBuffer(v.vId),friendships.to[ListBuffer])
        //val userFriends=friendshipMap.getOrElse(v.vId,ListBuffer())
        //val companions=v.companions.map(t=> t.vId).distinct.filter(t=> userFriends.contains(t))
        //println("No of friends/ no. of companions::"+userFriends.size+"/"+companions.size)
        println("All categories are ::"+v.onCategories  )
        v.companions.sortBy(t=> -t.convoyCount).take(10)
            //.filter(t=> t.vId==2511  )
          .foreach{c=>
          //val totalCats=c.onCategories
          if(v.onCategories.diff(c.onCategories).size>0) {
            println()
            println("with user::"+c.vId)
            println("Convoys: total /current users::"+v.convoyCount+"/"+c.convoyCount)
            println("With categories::" + c.onCategories)
            println("Not on categories with !!!!!!!!" + v.onCategories.diff(c.onCategories))
            println("size is ::"+getCategoryCountInConvoys(v.onCategories.diff(c.onCategories),v.convoys).size)
            println()
            getCategoryCountInConvoys(v.onCategories.diff(c.onCategories),v.convoys).foreach{t=>
              println(t)
            }
          }else {println("This is max matched")
            println("with user::"+c.vId)
            println("With categories::" + c.onCategories)
            println("Not on categories with !!!!!!!!" + v.onCategories.diff(c.onCategories))

            //println("Not on categories with !!!!!!!!" + getCategoryCountInConvoys(v.onCategories.diff(c.onCategories),v.convoys))
          }
        }

      }


  }

  def evaluateCategoryAffect3(fileConvoyTable:String): Unit ={ // user, companions, categories, locs
  val covoyTable=scala.io.Source.fromFile(fileConvoyTable).getLines().drop(1).toList
      .map(t=> t.split("\t")).map(t=> (t(0).toLong,t(1).toLong,t(2).toLong,t(3),t(4).toLong,t(5).toLong,t(6).toLong,t(7),t(8),t(9)))
      //convoyId,user,location,category,UGroupSize,LGroupSize,CategoryGroupSize,UGroup,LGroup,Categories
      .groupBy(t=> t._2).map(t=> (t._1, t._2.map(it=> (it._8,it._9,it._10)).distinct)).toList//group by user : {user - {convoy table tuple with user}}
      .map(t=> (t._1,t._2.map(it=> (it._1.split(",").map(iit=> iit.toLong).toList,it._2.split(",").map(iit=> iit.toLong).toList,it._3.split(",").toList))))
      .sortBy(t=> -t._2.size).take(100)
      .foreach{t=>
        val cVisitor=t._1
        var companions:ListBuffer[Long]=new ListBuffer()
        var locations:ListBuffer[Long]=new ListBuffer()
        var categories:ListBuffer[String]=new ListBuffer()
        var companCat:ListBuffer[String]=new ListBuffer()
        var compCatMap:ListBuffer[(Long,ListBuffer[String])]=new ListBuffer()
        t._2.foreach{it=>
          companions ++= it._1
          locations ++= it._2
          categories ++= it._3
        }
        companions= companions.distinct
        locations=locations.distinct
        categories=categories.distinct

        val userConvoys= t._2.distinct
        companions.foreach{c=>
          //companCat._1 += c
          val filtConvoys=userConvoys.filter(cf=> cf._1.contains(c)) // consider only those convoys which have this user
          categories.foreach{ct=>
            val catConvoys=filtConvoys.filter(ffc=> ffc._3.contains(ct)) // consider only those convoys which have this category
            if(catConvoys.size>0){
              companCat += ct
            }
          }
          compCatMap += ((c,companCat.distinct))
        }
        println()
        println()
        println("for user ::"+cVisitor)
        println("All categories are ::"+categories.mkString(","))
        println("All companions are ::"+companions.mkString(","))
        compCatMap.foreach{cM=>
          if(categories.diff(cM._2).size>0) {
            println("with user***" + cM._1)
            println("On categories***" + cM._2.mkString(","))
            println("Not in !!!!!!" + categories.diff(cM._2))
          }
        }
      }


  }

  def evaluateCategoryAffect2(fileConvoyTable:String): Unit ={ // user, companions, categories, locs
    val covoyTable=scala.io.Source.fromFile(fileConvoyTable).getLines().drop(1).toList
      .map(t=> t.split("\t")).map(t=> (t(0).toLong,t(1).toLong,t(2).toLong,t(3),t(4).toLong,t(5).toLong,t(6).toLong,t(7),t(8),t(9)))
      //convoyId,user,location,category,UGroupSize,LGroupSize,CategoryGroupSize,UGroup,LGroup,Categories
      .groupBy(t=> t._2).map(t=> (t._1, t._2.map(it=> (it._8,it._9,it._10)).distinct)).toList//group by user : {user - {convoy table tuple with user}}
      .sortBy(t=> -t._2.size).take(10)
      .map{t=>
        (t._1,
          t._2.groupBy(it=> it._3).toList.distinct)
      }
        .map{t=>
          val newInnerTuple=t._2.map{it=>

            val UUsers:ListBuffer[String]=new ListBuffer()
            val ULocs:ListBuffer[String]=new ListBuffer()
            val UCategories:ListBuffer[String]=new ListBuffer()
            it._2.map{iit=>
              UUsers ++= iit._1.split(",").to[ListBuffer]
              ULocs ++= iit._2.split(",").to[ListBuffer]
              UCategories ++= iit._3.split(",").to[ListBuffer]
            }

            (it._1,UUsers.distinct,ULocs.distinct,UCategories.distinct)
          }

          (t._1,newInnerTuple)
        }
      .foreach{t=>
        println()
        println()
        println("For user:: -------------------- "+t._1)
        t._2.foreach{it=>
          println("Category :: **************** "+ it._1)
          println("Users::"+it._2.mkString(","))
          println("Locations Size::"+it._3.size)
          //println("Categories::"+it._4)
        }
      }

  }

  def evaluateCategoryAffect(fileConvoyTable:String): Unit = {
    val covoyTable = scala.io.Source.fromFile(fileConvoyTable).getLines().drop(1).toList
      .map(t => t.split("\t")).map(t => (t(0).toLong, t(1).toLong, t(2).toLong, t(3), t(4).toLong, t(5).toLong, t(6).toLong, t(7), t(8), t(9)))
      //convoyId,user,location,category,UGroupSize,LGroupSize,CategoryGroupSize,UGroup,LGroup,Categories
      .groupBy(t => t._1).toList //group by user : {user - {convoy table tuple with user}}
      .sortBy(t => -t._2.size).take(10)
      .map { t =>
        (t._1,
          t._2.groupBy(it => it._10).toList.distinct)
      }
      .foreach { t =>
        println("For user:: -------------------- " + t._1)
        t._2.foreach { it =>
          println("Category :: **************** " + it._1)
          it._2.map(innert => (innert._8, innert._9)).distinct.
            foreach { iit => println("users:: " + iit._1)
              println(" locs::" + iit._2)
            }
        }
      }
  }


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
    convoys = convoys.map(t => (t._1.sortBy(t => t), t._2.sortBy(t => t), t._3)) // sorted users, locations and TS in convoys

    //findConvoyStats(convoys)
    val fr = new fileReaderLBSN
    val friends = fr.readFriendsFile(friendsFile)
    val locations = fr.readVenuesFileWee(venuesFile)
    println("Venues with categories size::"+locations.size)
    val locationMap = locations.map(t => (t.lId, t)).toMap
    /** Convoys */
    var convoysTable: ListBuffer[(Long, Long, Long, String, ListBuffer[Long], ListBuffer[Long],ListBuffer[String])]
    = new ListBuffer()
    //convoyId, userId, locationId,category, User group, location group, Category group
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
    }
    //convoysTable.groupBy(t=> t._2).map(t=> (t._1,t._2.map(it=> it._1).distinct)).toList
      //.sortBy(t=> -t._2.size).map(t=> (t._1, t._2.size, t._2)).take(10)
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
