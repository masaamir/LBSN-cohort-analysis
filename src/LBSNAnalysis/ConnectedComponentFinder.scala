package LBSNAnalysis

import org.jgrapht.alg.ConnectivityInspector
import org.jgrapht.graph.{DefaultEdge, SimpleGraph}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer


/**
 * Created by MAamir on 07-04-2016.
 */
class ConnectedComponentFinder {

  var filteredFriends: Map[Long, List[(Long, Long)]] = Map()

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

  def getConvoyWithConnectedComponents(filterdConvoy: ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[(Double, Double)])],
                                       friendsFilePath: String, minCovoyUsers: Long): ListBuffer[(List[Long], ListBuffer[Long], ListBuffer[(Double, Double)])] = {
    val friendslines = scala.io.Source.fromFile(friendsFilePath).getLines().map(t => t.split("\t"))
      .map(t => (t(0).toLong, t(1).toLong)).toList.distinct //Get friends
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
    val filteredCCConvoys: ListBuffer[(List[Long], ListBuffer[Long], ListBuffer[(Double, Double)])] = new
        ListBuffer[(List[Long], ListBuffer[Long], ListBuffer[(Double, Double)])]()
    filterdConvoy.foreach { fc =>
      val users = fc._1.distinct //users
    val cc = getConnectedComponents(users, minCovoyUsers)
      cc.foreach { t =>
        filteredCCConvoys += ((t.toList, fc._2, fc._3)) // add in main clique convoy list
      }
    }
    return filteredCCConvoys
  }

  /*
      val graph:SimpleGraph[Long, DefaultEdge]  = new SimpleGraph[Long, DefaultEdge](classOf[DefaultEdge])
      graph.addVertex(10L)
      graph.addVertex(11L)
      graph.addVertex(9L)
      graph.addVertex(12L)
      graph.addEdge(10L,11L)
      graph.addEdge(10L,12L)
      val cI:ConnectivityInspector[Long,DefaultEdge] = new ConnectivityInspector(graph)
      val cc=cI.connectedSets()
      cc.foreach(t=> println(t))*/
}
