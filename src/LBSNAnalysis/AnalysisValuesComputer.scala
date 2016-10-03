package LBSNAnalysis

import scala.collection.mutable.ListBuffer

/**
 * Created by MAamir on 08-04-2016.
 */
class AnalysisValuesComputer {
  def parseFile(CCConvoysfilePath: String, CCValuesFilePath: String): List[(Array[Long], Array[Long], Array[Long], Array[String])] = {
    val convoyFileActual = scala.io.Source.fromFile(CCConvoysfilePath).getLines().toList
    val convoyFileWithOutHeaders = scala.io.Source.fromFile(CCConvoysfilePath).getLines().slice(1, convoyFileActual.length)
    val convoyFile = convoyFileWithOutHeaders.map(t => t.split("\t"))
      .map(t => (t(0).split(",").map(t => t.toLong), t(1).split(",").map(t => t.toLong), t(2).split(",").map(t => t.toLong), t(3).split(","))).toList
    return convoyFile
  }

  def compress[T](values: List[T]): List[T] =
    compressTail(Nil, values)

  def compressTail[T](seen: List[T], remaining: List[T]): List[T] =
    remaining match {
      case Nil => seen
      case x :: y :: xs if (x == y) => compressTail(seen, y :: xs)
      case x :: xs => compressTail(seen ::: List(x), xs)
    }

  def getAnalysisValues(convoyFile: String, ConvoyValueFiles: String): Unit = {
    val convoyFileLines: List[(Array[Long], Array[Long], Array[Long], Array[String])] = parseFile(convoyFile, ConvoyValueFiles)
    /** Filter unique sequence locations */
    //val convoyWithSeqLines=convoyFileLines.map(t=> (t._1,t._2,compress(t._3.toList),t._3,t._4))
    val convoyWithSeqLines = convoyFileLines.foreach { t =>
      if (t._2.size != compress(t._3.toList).size)
        println(t._1.toList, t._2.toList, compress(t._3.toList).toList, t._3.toList.size)
    }

    /** Convoys received per location */
    /*val convoysRecievedPerLoc=scala.collection.mutable.Map[Long,Long]()
    convoyFileLines.foreach{c=>
      for(i<-1 until c._2.size){
        val value=convoysRecievedPerLoc.getOrElse(c._2(i),0L)
        convoysRecievedPerLoc += (c._2(i)-> (value +1))
      }
    }
    println("Convoys per locations")
    convoysRecievedPerLoc.foreach(t=> println(t))

    /**Convoys send per location*/
    val convoysSentPerLoc=scala.collection.mutable.Map[Long,Long]()
    convoyFileLines.foreach{c=>
      for(i<-0 until c._2.size-1){
        val value=convoysSentPerLoc.getOrElse(c._2(i),0L)
        convoysSentPerLoc += (c._2(i)-> (value +1))
      }
    }
    println("Convoys sent per locations")
    convoysSentPerLoc.foreach(t=> println(t))*/

    /** Users Participated in Convoys */
    val uniqueConvoyUsers: ListBuffer[Long] = new ListBuffer()
    val uniqueConvoyLocs: ListBuffer[Long] = new ListBuffer()
    convoyFileLines.foreach { c =>
      uniqueConvoyUsers ++= c._1.toList
      uniqueConvoyLocs ++= c._2.toList
    }
    println("total Users,Locs::" + uniqueConvoyUsers.size, uniqueConvoyLocs.size)
    println("Unique Users,Locs::" + uniqueConvoyUsers.distinct.size, uniqueConvoyLocs.distinct.size)



    //println("now locs are ::"+locs)


    /** Find number of convoys received by a location */
    /*val convoysRecivedPerLoc=Map[Long,Long]()
    convoyFileLines.foreach{c=>
      for(i<-0 until c.)
    }*/


  }


}
