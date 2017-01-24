package ConvoysLBSN

import java.io.{File, PrintWriter}

import scala.collection.mutable.ListBuffer
import FormatData.fileReaderLBSN

/**
  * Created by aamir on 05/01/17.
  */
class ConvoyStatsFinder {

  def findPopularGroupLocPair(convoysCatsFile:String,k:Int)
  : ListBuffer[(ListBuffer[Long],ListBuffer[Long],ListBuffer[String])] ={
    val fr=new fileReaderLBSN
    val convoyCats=fr.readConvoyCatsFile(convoysCatsFile)
    val groupLocsPair=convoyCats.groupBy(t=> (t._1,t._2,t._3)).toList
      .sortBy(t=> -t._2.size)
    val results:ListBuffer[(ListBuffer[Long],ListBuffer[Long],ListBuffer[String])]=groupLocsPair.map(t=> (t._1)).distinct.to[ListBuffer]
    //results.take(10).foreach(t=> println(t))
    return results.take(k)
    /*groupLocsPair.take(k).foreach{t=>
      println(t._1,t._2.size)
    }*/
  }

  def evaluatePredictedGroup(inPredictedGroup:ListBuffer[Long],inCats:List[String],convoysCatsFile:String): Boolean ={
    val fr=new fileReaderLBSN
    val convoyCats=fr.readConvoyCatsFile(convoysCatsFile)//readConvoyCatsFile(convoysCatsFile)
    val newGroup:ListBuffer[(ListBuffer[Long],ListBuffer[String])]=new ListBuffer[(ListBuffer[Long], ListBuffer[String])]()
    //println("Predicted Group is::"+inPredictedGroup)
    convoyCats.foreach{t=>
      if(inPredictedGroup.forall(t._1.contains)){
        //println("mapped")
        newGroup+=((t._1,t._3.intersect(inCats)))
        println("intersected categories ::"+t._3.intersect(inCats))
        //newGroup+=((t._1,ListBuffer[String]()))
      }
    }
    println("number of time this group appears in test data set is::"+newGroup.size)

    if(newGroup.size >0) return true else return false

  }



}
