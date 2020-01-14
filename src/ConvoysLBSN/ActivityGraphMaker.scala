package ConvoysLBSN

import java.io.{File, PrintWriter}

import scala.collection.mutable.ListBuffer

/**
  * Created by XXX on 21/12/16.
  */
class ActivityGraphMaker {

  def createActivityGraph(groupActsFile:String,edgesFile:String): Unit ={
    val edgesWriter=new PrintWriter(new File(edgesFile))
    val groupActs=scala.io.Source.fromFile(groupActsFile).getLines()
        .map(t=> t.split("\t")).map(t=> (t(0).split(","),t(1),t(2).split(","),t(3).split(",")))
        .map(t=> (t._1.map(it=> it.toLong).to[ListBuffer],t._2.toLong,t._3.to[ListBuffer],t._4.map(it=> it.toLong).to[ListBuffer]))
      //.filter(t=> t._4.size <1)
    var pairActs= new ListBuffer[(ListBuffer[Long],Long,ListBuffer[String],ListBuffer[Long])]()
    groupActs.toList.map{t=>
      if(t._1.size==2) {
        //println("here")
        pairActs += t // if just one pair
      }
      else if (t._1.size > 2){ // if group is bigger than 2 break group into pairs one sided
        var i,j=0
        for (i<-0 until t._1.size){
          for(j<- i+1 until t._1.size){
            //println("inserting ::+",(ListBuffer(t._1(i),t._1(j)),t._2,t._3,t._4))
            pairActs += ((ListBuffer(t._1(i),t._1(j)),t._2,t._3,t._4))
          }
        }
      }
    }
    println("pair Act size::"+pairActs.size)
    var pairActsDouble=new ListBuffer[(ListBuffer[Long],Long,ListBuffer[String],ListBuffer[Long])]()
    pairActsDouble ++= pairActs
    pairActs.foreach{t=>
      pairActsDouble += ((ListBuffer(t._1(1),t._1(0)),t._2,t._3,t._4))
    }
    println("pair double size::"+pairActsDouble.size)
    val edges=pairActsDouble.groupBy(t=> t._1)
      .map(t=> (t._1,t._2.map(it=> (it._2,it._3,it._4))))
        .toList
    val maxCommonActs=edges.maxBy(t=> t._2.size)._2.size
    println("max is ::"+maxCommonActs)
    val edgesWithWeight=edges.map{t=>
      (t._1, t._2.size.toDouble/maxCommonActs,t._2)
    }
    //write file
    edgesWithWeight.foreach{t=> //.filter(t=> t._3.size==2).take(1)
      edgesWriter.print(t._1.mkString(",")+"\t"+t._2+"\t")
        t._3.foreach(it=> edgesWriter.print(it._1+":"+it._2.mkString(",")+":"+it._3.mkString(",")+":"))
      edgesWriter.println()
    }
    edgesWriter.close()
    //edgesWithWeight.take(1).foreach(t=> println(t._1,t._2,t._3.size))
    //println("edges"+edges)

      //.sortBy(t=> -t._2.size).head._2.size
    //println("max edges::"+maxScore)
      //.foreach(t=> )
      //.filter(t=> t._1.size>2)
      //.take(10).foreach(println)
  }

}
