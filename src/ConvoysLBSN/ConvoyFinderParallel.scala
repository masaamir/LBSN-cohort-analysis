package ConvoysLBSN
import java.io.{File, PrintWriter}
import java.util.Date

import FormatData.fileReaderLBSN

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}
//import scala.util.parsing.combinator.Parsers.{Failure, Success}
/**
  * Created by XXX on XXX.
  */
class ConvoyFinderParallel {

  def checkinPartitions(noParts:Int,checkins:List[(Long, Date, Double, Double, String, Long, String)])
  : List[List[(Long, Date, Double, Double, String, Long, String)]] ={
    println("input checkins::"+checkins.size)
    val sizePart=checkins.size/noParts
    var totalCount=0
    val parts=checkins.grouped(sizePart).toList
    var count=0
    parts.foreach{t=>
      count+=1
      totalCount += t.size
      //println("count, size is ::"+count, t.size)
    }
    //println("total count now is ::"+totalCount)

    return parts.toList
  }

  def readPartConvoys(partConvoysFile:String)
  : ListBuffer[((Long,Long), ListBuffer[(ListBuffer[Long],ListBuffer[Long],ListBuffer[Long])])] ={
    val partConvoys=scala.io.Source.fromFile(partConvoysFile).getLines().toList
      .map(t=> t.split("!!!")).filter(t=> t.size>1).map(t=> (t(0).split(","),t(1)))
      .map{t=>
        //val id=t._1
        //val convoyList=
        (((t._1(0)).toLong,(t._1(1)).toLong),t._2.split("\\$\\$\\$").filter(con=> con.size>1).map(it=> it.split("\t")).map(it=>(it(0).split(",")
          .map(iit=> iit.toLong).to[ListBuffer],it(1).split(",")
          .map(iit=> iit.toLong).to[ListBuffer],it(2).split(",").map(iit=> iit.toLong).to[ListBuffer])).to[ListBuffer])
      }.to[ListBuffer]

    println("part convoy size is ::"+partConvoys.size)
    partConvoys.foreach{pc=>
      println(pc)
    }
    return partConvoys
  }

  def updateConvoyList(c:(ListBuffer[Long],ListBuffer[Long],ListBuffer[Long])
                       ,cList:ListBuffer[(ListBuffer[Long],ListBuffer[Long],ListBuffer[Long])])
  : ListBuffer[(ListBuffer[Long],ListBuffer[Long],ListBuffer[Long])] ={
    var newList=cList.clone()
    var bool =true
    var superConvoy=false
    cList.foreach{cl=>
      if(c._1.forall(cl._1.contains) && c._3.forall(cl._3.contains) ){
        bool=false
      }
      else if(c._1.forall(cl._1.contains) && cl._3.forall(c._3.contains) ){
        superConvoy=true
        newList -= cl
      }
    }
    if(bool==true){
      newList += c
    }
    if(superConvoy==true && bool==false){
      newList += c
    }
    return newList
  }

  def filterSubsetConvoys(convoys:ListBuffer[(ListBuffer[Long],ListBuffer[Long],ListBuffer[Long])])
  : ListBuffer[(ListBuffer[Long],ListBuffer[Long],ListBuffer[Long])] ={
    // val result=convoys.filter(c=> !convoys.view.filter(_!=c).exists(_.contains(c)))
    val newConvoys=convoys.clone()
    for (i<-0 until convoys.size){
      var bool =true
      for(j<-0 until convoys.size){
        if(i!=j){
          if(convoys(i)._1.forall(convoys(j)._1.contains) && convoys(i)._2.forall(convoys(j)._2.contains)
            &&convoys(i)._3.forall(convoys(j)._3.contains) ){
            newConvoys -= convoys(i)
          }
        }
      }
    }

    return newConvoys
  }

  def mergePartConvoys(partConvoysFile:String, writeFile:String): Unit ={
    val writer=new PrintWriter(new File(writeFile))
    val partConvoys=readPartConvoys(partConvoysFile).sortBy(t=> t._1._1)
    var permConvoys= new ListBuffer[(ListBuffer[Long],ListBuffer[Long],ListBuffer[Long])]()
    var onGoingConvoys= new ListBuffer[(ListBuffer[Long],ListBuffer[Long],ListBuffer[Long])]()
    var pNextConvoys=new ListBuffer[(ListBuffer[Long],ListBuffer[Long],ListBuffer[Long])]()
    var closedConvoys=new ListBuffer[(ListBuffer[Long],ListBuffer[Long],ListBuffer[Long])]()
    for(i<-1 until partConvoys.size) {
      val prev = partConvoys(i - 1)
      val current = partConvoys(i)
      pNextConvoys = new ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[Long])]()

      // add closed convoys to the permanent convoys for any case
      closedConvoys = prev._2.filter(t => (t._3.min > 0) && t._3.max < prev._1._2)
      permConvoys ++= closedConvoys

      //start or previous was null
      if (i == 1 || prev._1._1 != (partConvoys(i - 2)._1._1 + 1)) {
        permConvoys ++= onGoingConvoys
        onGoingConvoys = new ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[Long])]()
        val leftOpen = prev._2.filter(t => t._3.min == 0)
        leftOpen.foreach { lo =>
          if (lo._3.max < prev._1._2) {
            permConvoys += lo
          } else {
            onGoingConvoys += lo
          }
        }
      }

      //add right open that start in middle to ongoing convoys
      val rightOpen = prev._2.filter(t => t._3.min > 0 && t._3.max == prev._1._2)
      onGoingConvoys ++= rightOpen

      if(current._1._2==(prev._1._1+1)){

      //right open in next convoys
      val rOpenNext = current._2.filter(t => t._3.min == 0)

      // main logic of comparisons
      onGoingConvoys.foreach { oc =>
        //var absorbed = false
        //var intersection=false
        rOpenNext.foreach { ron =>
          if (oc._1.forall(ron._1.contains)) {
            //absorbed = true
            val pNew = (oc._1, oc._2.union(ron._2), oc._3.union(ron._3))
            pNextConvoys = updateConvoyList(pNew, pNextConvoys)
            pNextConvoys = updateConvoyList(ron, pNextConvoys)
          }
          // intersection
          else if (oc._1.intersect(ron._1).size >= 2) {
            val pNew = (oc._1.intersect(ron._1), oc._2.union(ron._2), oc._3.union(ron._3))
            pNextConvoys = updateConvoyList(pNew, pNextConvoys)
            pNextConvoys = updateConvoyList(ron, pNextConvoys)
            pNextConvoys = updateConvoyList(oc, pNextConvoys)
          }
        }
      }
      onGoingConvoys = new ListBuffer[(ListBuffer[Long], ListBuffer[Long], ListBuffer[Long])]()
      pNextConvoys.foreach { c =>
        if (c._3.max == current._1._2) {
          onGoingConvoys += c
        }
        else {
          permConvoys += c
        }
      }
    }

      if(i==partConvoys.size-1){
        permConvoys ++= onGoingConvoys
      }


      //onGoing ++= prev._2
      /*if(current._1._2==(prev._1._1+1)){
        val prevOpenConvoys=prev._2.filter(t=> t._3.max==prev._1._2)
      }else{
        //insert ongoing to permanent
        //no convoys to merge//
        //insert all convoys to permanent one
      }*/
    }
    //permConvoys=filterSubsetConvoys(permConvoys) wrong function
    //permConvoys.foreach(println)
    permConvoys.foreach{pc=>
      writer.println(pc._1.mkString(",")+"\t"+pc._2.mkString(",")+"\t"+pc._3.mkString(","))

    }
    writer.close()
    println(" permanent convoy size is ::"+permConvoys.size)
  }

  def evaluateConvoyBasedTravelerGroup(trainFile:String,testFile:String,inputCatsFile:String): Unit = {
    val fr = new fileReaderLBSN
    val inputCatsList = scala.io.Source.fromFile(inputCatsFile).getLines().toList.map(t => t.split(",").toList).to[ListBuffer]
    val trainConvoys = fr.readConvoyCatsFile(trainFile)
    val testConvoys = fr.readConvoyCatsFile(testFile)

    val kList = List(1, 3, 5)

    kList.foreach { k =>
      println(" ****************k is ::"+k)
      var overallRatio=0.0
    inputCatsList.foreach { cl =>
      println("categories are ::" + cl)
      val predictedConvoys = trainConvoys.filter(t => cl.forall(t._3.contains))
        .groupBy(t => t._1).toList.sortBy(t => -t._2.size).take(k)
      //println("predicted convoys are")
      //predictedConvoys.foreach(println)
      var correctCount = 0
      predictedConvoys.foreach { pc =>
        val correctConvoys = testConvoys.filter(t => pc._1.forall(t._1.contains) && cl.forall(t._3.contains))
        if (correctConvoys.size > 0) {
          //println("correct convoys are ::")
          //correctConvoys.foreach(println)
          correctCount += 1
        }
      }
      println("correct are " + correctCount + " out of " + k  )
      overallRatio += (correctCount.toDouble / k.toDouble)
    }
      println("correct ratio for k is ::"+(overallRatio/inputCatsList.size.toDouble))

  }




  }

  def evaluateConvoyBasedTravelerGroupAllCats(trainFile:String,testFile:String): Unit = {
    val fr = new fileReaderLBSN
    //val inputCatsList = scala.io.Source.fromFile(inputCatsFile).getLines().toList.map(t => t.split(",").toList).to[ListBuffer]
    val trainConvoys = fr.readConvoyCatsFile(trainFile)
    val testConvoys = fr.readConvoyCatsFile(testFile)

    val kList = List(1, 3, 5)

    kList.foreach { k =>
      println(" ****************k is ::"+k)
      var overallRatio=0.0
      //inputCatsList.foreach { cl =>
        //println("categories are ::" + cl)
        val predictedConvoys = trainConvoys//.filter(t => cl.forall(t._3.contains))
          .groupBy(t => t._1).toList.sortBy(t => -t._2.size).take(k)
        //println("predicted convoys are")
        //predictedConvoys.foreach(println)
        var correctCount = 0
        predictedConvoys.foreach { pc =>
          val correctConvoys = testConvoys.filter(t => pc._1.forall(t._1.contains))
          if (correctConvoys.size > 0) {
            //println("correct convoys are ::")
            //correctConvoys.foreach(println)
            correctCount += 1
          }
        }
        println("correct are " + correctCount + " out of " + k  )
        overallRatio += (correctCount.toDouble / k.toDouble)
      //}
      println("correct ratio for k is ::"+(overallRatio))

    }




  }

  def divideActivities(checkinFile:String, writeFile:String): Unit ={
    val writer=new PrintWriter(new File(writeFile))
    val fr=new fileReaderLBSN
    val noPats=50
    val checkins=fr.readCheckinFile(checkinFile).sortBy(t=> t._2)
    println("checkins are read::"+checkins.size)
    val parts=checkinPartitions(noPats,checkins)
    /*parts.take(10).foreach{p=>
      val convoy = new ConvoyAnalysisFromCheckins
      val retValue=convoy.getConvoys(p,60)
      //println(retValue)
    }*/
    /*
    val partConvoys:Seq[Future[ListBuffer[(ListBuffer[Long],ListBuffer[Long],ListBuffer[Long])]]]=
      for (i<-0 until parts.size) yield Future{
        println("Inside loop")
      val convoy = new ConvoyAnalysisFromCheckins
      val retValue=convoy.getConvoys(parts(i),60)
        println("retValue is ::"+retValue.toString())
        retValue
      //1*1
    }
    println("returned size is ::"+partConvoys.size)
    partConvoys.foreach{t=>
      if(t.isCompleted){
        println("completed")
      }
      else{
        println("no not yet")
      }
      t.onComplete{it=>
        println("In complete ")
        if(it.isSuccess){
          println("success")
        }else {
          println("it failed")
        }
        println("Convoy for this are::")
        println(it.get)

      }

    }*/
      val listFuture:Seq[Future[((Int,Long),ListBuffer[Convoy])]]=for (i<-0 until parts.size) yield Future {
        println("future "+i)
        val convoy = new ConvoyAnalysisFromCheckins
        val retValue=convoy.getConvoys(i,parts(i),60)
        retValue
        }
    var users:ListBuffer[Long]=new ListBuffer[Long]()
    var locations:ListBuffer[Long]=new ListBuffer[Long]()
    var timeStamps:ListBuffer[Long]=new ListBuffer[Long]()
    var failuresCount=0
    var returnedValues:ListBuffer[((Int,Long),ListBuffer[Convoy])]=new ListBuffer[((Int,Long), ListBuffer[Convoy])]()
    var countFuture=0
    var retCount=0
    listFuture.foreach{t=>
      Await.ready(t,Duration.Inf)
      countFuture +=1
      println("Future Count::"+countFuture)
      t.onComplete{
        case Success(value) => {
          returnedValues += (value)
          retCount+=value._2.size
          println("So far convoy count is::"+retCount)
          /*writer.print(value._1+"***")
          println("number of convoys are::"+value._2.size)
          value._2.foreach{it=>
            users=it.getUsers()
            locations=it.getLocations()
            timeStamps=it.getTimeStamps()
            //users.mkString(",")
            writer.print(users.mkString(",")+"\t"+locations.mkString(",")+"\t"+timeStamps.mkString(","))
            writer.print("$$$")
          }
          writer.println()
          println(value)*/
        }
        case Failure(e)=> {
          failuresCount +=1
          println("Failed")
          e.printStackTrace()
        }
      }
    }
    /**write file*/
    var idTime:(Int,Long)=null
    returnedValues.foreach{t=>
      idTime=t._1
      //val int=idTime._1
      //val long=idTime._2
      writer.print(idTime._1+","+idTime._2+"!!!")
      t._2.foreach{it=>
        writer.print(it.getUsers().mkString(",")+"\t"+it.getLocations().mkString(",")+"\t"+it.getTimeStamps().mkString(","))
        writer.print("$$$")
      }
      writer.println()
    }
    println("returned values size is ::"+returnedValues.size)
    println("Failure count is ::"+failuresCount)
    writer.close()

  }

}
