package ConvoysLBSN

import java.io.{File, PrintWriter}
import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

import FormatData.fileReaderLBSN

/**
  * Created by aamir on 13/10/16.
  */
class Evaluation {
  def stringToDate(dateString: String): Date = {
    val updateDate = dateString.replaceAll("T", "").replaceAll("Z", "")
    val formatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-ddhh:mm:ss")
    val date: Date = formatter.parse(updateDate)
    return date
  }
  def dateToString(inDate:Date): String ={// standard which is read
    val df:DateFormat=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
    val stringDate=df.format(inDate)
    return stringDate
  }
  def createDatasetForCrossValidation(inPercentTest:Double,inCCheckinsFile:String,inTrainCheckinFile:String,inTestCheckinFile:String): Unit ={
    val fr=new fileReaderLBSN
    /*val tempCheckins = scala.io.Source.fromFile(inCCheckinsFile).getLines().take(2)
      .map(t => t.split("\t"))
      .map{t =>
        println("\n")
        println("Date before::"+t(1))
        println("Date After::"+stringToDate(t(1)))
        println("Trying to convert::"+stringToDate(t(1)))
        dateToString(stringToDate(t(1)))
        (t(0).toLong, stringToDate(t(1)), t(2).toDouble, t(3).toDouble, t(4), t(5).toLong, t(1))}
      .toList.distinct*/
    val totalCheckins=fr.readCheckinFileNew(inCCheckinsFile).sortBy(t=> t._2)
    println("minimum Date::"+totalCheckins.minBy(t=> t._2)._2)
    println("maximum Date::"+totalCheckins.maxBy(t=> t._2)._2)

    val partIndex=totalCheckins((totalCheckins.size *inPercentTest).floor.toInt)
    //divide total data-set into to two parts first on the basis of number of tuple and then make them separate on the basis of time stamps
    //println("partition Index::"+partIndex.size)
    val partIndexTime=partIndex._2
    println("partition Date::"+partIndexTime)
    println("Total Checkins::"+totalCheckins.size)
    val trainCheckins=totalCheckins.filter(t=> t._2.before(partIndexTime) || t._2==partIndexTime)
    println("Training tuple size::"+trainCheckins.size)
    val testCheckins=totalCheckins.filter(t=> t._2.after(partIndexTime))
    println("Testing tuple size::"+testCheckins.size)
    /**writing check-ins for training data-set*/
    fr.writeCheckinsInFile(trainCheckins,inTrainCheckinFile)
    /**writing check-ins for testing data-set*/
    fr.writeCheckinsInFile(testCheckins,inTestCheckinFile)
  }

}
