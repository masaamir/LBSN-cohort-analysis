package ConvoysLBSN

import scala.collection.mutable.ListBuffer

/**
  * Created by aamir on 10/10/16.
  */
class ConvoyCompanion (inVId:Long,inWithUsers:ListBuffer[Long],
                       inOnLocations:ListBuffer[Long],inOnCategories:ListBuffer[String]) {
  val vId:Long=inVId
  val withUsers:ListBuffer[Long]=inWithUsers
  val onLocations:ListBuffer[Long]=inOnLocations
  val onCategories:ListBuffer[String]=inOnCategories
  //val onTimeStamps:ListBuffer[Long]=inOnTimeStamps


  /*
  def this(iId:Long,iOnCategories:ListBuffer[String]){
    this(iId,new ListBuffer(),iOnCategories,new ListBuffer(),new ListBuffer())
  }*/

}