package ConvoysLBSN

import scala.collection.mutable.ListBuffer

/**
  * Created by XXX on 10/10/16.
  */
class ConvoyCompanion (inVId:Long,inConvoysCount:Long,
                       inWithUsers:ListBuffer[Long],inOnLocations:ListBuffer[Long],inOnCategories:ListBuffer[String]) {
  val vId:Long=inVId
  val convoyCount=inConvoysCount
  val withUsers:ListBuffer[Long]=inWithUsers
  val onLocations:ListBuffer[Long]=inOnLocations
  val onCategories:ListBuffer[String]=inOnCategories
  //val onTimeStamps:ListBuffer[Long]=inOnTimeStamps


  /*
  def this(iId:Long,iOnCategories:ListBuffer[String]){
    this(iId,new ListBuffer(),iOnCategories,new ListBuffer(),new ListBuffer())
  }*/

}
