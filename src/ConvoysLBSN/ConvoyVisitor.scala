package ConvoysLBSN

import scala.collection.mutable.ListBuffer

/**
  * Created by aamir on 10/10/16.
  */
class ConvoyVisitor (inVId:Long,inCompanions:ListBuffer[ConvoyCompanion],
                     inOnLocations:ListBuffer[Long],inOnCategories:ListBuffer[String]) {
  val vId:Long=inVId
  val companions:ListBuffer[ConvoyCompanion]=inCompanions
  val onLocations:ListBuffer[Long]=inOnLocations
  val onCategories:ListBuffer[String]=inOnCategories
  //val onTimeStamps:ListBuffer[Long]=inOnTimeStamps


}
