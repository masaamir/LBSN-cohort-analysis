package ConvoysLBSN

import scala.collection.mutable.ListBuffer

/**
  * Created by XXX on 10/10XXX.
  */
class ConvoyVisitor (inVId:Long,inConvoyCount:Long,inCompanions:ListBuffer[ConvoyCompanion],
                     inOnLocations:ListBuffer[Long],inOnCategories:ListBuffer[String],inUserConvoys:ListBuffer[(ListBuffer[Long],ListBuffer[Long],ListBuffer[String])]) {
  val vId:Long=inVId
  val convoyCount:Long=inConvoyCount
  val companions:ListBuffer[ConvoyCompanion]=inCompanions
  val onLocations:ListBuffer[Long]=inOnLocations
  val onCategories:ListBuffer[String]=inOnCategories
  val convoys=inUserConvoys
  //val onTimeStamps:ListBuffer[Long]=inOnTimeStamps


}
