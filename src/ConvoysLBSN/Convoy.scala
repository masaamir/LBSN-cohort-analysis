package ConvoysLBSN

/**
 * Created by XXX on 9/19/2016.
 */
import scala.collection.mutable.ListBuffer
import scala.reflect.internal.util.Collections

class Convoy  {

  //private List<Integer> objs = new ArrayList<Integer>();
  var startTime:Long= -1L
  var endTime:Long= -1L
  var extended:Boolean=false
  var absorbed:Boolean=false
  var matched:Boolean=false
  var rightOpen:Boolean=false
  var leftOpen:Boolean=false
  var id:Long=0L
  var users:ListBuffer[Long]=new ListBuffer()
  var locations:ListBuffer[Long]=new ListBuffer()
  var timeStamps:ListBuffer[Long]=new ListBuffer()



  def lifeTime(): Long ={
    return endTime-startTime+1
  }

  def getUsers(): ListBuffer[Long] ={
    return users
  }
  def getLocations():ListBuffer[Long]={
    return locations
  }
  def getTimeStamps():ListBuffer[Long]={
    return timeStamps
  }

  def hasSameUsers(v:Convoy): Boolean ={
    val vUsers = v.getUsers()
    if(users==vUsers ){
      return true;
    }
    else
      return false;
  }
  def isSubset(c:Convoy):Boolean={
    if(users.toSet.subsetOf(c.getUsers().toSet))
      return true;
    else
      return false;
  }

  def intersection(v:Convoy):ListBuffer[Long]={
     return users.intersect(v.getUsers())
  }
  def addUsers(uid:Long){
    this.users += uid
  }
  def addLocations(lid:Long){
    this.locations += lid
  }
  def addTimeStamp(tid:Long): Unit ={
    this.timeStamps +=tid
  }
  def setTime(l:Long){
    this.startTime = l;
    this.endTime = l;
  }
  def isExtended():Boolean= {
    return extended
  }

  def setExtended(extended:Boolean) {
    this.extended = extended;
  }

  def isAbsorbed():Boolean= {
    return absorbed;
  }

  def setAbsorbed(absorbed:Boolean) {
    this.absorbed = absorbed;
  }

  def isMatched():Boolean= {
    return matched;
  }

  def setMatched(matched:Boolean) {
    this.matched = matched;
  }
  def getStartTime():Long= {
    return startTime;
  }
  /*def createConvoyOld(inList:(ListBuffer[Long],ListBuffer[Long],ListBuffer[Long],Long,Long)): Convoy={ //inList:{Users},{locs},{TimeStamps},timeStampId
    val convoy:Convoy = new Convoy()
    inList._1.foreach{u=> //add users
      convoy.addUsers(u)
    }
    //println("get users::"+convoy.getUsers())
    inList._2.foreach{l=> // add locations
      convoy.addLocations(l)
    }
    inList._3.foreach{t=> // add timeStamps
      convoy.addTimeStamp(t)
    }
    convoy.startTime=inList._4
    convoy.endTime=inList._5
    //convoy.setTime(inList._3)
    //convoy.users=convoy.users.sortBy(t=> t)
     // convoy.getUsers().sortBy(t=> t)
    return convoy
  }*/
  def createConvoy(inList:(ListBuffer[Long],ListBuffer[Long],ListBuffer[Long],Long,Long)): Convoy={ //inList:{Users},{locs},{TimeStamps},timeStampId
    //val convoy:Convoy = new Convoy()
    /*inList._1.foreach{u=> //add users
      convoy.addUsers(u)
    }*/
    //convoy.
    users ++=inList._1
    //println("get users::"+convoy.getUsers())
    /*inList._2.foreach{l=> // add locations
      convoy.addLocations(l)
    }*/
    //convoy.
    locations ++=inList._2
    /*inList._3.foreach{t=> // add timeStamps
      convoy.addTimeStamp(t)
    }*/
    //convoy.
    timeStamps ++=inList._3
    //convoy.
    startTime=inList._4
    //convoy.
    endTime=inList._5
    //convoy.setTime(inList._3)
    //convoy.users=convoy.users.sortBy(t=> t)
    // convoy.getUsers().sortBy(t=> t)
    return this
  }

}
