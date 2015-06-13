import akka.actor.{ Actor, ActorRef, Props, ActorSystem }
import akka.actor.actorRef2Scala
import akka.dispatch.ExecutionContexts.global
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration.DurationInt
import scala.io.Source.fromFile
import java.security.MessageDigest
import java.util.UUID
import akka.actor.{ Address, AddressFromURIString }
import akka.routing.RoundRobinRouter
import java.net.InetAddress
import com.typesafe.config.ConfigFactory
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import scala.util.Random
import scala.collection.mutable._
import java.io._
import spray.http._
import spray.client.pipelining._
import akka.actor.ActorSystem
import scala.util.Random
import org.json4s.native.Serialization
import org.json4s.native.Serialization._
import org.json4s.ShortTypeHints
import org.json4s.Formats
import spray.routing._
import spray.http.MediaTypes
import spray.routing.Directive.pimpApply
import spray.routing.SimpleRoutingApp
import spray.routing.directives.ParamDefMagnet.apply
import spray.httpx._

case class Inilialize()
case class START()
case class Tweet(i:Int)
case class HashedTweet(i:Int)
case class ReTweet(i:Int)
case class Stop(start: Long, cores: Int, a: ArrayBuffer[ActorRef])
case class users(num:Int)
case class Retrieve(num:Int)
case class Retrieve_Self(num:Int)
case class PrintTweet(n : Int, TweetList: Queue[String])
case class Final (count: Int)
case class CheckStart(Start:Long)
case class follow(num : Int)
object twitterremote
{
  //System.setOut(new PrintStream(new FileOutputStream("output.doc")));
  def main(args: Array[String])
  {
    
   /* val hostname = InetAddress.getLocalHost.getHostName
    val config = ConfigFactory.parseString(
        """akka
        {
          actor
          {
            provider = "akka.remote.RemoteActorRefProvider"
          }
          remote
          {
                   enabled-transports = ["akka.remote.netty.tcp"]
            netty.tcp
            {
              hostname = "127.0.0.1"
              port = 0
            }
          }     
        }""")*/
    
    val system = ActorSystem("HelloRemoteSystem")
    val Master =system.actorOf(Props(new Master((args(0).toInt),system)),name="Master")
    
    println("Starting Twitter Client....\n")
    
    Master! Inilialize()
    
  }
}

class Master(UserCount : Int, system: ActorSystem) extends Actor 
{
      var ClientActor= new ArrayBuffer[ActorRef]() 
      //var remoteMaster = context.actorFor("akka.tcp://LocalSystem@"+ServerAddress+":5150/user/ServerActor")
      
      import system.dispatcher
      val pipeline2 = sendReceive
      val securePipeline2 = addCredentials(BasicHttpCredentials("zest", "97531")) ~> sendReceive
      val result = securePipeline2(Post("http://localhost:8080/list/all"))
      result.foreach { response =>}
      
      def receive = 
      {
      case Inilialize() => 
      {
        val start: Long = System.currentTimeMillis
        var ClientActor1= context.actorOf(Props(new Twitter(system)),name="checkstartactor")       
      
        pipeline2(Post("http://localhost:8080/tweet/total?numUser="+UserCount))
        var duration:Duration = (System.currentTimeMillis - start).millis
        //import system.dispatcher
        //system.scheduler.schedule(0 milliseconds,10 milliseconds, ClientActor1, CheckStart(start))
          
        var duration1 = (System.currentTimeMillis - start).millis
        while(duration1<30.second)
        {
          duration1 = (System.currentTimeMillis - start).millis
        }     
           self ! START() 
      
      }
      
      case START() =>
      {
        val start: Long = System.currentTimeMillis
        val cores: Int = Runtime.getRuntime().availableProcessors()
        for(i <-0 until cores) 
        {
          ClientActor += context.actorOf(Props(new Twitter(system)),name="Twitter"+i)       
        }

        import system.dispatcher
          system.scheduler.schedule(0 milliseconds,100 milliseconds, ClientActor(cores-1), Stop(start,cores, ClientActor))
          
         for (i <- 0 to (5*UserCount/10)-1 )
        {
          import system.dispatcher
            system.scheduler.schedule(0 milliseconds,10 milliseconds,ClientActor(i%(cores-2)),Tweet(i))
        }
         for (i <- (5*UserCount/10) to (9*UserCount/10)-1 )
        {
          import system.dispatcher
            system.scheduler.schedule(0 milliseconds,1000 milliseconds,ClientActor(cores-2),Tweet(i))
        }
         for (i <- (9*UserCount/10) to UserCount-1 )
        {
          import system.dispatcher
            system.scheduler.schedule(0 milliseconds,70000 milliseconds,ClientActor(cores-1),HashedTweet(i))
        }
         
        import system.dispatcher
          system.scheduler.schedule(0 milliseconds,80000 milliseconds, ClientActor(cores-1), Retrieve(UserCount))  
         
        import system.dispatcher
          system.scheduler.schedule(0 milliseconds,90000 milliseconds, ClientActor(cores-1), Retrieve_Self(UserCount))          
          
        import system.dispatcher
          system.scheduler.schedule(0 milliseconds,60000 milliseconds, ClientActor(cores-1), follow(UserCount))          
       
       import system.dispatcher
          system.scheduler.schedule(0 milliseconds,50000 milliseconds, ClientActor(cores-1), ReTweet(UserCount))          
          
      }
    }
} 

class Twitter(system: ActorSystem) extends Actor 
{
    import system.dispatcher
    val pipeline1 = sendReceive
    val securePipeline1 = addCredentials(BasicHttpCredentials("zest", "97531")) ~> sendReceive
    val result = securePipeline1(Post("http://localhost:8080/list/all"))
    result.foreach { response =>}
 
   //var remote = context.actorFor("akka.tcp://LocalSystem@"+ServerAddress+":5150/user/ServerActor")
    
    def receive = 
    {
      case Tweet(i) => 
      {
        pipeline1(Post("http://localhost:8080/tweet/new?user="+i+"&content="+Random.alphanumeric.take(140).mkString))
        //remote ! msg(i,TwitterString )
      }
      case HashedTweet(i) => 
      {
        var htweet=Random.alphanumeric.take(130).mkString+"%23"+Random.nextInt(10)
        pipeline1(Post("http://localhost:8080/tweet/newhash?user="+i+"&content="+htweet))
        //remote ! msg(i,TwitterString )
      }
      
      case ReTweet(i) => 
      {
        pipeline1(Post("http://localhost:8080/tweet/retweet?user="+Random.nextInt(i)))
        //remote ! msg(i,TwitterString )
      }
      case follow(num) => 
      {
        pipeline1(Post("http://localhost:8080/tweet/follow?user="+Random.nextInt(num)+"&follows="+Random.nextInt(num)))
        //remote ! msg(i,TwitterString )
      }

      case Retrieve(num) => 
      {
        pipeline1(Post("http://localhost:8080/tweet/User?UserID="+Random.nextInt(num)))
        //remote ! UserTweet(UserID)
      }
      case Retrieve_Self(num) => 
      {
        pipeline1(Post("http://localhost:8080/tweet/User_Self?UserID="+Random.nextInt(num)))
        //remote ! UserTweet(UserID)
      }      
      case PrintTweet (n,t) =>
      {
        println(n+" : "+t)
      }
      
      case Stop(start,cores,a) => 
      {
        val duration: Duration = (System.currentTimeMillis - start).millis
        if (duration > 100.second)
        {
         // remote ! GetCount()
          pipeline1(Post("http://localhost:8080/tweet/Stop?Counter="+1))
          println("\nKilling Actors\n")
          for (i <- 0 to cores - 1 )
          {
            context.stop(a(i))
          }
          //System.exit(0)
        }
      }         
    }
}