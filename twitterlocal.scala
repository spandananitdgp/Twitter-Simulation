import akka.actor.{ Actor, ActorRef, Props, ActorSystem }
import akka.actor.actorRef2Scala
import akka.dispatch.ExecutionContexts.global
import akka.pattern.ask
import akka.util.Timeout
import scala.io.Source.fromFile
import java.security.MessageDigest
import akka.routing.RoundRobinRouter
import java.net.InetAddress
import com.typesafe.config.ConfigFactory
import akka.util.Timeout
import scala.concurrent.duration._
import akka.pattern.ask
import akka.dispatch.ExecutionContexts._
import scala.collection.mutable._
import scala.util.Random
import java.io._
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
import spray.http._
import spray.client.pipelining._
import akka.actor.ActorSystem
import scala.util.Random

case class msg(ID:Int, tweet: String)
case class PrintList(i: Int)
case class usertable(n: Int)
case class UserTweet(n: Int)
case class followuser(m: Int, n : Int)
case class unfollowuser(m: Int)
case class UserTweet_Self(n: Int)
case class TweetCount()
case class ReTweetPost(n : Int)
case class FavTweetPost(n : Int)
case class GetCount()
case class HtweetPost(i:Int, htweet:String)

object twitterlocal extends App with SimpleRoutingApp
{ 
  var TweetCount: Int =0
  
  def Increment()
  {
    TweetCount = TweetCount +1
  }
   override def main(args: Array[String]) 
  {
     
  /*  val hostname = InetAddress.getLocalHost.getHostName
    val config = ConfigFactory.parseString(
        """ 
        akka
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
              hostname = "192.168.0.17" 
              port = 5150
            } 
          }      
        }""") */
     
    implicit val system = ActorSystem("LocalSystem")
    val cores: Int = Runtime.getRuntime().availableProcessors()
    var TweetList = new ListBuffer[Queue[String]] ()
    var HTweetList = new ListBuffer[Queue[String]] ()
    var FTweetList = new ListBuffer[Queue[String]] ()
    var FolTab = new ArrayBuffer[ArrayBuffer[Int]] ()

    println("Starting Twitter Server....\n")
    
    val ActorRouter = system.actorOf(Props(new ServerActor(TweetList, HTweetList, FTweetList, FolTab, system)).withRouter(RoundRobinRouter(cores)), name = "ServerActor")
    
    def getJson(route: Route) = get {
    respondWithMediaType(MediaTypes.`application/json`) { route }
    }
   
    startServer(interface = "localhost", port = 8080) {
    get {
      path("") { ctx =>
        ctx.complete("Welcome to Tweeter!")
      }
    }~
    post {
      path("tweet" / "follow") {
        parameters("user".as[Int], "follows".as[Int]) { (user, follows) =>
         ActorRouter!followuser(user, follows)
         ActorRouter!unfollowuser(user)
          complete {
            "User:"+user+"Follows User:"+ follows
          }
        }
      }
    }~
    post {
      path("tweet" / "new") {
        parameters("user".as[Int], "content".as[String]) { (user, content) =>
         ActorRouter!msg(user,content)
          complete {
            "Tweet: "+content
          }
        }
      }
    }~
    post {
      path("tweet" / "newhash") {
        parameters("user".as[Int], "content".as[String]) { (user, content) =>
         ActorRouter!HtweetPost(user,content)
          complete {
            "HashedTweet: "+content
          }
        }
      }
    }~
    post {
      path("tweet" / "total") {
        parameters("numUser".as[Int]) { (numUser) =>
       ActorRouter!usertable(numUser)
          complete {
            "Number of Users="+numUser
          }
        }
      }
    }~
    post {
      path("tweet" / "retweet") {
        parameters("user".as[Int]) { (user) =>
       ActorRouter!ReTweetPost(user)
       ActorRouter!FavTweetPost(Random.nextInt(10))
          complete {
            "User ID="+user
          }
        }
      }
    }~    
    post {
      path("tweet" / "User") {
        parameters("UserID".as[Int]) { (UserID) =>
       ActorRouter!UserTweet(UserID)
          complete {
            "UserID="+UserID
          }
        }
      }
    }~    
    post {
      path("tweet" / "Stop") {
        parameters("Counter".as[Int]) { (Counter) =>
       ActorRouter!GetCount()
          complete {
            "Stop Counter="+Counter
          }
        }
      }
    }~
    post {
      path("tweet" / "User_Self") {
        parameters("UserID".as[Int]) { (UserID) =>
       ActorRouter!UserTweet_Self(UserID)
          complete {
            "UserID_Self="+UserID
          }
        }
      }
    }    
   }
  }

  class ServerActor(TweetList: ListBuffer[Queue[String]],HTweetList: ListBuffer[Queue[String]],FTweetList: ListBuffer[Queue[String]],FolTab : ArrayBuffer[ArrayBuffer[Int]], system:ActorSystem) extends Actor 
  { 
    def receive = 
    {  
      case usertable(n) =>
      {      
      val numusers = n
      for(i <- 0 to numusers-1)
      {
        val Q = new Queue[String]
        TweetList+= Q
      }
      for(i <- 0 to 9)
      {
        val HQ = new Queue[String]
        HTweetList+= HQ
      }
      for(i <- 0 to 9)
      {
        val FQ = new Queue[String]
        FTweetList+= FQ
      }
      for(k<-0 to numusers-1)
      {
        val F = new ArrayBuffer[Int]         
        FolTab+= F
      }
      for(i <- 0 to numusers-1)
      {
        var fol= Random.nextInt(numusers/100)
        for(j<- 0 to fol)
        {
          var ran=Random.nextInt(numusers)
          while(ran==i || FolTab(i).contains(ran))
          {
            ran=Random.nextInt(numusers)
          }
          FolTab(i)+= ran 
        }
      }
      //println("List of users each User follows: \n")
      //println(FolTab)
      //println("\n")
      
      //sender ! START()
    }
    
    case followuser(m,n)=>
    {
      if(n==m || FolTab(m).contains(n))
      {println("User: "+m+" is already Following User: "+n)}
      else
      {
      FolTab(m)+= n
      println("User:"+m+" has started to follow User: "+ n)
      twitterlocal.Increment()
      
      //println("User: "+m+"'s updated following list")
      //println(FolTab(m))
      }
      println("\n")
    }
    case unfollowuser(m)=>
    {
      var unfol = Random.nextInt(FolTab(m).size)
      //println("User: "+m+"'s following list before unfollowing User "+unfol+" : "+FolTab(m))
      FolTab(m)(unfol)=FolTab(m)(FolTab(m).size-1)
      println("user:"+m+" unfollowed user:"+FolTab(m)(unfol))
      FolTab(m).trimEnd(1)
      twitterlocal.Increment()
      //println("User: "+m+"'s following list after unfollowing User "+unfol+" : "+ FolTab(m))
      println("\n")
    }
    case msg(i, tweet) => 
    {
      TweetList(i)+= tweet
      twitterlocal.Increment()
      if(TweetList(i).length==101)
        TweetList(i).dequeue
    }
    case HtweetPost(i, htweet) => 
    {
      val tweetsplit= htweet.split("#")
      val tagid=tweetsplit(1).toInt
      TweetList(i)+= htweet
      HTweetList(tagid)+= i+":"+htweet
      println("Tweets with hash ID: "+tagid+" : "+HTweetList(tagid))
      println("\n")
      twitterlocal.Increment()
      if(TweetList(i).length==101)
        TweetList(i).dequeue
    }
    
    case ReTweetPost(n) => 
    {
      //println(n)
      //println(FolTab(n))
      var k=0
      var ranfol = Random.nextInt(FolTab(n).size)
      if(FolTab(n).isEmpty)
      {
        println("Isn't following anyone, nothing to retweet!\n")
      }
      else 
      {
        //println(TweetList(FolTab(n)(ranfol)))
        while(TweetList(FolTab(n)(ranfol)).isEmpty && k!=FolTab(n).size)
        {
          ranfol = k
          k+=1
        }
        if(k!=FolTab(n).size)
        {
          var rtweet=TweetList(FolTab(n)(ranfol))(Random.nextInt(TweetList(FolTab(n)(ranfol)).size))
          rtweet="RT @"+FolTab(n)(ranfol)+":"+rtweet
          //println(FolTab(n))
          println(n+" : "+rtweet)
          TweetList(n)+= rtweet
          twitterlocal.Increment()
          if(TweetList(n).length==101)
            TweetList(n).dequeue
        }
        else
        {
          println("people you are following haven't tweeted yet.!")
        }
      }
      println("\n")
    }
    case FavTweetPost(n) => 
    {
      //println(n)
      //println(FolTab(n))
      var k=0
      var ranfol = Random.nextInt(FolTab(n).size)
      if(FolTab(n).isEmpty)
      {
        println("Isn't following anyone, nothing to favorite!\n")
      }
      else 
      {
        //println(TweetList(FolTab(n)(ranfol)))
        while(TweetList(FolTab(n)(ranfol)).isEmpty && k!=FolTab(n).size)
        {
          ranfol = k
          k+=1
        }
        if(k!=FolTab(n).size)
        {
          var ftweet=TweetList(FolTab(n)(ranfol))(Random.nextInt(TweetList(FolTab(n)(ranfol)).size))
          //println(FolTab(n))
          println("User "+n+" favorites User "+FolTab(n)(ranfol)+"'s tweet: "+ftweet)
          FTweetList(n)+= ftweet
          twitterlocal.Increment()
          println("User "+n+"'s list of favourite tweets: "+FTweetList(n))
          if(FTweetList(n).length==21)
             FTweetList(n).dequeue
          
        }
        else
        {
          println("people you are following haven't tweeted yet, No tweets to favorite.!")
        }
      }
      println("\n")
    }
    case GetCount() =>
    {
       
      
        println("Final Tweet Count: "+twitterlocal.TweetCount)
    }
    
    case UserTweet(n) =>
    {
          for(k<-0 to (FolTab(n).size)-1)
          {
            if(!TweetList(FolTab(n)(k)).isEmpty)
              println("Tweets User "+n+" Follows: "+FolTab(n)(k)+" :"+TweetList(FolTab(n)(k)))
            twitterlocal.Increment()
      
              //sender ! PrintTweet(n,TweetList(FolTab(n)(k)))
          }
          println("\n")
    }
    case UserTweet_Self(n) =>
    {
            if(!TweetList(n).isEmpty)
              println("User "+n+"'s Tweets: "+TweetList(n))
              println("\n")
            twitterlocal.Increment()
      
              //sender ! PrintTweet(n,TweetList(FolTab(n)(k))) 
     }    
    }
  }
}
