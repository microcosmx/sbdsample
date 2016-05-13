import org.apache.hadoop.fs._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import scala.io.Source
import scala.reflect._
import scala.reflect.runtime.{universe => ru}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

case class mars_tianchi_songs(
    song_id:String, 
    artist_id:String, 
    publish_time:String,
    song_init_plays:String,
    Language:String,
    Gender:String
)

case class mars_tianchi_user_actions(
    user_id:String, 
    song_id:String, 
    gmt_create:String,
    action_type:String,
    Ds:String
)


case class artistId_plays_ds(
    artist_id:String,
    Plays:Double,
    Downloads:Double,
    Favors:Double,
    Ds:Double
)

case class params(
    label:Double,
    features:Vector
)


case class State(
    universe: Long = 0,
    generation: Long = -1,
    modificationTime: Long = -1)
{
    def makeId(prefix: String) = s"$prefix-0"
}