import scala.collection.GenTraversableOnce
import scala.collection.JavaConverters._

object Config {
    def load(stream: java.io.InputStream, overwrite: Boolean) {
        val reader = new java.io.InputStreamReader(stream, "UTF-8")
        val props = new java.util.Properties
        props.load(stream)
        props.asScala.foreach { case (k,v) =>
            if (overwrite || System.getProperty(k) == null) {
                println(s"System.setProperty: $k=$v")
                System.setProperty(k,v)
            }
        }
    }
    def load(data: Array[Byte], overwrite: Boolean) {
        load(new java.io.ByteArrayInputStream(data), overwrite)
    }
    def load(path: String, overwrite: Boolean) {
        val stream = if (path startsWith "hdfs://") {
            val conf = new org.apache.hadoop.conf.Configuration()
            conf.set("fs.defaultFS", path.split('/').take(3).mkString("/"))
            val fs = org.apache.hadoop.fs.FileSystem.get(conf)
            fs.open(new org.apache.hadoop.fs.Path(path))
        } else {
            val file = new java.io.File(path)
            new java.io.FileInputStream(file)
        }
        try load(stream, overwrite)
        finally stream.close()
    }
}

class Config(path: String, overwrite: Boolean = false) {
    Config.load(path, overwrite)

    def getOption(key: String) = System.getProperty(key) match {
        case null => None
        case value => Some(value)
    }
    
    def get(key: String) = getOption(key).getOrElse(throw new Exception(s"key $key not found in $path"))
    def has(key: String) = !getOption(key).isEmpty
    def getOrElse(key: String, default: => String) = getOption(key).getOrElse(default)

    def slice(prefix: String) = System.getProperties.asScala.toMap.filter(_._1 startsWith prefix)

    override def toString = {
        val output = new java.io.ByteArrayOutputStream
        System.getProperties.store(output, "System settings")
        output.toString.split("\n").sortBy(identity).mkString("", "\n", "\n")
    }
}
