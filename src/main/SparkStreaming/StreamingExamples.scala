import com.sun.javafx.util.Logging
import org.apache.log4j.{Level, Logger}

/**
  * Streaming 套接字流
  *
  * @author xjh 2018.09.28
  */
object StreamingExamples extends Logging{
    def setStreamingLogLevels(){
      val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
      if(!log4jInitialized){
        println("Setting log level to [WARN] for streaming example." +
          " To override add a custom log4j.properties to the classpath.")
        Logger.getRootLogger.setLevel(Level.WARN)
      }

  }
}
