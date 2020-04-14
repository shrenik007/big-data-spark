
//-------------------------------------
// IMPORTS
//-------------------------------------
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{ Level, Logger }

//-------------------------------------
// OBJECT MyProgram
//-------------------------------------
object MyProgram {

  // ------------------------------------------
  // FUNCTION myMain
  // ------------------------------------------
  def myMain(sc: SparkContext, myDatasetDir: String, num_iterations: Int): Unit = {
    // 1. Operation C1: We create the graph with GraphLoader, which makes all vertex and edge attributes default to 1.
    val myGraphRDD = GraphLoader.edgeListFile(sc, myDatasetDir)

    // 3. Operation T2: We run the iterative algorithm via the Pregel API
    val myFinalGraphRDD = myGraphRDD.stronglyConnectedComponents(num_iterations)

    // 4. Operation A1: We get the VertexRDD
    val vertexRDD = myFinalGraphRDD.vertices

    // 5. Operation A2: We collect the RDD
    val resVAL = vertexRDD.collect()

    // 6. We print the result
    for( item <- resVAL ){
      println(item)
    }
  }

  // ------------------------------------------
  // PROGRAM MAIN ENTRY POINT
  // ------------------------------------------
  def main(args: Array[String]) {
    // 1. We use as many input arguments as needed
    val num_iterations = 3 //This is the minimum amount of iterations needed to solve the problem

    // 2. Local or Databricks
    val localFalseDatabricksTrue = false

    // 3. We set the path to my_dataset and my_result
    val myLocalPath = "/home/nacho/CIT/Tools/MyCode/Spark/"
    val myDatabricksPath = "/"

    var myDatasetDir : String = "FileStore/tables/4_Spark_Graph_Libs/1_TinyGraph/my_dataset"

    if (localFalseDatabricksTrue == false) {
      myDatasetDir = myLocalPath + myDatasetDir
    }
    else {
      myDatasetDir = myDatabricksPath + myDatasetDir
    }

    // 4. We configure the Spark Context sc
    var sc : SparkContext = null;

    // 4.1. Local mode
    if (localFalseDatabricksTrue == false){
      // 4.1.1. We create the configuration object
      val conf = new SparkConf()
      conf.setMaster("local")
      conf.setAppName("MyProgram")

      // 4.1.2. We initialise the Spark Context under such configuration
      sc = new SparkContext(conf)
    }
    // 4.2. Databricks Mode
    else{
      sc = SparkContext.getOrCreate()
    }

    Logger.getRootLogger.setLevel(Level.WARN)
    for( index <- 1 to 10){
      printf("\n")
    }

    // 5. We call to myMain
    myMain(sc, myDatasetDir, num_iterations)
  }

}

//------------------------------------------------
// TRIGGER: Local (Comment) - Databricks (Enable)
//------------------------------------------------
//MyProgram.main(Array())
