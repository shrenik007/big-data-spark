
# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Python interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------

import pyspark
import pyspark.sql.functions
import graphframes

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark, my_dataset_dir):
    # 1. We load my_edgesDF from the dataset

    # 1.1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType(
      [pyspark.sql.types.StructField("src", pyspark.sql.types.IntegerType(), True),
       pyspark.sql.types.StructField("dst", pyspark.sql.types.StringType(), True)
       ])

    # 1.2. Operation C1: We create the DataFrame from the dataset and the schema
    my_edges_datasetDF = spark.read.format("csv") \
      .option("delimiter", " ") \
      .option("quote", "") \
      .option("header", "false") \
      .schema(my_schema) \
      .load(my_dataset_dir)

    # 1.3. Operation T1: We add the value column
    my_edgesDF = my_edges_datasetDF.withColumn("value", pyspark.sql.functions.lit(1))

    # 1.4. Operation P1: We persist it
    my_edgesDF.persist()

    # 1.5. Operation T3: We transform my_edgesDF so as to get my_verticesDF
    my_verticesDF = my_edgesDF.select(my_edgesDF["src"]) \
      .withColumnRenamed("src", "id") \
      .dropDuplicates() \
      .withColumn("value", pyspark.sql.functions.lit(1))

    # 1.6. Operation C3: We create myGF from my_verticesDF and my_edgesDF
    myGF = graphframes.GraphFrame(my_verticesDF, my_edgesDF)

    # 2. Operation P1: We persist myGF
    myGF.persist()

    # 3. Operation C2: We create my_external_verticesDF
    p1 = pyspark.sql.Row(id=1, value=10)
    p2 = pyspark.sql.Row(id=2, value=10)
    p3 = pyspark.sql.Row(id=3, value=10)
    my_external_verticesDF = spark.createDataFrame([p1, p2, p3]).withColumnRenamed("id", "B_id").withColumnRenamed("value", "B_value")

    # 4. We get the vertices from myGF
    myGF_verticesDF = myGF.vertices.withColumnRenamed("id", "A_id").withColumnRenamed("value", "A_value")

    # 5. Operation T4: We join my_external_verticesDF with myGF_verticesDF
    res_verticesDF = myGF_verticesDF.join(my_external_verticesDF,
                                          myGF_verticesDF["A_id"] == my_external_verticesDF["B_id"],
                                          "left_outer") \
                                    .na.fill(0, ["B_value"]) \
                                    .withColumn("new_value", pyspark.sql.functions.col("A_value") + pyspark.sql.functions.col("B_value")) \
                                    .drop("A_value").drop("B_value").drop("B_id") \
                                    .withColumnRenamed("A_id", "id") \
                                    .withColumnRenamed("new_value", "value")
    # 6. Operation A1: We display the DF
    res_verticesDF.show()

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. Local or Databricks
    local_False_databricks_True = False

    # 2. We set the path to my_dataset and my_result
    my_local_path = "/home/nacho/CIT/Tools/MyCode/Spark/"
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/4_Spark_Graph_Libs/1_TinyGraph/my_dataset"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 3. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    # 4. We call to our main function
    my_main(spark, my_dataset_dir)
