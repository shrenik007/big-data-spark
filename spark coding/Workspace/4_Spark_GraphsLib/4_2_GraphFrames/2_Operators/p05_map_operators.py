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
# FUNCTION my_map_vertices
# ------------------------------------------
def my_map_vertices(spark, my_dataset_dir):
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

    # 2. Operation P2: We persist myGF
    myGF.persist()

    # 3. Operation T3: We transform the vertices value
    my_new_verticesDF = myGF.vertices.withColumn("value", pyspark.sql.functions.col("id") + pyspark.sql.functions.col("value"))

    # 4. Operation C3: We create myNewGF from myNewVerticesDF and myEdgesDF
    my_newGF = graphframes.GraphFrame(my_new_verticesDF, myGF.edges)

    # 5. Operation T4: We get the sol_verticesDF
    sol_verticesDF = my_newGF.vertices

    # 5. Operation A1: We collect the DF
    resVAL = sol_verticesDF.collect()

    # 6. We print resVAL
    for item in resVAL:
      print(item)

# ------------------------------------------
# FUNCTION my_map_edges
# ------------------------------------------
def my_map_edges(spark, my_dataset_dir):
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

    # 2. Operation P2: We persist myGF
    myGF.persist()

    # 3. Operation T3: We transform the edges value
    my_new_edgesDF = myGF.edges.withColumn("value", pyspark.sql.functions.col("value") + 1)

    # 4. Operation C3: We create myNewGF from myNewVerticesDF and myEdgesDF
    my_newGF = graphframes.GraphFrame(myGF.vertices, my_new_edgesDF)

    # 5. Operation T4: We get the sol_edges_DF
    sol_edgesDF = my_newGF.edges

    # 5. Operation A1: We collect the DF
    resVAL = sol_edgesDF.collect()

    # 6. We print resVAL
    for item in resVAL:
      print(item)

# ------------------------------------------
# FUNCTION my_map_triplets
# ------------------------------------------
def my_map_triplets(spark, my_dataset_dir):
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

    # 2. Operation P2: We persist myGF
    myGF.persist()

    # 3. Operation T3: We transform the edges value
    my_new_edgesDF = myGF.triplets.select("edge")\
                                  .withColumn("src", pyspark.sql.functions.col("edge.src"))\
                                  .withColumn("dst", pyspark.sql.functions.col("edge.dst"))\
                                  .withColumn("value", pyspark.sql.functions.col("edge.value"))\
                                  .drop("edge")\
                                  .withColumn("value", pyspark.sql.functions.col("value") + 1)

    # 4. Operation C3: We create myNewGF from myNewVerticesDF and myEdgesDF
    my_newGF = graphframes.GraphFrame(myGF.vertices, my_new_edgesDF)

    # 5. Operation T4: We get the sol_edges_DF
    sol_edgesDF = my_newGF.edges

    # 5. Operation A1: We collect the DF
    resVAL = sol_edgesDF.collect()

    # 6. We print resVAL
    for item in resVAL:
      print(item)


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark, my_dataset_dir, option):
    # We decide which function to call to based on the option
    if (option == 1):
        my_map_vertices(spark, my_dataset_dir)
    elif (option == 2):
        my_map_edges(spark, my_dataset_dir)
    elif (option == 3):
        my_map_triplets(spark, my_dataset_dir)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We use as many input arguments as needed
    option = 3

    # 2. Local or Databricks
    local_False_databricks_True = True

    # 3. We set the path to my_dataset and my_result
    my_local_path = "/home/nacho/CIT/Tools/MyCode/Spark/"
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/4_Spark_GraphsLib/1_TinyGraph/my_dataset"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    my_main(spark, my_dataset_dir, option)
