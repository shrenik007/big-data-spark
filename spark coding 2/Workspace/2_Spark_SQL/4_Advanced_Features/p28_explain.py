# --------------------------------------------------------
#
# PYTHON PROGRAM DEFINITION
#
# The knowledge a computer has of Python can be specified in 3 levels:
# (1) Prelude knowledge --> The computer has it by default.
# (2) Borrowed knowledge --> The computer gets this knowledge from 3rd party libraries defined by others
#                            (but imported by us in this program).
# (3) Generated knowledge --> The computer gets this knowledge from the new functions defined by us in this program.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer first processes this PYTHON PROGRAM DEFINITION section of the file.
# On it, our computer enhances its Python knowledge from levels (2) and (3) with the imports and new functions
# defined in the program. However, it still does not execute anything.
#
# --------------------------------------------------------

import pyspark

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark, my_dataset_dir):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType([pyspark.sql.types.StructField("Name", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("Goals", pyspark.sql.types.IntegerType(), True)
                                             ]
                                            )

    # 2. Operation C1: Creation createDataFrame
    inputDF = spark.read.format("csv") \
        .option("delimiter", ";") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)

    # 3. Operation P1: Persist
    inputDF.persist()

    # 4. Operation T1: Sum of goals
    aux1DF = inputDF.agg( {"Goals" : "sum"} )

    # 5. Operation T2: Rename column
    sumDF = aux1DF.withColumnRenamed("sum(Goals)", "sum")

    # 6. Operation T3: Filter
    filterDF = inputDF.filter(inputDF["Goals"] >= 5)

    # 7. Operation T4: Crossjoin
    joinDF = filterDF.crossJoin(sumDF)

    # 8. Operation T5: Percentage
    aux2DF = finalDF.withColumn("perc", joinDF["Goals"] / joinDF["sum"])

    # 9. Operation T6: Drop
    resultDF = aux2DF.drop("sum")

    # 10. Operation P2: Persist
    resultDF.persist()

    # 11. Operation E1: Explain
    resultDF.explain()

    # 12. Operation A1: Collect
    resVAL = resultDF.collect()

    # 13. We print the result
    for item in resVAL:
        print(item)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We use as many input arguments as needed
    pass

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    my_local_path = "/home/nacho/CIT/Tools/MyCode/Spark/"
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/2_Spark_SQL/my_dataset/my_explain_example.csv"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We run my_main
    my_main(spark, my_dataset_dir)