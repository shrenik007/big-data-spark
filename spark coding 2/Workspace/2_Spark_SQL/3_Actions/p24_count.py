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
def my_main(spark):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType([pyspark.sql.types.StructField("Name", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("Number", pyspark.sql.types.IntegerType(), True),
                                              pyspark.sql.types.StructField("LikesTennis", pyspark.sql.types.BooleanType(), True)
                                             ]
                                            )

    # 2. Operation C1: Creation createDataFrame
    inputDF = spark.createDataFrame( [ ('Anna', 1, True), ('Bruce', 2, False), ('Connor', 3, True), ('Diarmuid', 4, False) ], my_schema )

    # 3. Operation A1: We get the name of the columns
    resVAL = inputDF.count()

    # 4. We print the result
    print(resVAL)

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
    pass

    # 3. We configure the Spark Context
    pass

    # 4. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We run my_main
    my_main(spark)
