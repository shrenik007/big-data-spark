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

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType([pyspark.sql.types.StructField("Name", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("Val1", pyspark.sql.types.IntegerType(), True),
                                              pyspark.sql.types.StructField("Val2", pyspark.sql.types.IntegerType(), True),
                                              pyspark.sql.types.StructField("Val3", pyspark.sql.types.IntegerType(), True)
                                             ]
                                            )

    # 2. Operation C1: Creation createDataFrame
    inputDF = spark.createDataFrame( [ ('Anna', 1, 1, 10), ('Barry', 1, 1, 15), ('Connor', 1, 3, 20),
                                       ('Diarmuid', 2, 4, 25), ('Emma', 2, 4, 30), ('Finbarr', 2, 6, 35) ], my_schema )

    # 3. Operation T1: Window aggregation-based transformation to generate the ranking per country

    # 3.1. We create the window we want to aggregate by
    my_window = pyspark.sql.Window.partitionBy(inputDF["Val1"], inputDF["Val2"]).orderBy(inputDF["Val3"].desc())

    # 3.2. We apply the window aggregation-based transformation
    resultDF = inputDF.withColumn("Rank", pyspark.sql.functions.row_number().over(my_window))

    # 4. Operation A1: Action of displaying the content of resultDF
    resultDF.show()


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
