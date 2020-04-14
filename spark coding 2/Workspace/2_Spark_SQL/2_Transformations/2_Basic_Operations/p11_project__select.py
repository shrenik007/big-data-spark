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
import pyspark.sql.functions

# ------------------------------------------
# FUNCTION project_subset_columns
# ------------------------------------------
def project_subset_columns(spark):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType([pyspark.sql.types.StructField("Name", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("Number", pyspark.sql.types.IntegerType(), True),
                                              pyspark.sql.types.StructField("LikesTennis", pyspark.sql.types.BooleanType(), True)
                                             ]
                                            )

    # 2. Operation C1: Creation createDataFrame
    inputDF = spark.createDataFrame( [ ('Anna', 1, True), ('Bruce', 2, False), ('Connor', 3, True), ('Diarmuid', 4, False) ], my_schema )

    # 3. Operation T1: Transformation to select two columns from inputDF
    resultDF = inputDF.select(inputDF["LikesTennis"], inputDF["Name"])

    # 4. Operation P1: We persist resultDF
    resultDF.persist()

    # 5. Operation A1: Action to show the schema of resultDF
    resultDF.printSchema()

    # 6. Operation A2: Action of displaying the content of resultDF
    resultDF.show()


# --------------------------------------------
# FUNCTION project_subset_columns_rename_one
# --------------------------------------------
def project_subset_columns_rename_one(spark):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType([pyspark.sql.types.StructField("Name", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("Number", pyspark.sql.types.IntegerType(), True),
                                              pyspark.sql.types.StructField("LikesTennis", pyspark.sql.types.BooleanType(), True)
                                             ]
                                            )

    # 2. Operation C1: Creation createDataFrame
    inputDF = spark.createDataFrame( [ ('Anna', 1, True), ('Bruce', 2, False), ('Connor', 3, True), ('Diarmuid', 4, False) ], my_schema )

    # 3. Operation T1: Transformation to select two columns from inputDF and rename one of them
    resultDF = inputDF.select(inputDF["LikesTennis"], inputDF["Name"].alias("NewName"))

    # 4. Operation P1: We persist resultDF
    resultDF.persist()

    # 5. Operation A1: Action to show the schema of resultDF
    resultDF.printSchema()

    # 6. Operation A2: Action of displaying the content of resultDF
    resultDF.show()


# --------------------------------------------------
# FUNCTION project_subset_columns_generate_new_one
# --------------------------------------------------
def project_subset_columns_generate_new_one(spark, my_constant):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType([pyspark.sql.types.StructField("Name", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("Number", pyspark.sql.types.IntegerType(), True),
                                              pyspark.sql.types.StructField("LikesTennis", pyspark.sql.types.BooleanType(), True)
                                             ]
                                            )

    # 2. Operation C1: Creation createDataFrame
    inputDF = spark.createDataFrame( [ ('Anna', 1, True), ('Bruce', 2, False), ('Connor', 3, True), ('Diarmuid', 4, False) ], my_schema )

    # 3. Operation T1: Transformation to select one column from inputDF and generate a new one
    resultDF = inputDF.select(inputDF["LikesTennis"], pyspark.sql.functions.lit(my_constant).alias("NewColumn"))

    # 4. Operation P1: We persist resultDF
    resultDF.persist()

    # 5. Operation A1: Action to show the schema of resultDF
    resultDF.printSchema()

    # 6. Operation A2: Action of displaying the content of resultDF
    resultDF.show()


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark, option, my_constant):
    if (option == 1):
        print("\n\n--- Project a subset of the existing columns ---\n\n")
        project_subset_columns(spark)

    if (option == 2):
        print("\n\n--- Project a subset of the existing columns and rename one of them ---\n\n")
        project_subset_columns_rename_one(spark)

    if (option == 3):
        print("\n\n--- Project a subset of the existing columns and generate a new one ---\n\n")
        project_subset_columns_generate_new_one(spark, my_constant)


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

    my_constant = 5

    # 2. Local or Databricks
    pass

    # 3. We configure the Spark Context
    pass

    # 4. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We run my_main
    my_main(spark, option, my_constant)
