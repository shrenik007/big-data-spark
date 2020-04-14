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
import pyspark.sql.functions as func
import pyspark

# ------------------------------------------
# FUNCTION ex1
# ------------------------------------------
def ex1(spark, my_dataset_dir):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("status", pyspark.sql.types.IntegerType(), True),
         pyspark.sql.types.StructField("name", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("longitude", pyspark.sql.types.FloatType(), True),
         pyspark.sql.types.StructField("latitude", pyspark.sql.types.FloatType(), True),
         pyspark.sql.types.StructField("dateStatus", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("bikesAvailable", pyspark.sql.types.IntegerType(), True),
         pyspark.sql.types.StructField("docksAvailable", pyspark.sql.types.IntegerType(), True)
         ])

    # 2. Operation C2: We create the DataFrame from the dataset and the schema
    inputDF = spark.read.format("csv") \
        .option("delimiter", ";") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)
    # 3. Operation A1: We count the number of entries in inputDF
    number_entries = inputDF.count()
    # 4. We print the result
    print('\n------------------------------')
    print('Exercise 1:')
    print('------------------------------\n')
    print(number_entries)

# ------------------------------------------
# FUNCTION ex2
# ------------------------------------------
def ex2(spark, my_dataset_dir):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("status", pyspark.sql.types.IntegerType(), True),
         pyspark.sql.types.StructField("name", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("longitude", pyspark.sql.types.FloatType(), True),
         pyspark.sql.types.StructField("latitude", pyspark.sql.types.FloatType(), True),
         pyspark.sql.types.StructField("dateStatus", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("bikesAvailable", pyspark.sql.types.IntegerType(), True),
         pyspark.sql.types.StructField("docksAvailable", pyspark.sql.types.IntegerType(), True)
         ])

    # 2. Operation C2: We create the DataFrame from the dataset and the schema
    inputDF = spark.read.format("csv") \
        .option("delimiter", ";") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)

    # 3. Operation T2: We group the information by the name of the station
#     grouped_by_col = inputDF.select("name").distinct()
    grouped_by_col = inputDF.groupBy('name').agg(func.countDistinct('name'))
    # 4. Operation A1: We count the amount of rows in infoDF
    total_unique_stations = grouped_by_col.count()
    # 5. We print the result
    print('\n------------------------------')
    print('Exercise 2:')
    print('------------------------------\n')
    print(total_unique_stations)

# ------------------------------------------
# FUNCTION ex3
# ------------------------------------------
def ex3(spark, my_dataset_dir):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("status", pyspark.sql.types.IntegerType(), True),
         pyspark.sql.types.StructField("name", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("longitude", pyspark.sql.types.FloatType(), True),
         pyspark.sql.types.StructField("latitude", pyspark.sql.types.FloatType(), True),
         pyspark.sql.types.StructField("dateStatus", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("bikesAvailable", pyspark.sql.types.IntegerType(), True),
         pyspark.sql.types.StructField("docksAvailable", pyspark.sql.types.IntegerType(), True)
         ])

    # 2. Operation C2: We create the DataFrame from the dataset and the schema
    inputDF = spark.read.format("csv") \
        .option("delimiter", ";") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)

    # 3. Operation T2: We group the information by the name of the station
    grouped_by_col = inputDF.groupBy('name').agg(func.countDistinct('name'))
#     grouped_by_col = inputDF.select('name').distinct()
    # 4. Operation T3: We remove the extra column generated by the group aggregation command
    grouped_by_col = grouped_by_col.select('name')
    # 5. Operation A1: We collect the rows
    collected_rows = grouped_by_col.collect()

    # 6. We print the result
    print('\n------------------------------')
    print('Exercise 3:')
    print('------------------------------\n')
    for row in collected_rows:
      print(row)

# ------------------------------------------
# FUNCTION ex4
# ------------------------------------------
def ex4(spark, my_dataset_dir):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("status", pyspark.sql.types.IntegerType(), True),
         pyspark.sql.types.StructField("name", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("longitude", pyspark.sql.types.FloatType(), True),
         pyspark.sql.types.StructField("latitude", pyspark.sql.types.FloatType(), True),
         pyspark.sql.types.StructField("dateStatus", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("bikesAvailable", pyspark.sql.types.IntegerType(), True),
         pyspark.sql.types.StructField("docksAvailable", pyspark.sql.types.IntegerType(), True)
         ])

    # 2. Operation C2: We create the DataFrame from the dataset and the schema
    inputDF = spark.read.format("csv") \
        .option("delimiter", ";") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)

    # 3. Operation T2: We group the information by the name of the station
    grouped_by_col = inputDF.select('name', 'longitude')
    # 4. Operation T3: We remove the extra column generated by the group aggregation command
    distinct_df = grouped_by_col.distinct()
    # 5. Operation T3: We remove the extra column generated by the group aggregation command
    ordered_rows = distinct_df.orderBy('longitude', ascending=False)
    # 6. Operation A1: We collect the rows
    collected_rows = ordered_rows.collect()
    # 7. We print the result
    print('\n------------------------------')
    print('Exercise 4:')
    print('------------------------------\n')
    for row in collected_rows:
      print(row)

# ------------------------------------------
# FUNCTION ex5
# ------------------------------------------
def ex5(spark, my_dataset_dir):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("status", pyspark.sql.types.IntegerType(), True),
         pyspark.sql.types.StructField("name", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("longitude", pyspark.sql.types.FloatType(), True),
         pyspark.sql.types.StructField("latitude", pyspark.sql.types.FloatType(), True),
         pyspark.sql.types.StructField("dateStatus", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("bikesAvailable", pyspark.sql.types.IntegerType(), True),
         pyspark.sql.types.StructField("docksAvailable", pyspark.sql.types.IntegerType(), True)
         ])

    # 2. Operation C2: We create the DataFrame from the dataset and the schema
    inputDF = spark.read.format("csv") \
        .option("delimiter", ";") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)

    # 3. Operation T2: We filter the bikes of "Kent Station"
    filtered_df = inputDF.filter("name == 'Kent Station'")
    # 4. Operation T3: We project just the info we are interested into
    required_columns = filtered_df.select("name", "bikesAvailable")
    # 5. Operation T4: We group the entries by the name and compute the average amount of bikes available
    result = required_columns.groupBy('name').agg(func.avg('bikesAvailable'))
    # 6. Operation A1: We collect the rows
    collected_rows = result.collect()
    # 7. We print the result
    print('\n------------------------------')
    print('Exercise 5:')
    print('------------------------------\n')
    for row in collected_rows:
      print(row)

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark, my_dataset_dir, option):
    # Exercise 1: Total amount of entries in the dataset.
    if option == 1:
        ex1(spark, my_dataset_dir)

    # Exercise 2: Number of Coca-cola bikes stations in Cork.
    if option == 2:
        ex2(spark, my_dataset_dir)

        # Exercise 3: List of Coca-Cola bike stations.
    if option == 3:
        ex3(spark, my_dataset_dir)

    # Exercise 4: Sort the bike stations by their longitude (East to West).
    if option == 4:
        ex4(spark, my_dataset_dir)

    # Exercise 5: Average number of bikes available at Kent Station.
    if option == 5:
        ex5(spark, my_dataset_dir)


# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We use as many input arguments as needed
    option = 5

    # 2. Local or Databricks
    local_False_databricks_True = True

    # 3. We set the path to my_dataset and my_result
    my_local_path = "/home/nacho/CIT/Tools/MyCode/Spark/"
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/Assignment1Dataset/"

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
