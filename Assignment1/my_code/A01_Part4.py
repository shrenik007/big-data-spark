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
import pyspark.sql.types
import pyspark.sql.functions as func
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import *
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

    #  filter data when bikesAvailable is 0 and status is 0
    filtered_DF = inputDF.filter("bikesAvailable=0").filter("status=0")
    
    # grouping dataframe by name
    group_by_DF = filtered_DF.groupBy('name')
    
    # counting each group
    count_DF = group_by_DF.count()
    
    # ordering in descending order
    ordered_DF = count_DF.orderBy('count', ascending=False)
    
    # collecting the required output
    collected_DF = ordered_DF.collect()
    
    # printing output
    print('------------------------------')
    print('Exercise 1:')
    print('------------------------------\n')
    print(len(collected_DF))
    for row in collected_DF:
      print(row)
    

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
        .load(my_dataset_dir+'bikeMon_20170827.csv')
    
    # spliting dateStatus column 
    split_col = pyspark.sql.functions.split(inputDF['dateStatus'], ' ')    
    
    # creating new row 'hour' by taking substring of dateStatus column
    new_col_DF = inputDF.withColumn('hour', substring(split_col.getItem(1), 0, 2))
    order_by_names = new_col_DF.orderBy('name', 'hour')
    
    # grouping dataframe by name and hour and calculating average of each group
    group_by_DF = order_by_names.groupBy('name', 'hour').agg(avg('bikesAvailable'))
    # collecting the required output
    collected_DF = group_by_DF.collect()
    # printing output
    print('------------------------------')
    print('Exercise 2:')
    print('------------------------------\n')
    for row in collected_DF:
      print(row)

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
        .load(my_dataset_dir+'bikeMon_20170827.csv').filter("status == 0")
    # spliting dateStatus column and taking only time from it
    split_col = pyspark.sql.functions.split(inputDF['dateStatus'], ' ')
    new_col_DF = inputDF.withColumn('time', substring(split_col.getItem(1), 0, 5))
    
    # again spliting time column and taking only hour from it
    split_time = pyspark.sql.functions.split(new_col_DF['time'], ':') 
    new_col_DF = new_col_DF.withColumn('hour', split_time.getItem(0))
    
    #ordering the resulting DF by time
    order_by_names = new_col_DF.orderBy('time')
    # creating partitions by name
    my_window = Window.partitionBy('name').orderBy('time', 'name')
    # adding new column for storing previous bikesAvailable
    df = order_by_names.withColumn("prev_bikesAvailable", F.lag(order_by_names.bikesAvailable).over(my_window))
    # adding new column for storing previous station name
    df = df.withColumn("prev_name", F.lag(order_by_names.name).over(my_window))
    # adding new column to add previous hour
    df = df.withColumn("prev_hour", F.lag(df.hour).over(my_window))    
    # ordering by name and time
    required_columns = df.orderBy('name', 'time')
    # filtering based on required output
    final_result = required_columns.filter("bikesAvailable==0 and ((prev_hour is null or prev_hour != hour) or (name == prev_name)) and (prev_bikesAvailable is null or prev_bikesAvailable != bikesAvailable)")
    # selecting required columns and ordering by time
    required_DF = final_result.select("name", "time").orderBy("time")
    # we collect output
    collected_DF = required_DF.collect()
    # printing output
    print('------------------------------')
    print('Exercise 3:')
    print('------------------------------\n')
    for row in collected_DF:
      print(row)


# ------------------------------------------
# FUNCTION ex4
# ------------------------------------------
def ex4(spark, my_dataset_dir, ran_out_times):
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
        .load(my_dataset_dir+'bikeMon_20170827.csv')
    #  spliting dateStatus column and creating new column for storing time
    split_col = pyspark.sql.functions.split(inputDF['dateStatus'], ' ')
    new_col_DF = inputDF.withColumn('time', split_col.getItem(1))
    # selecting required columns and ordering by time
    ordered_by_time_DF = new_col_DF.select("status", "name", "dateStatus", "bikesAvailable", "time").orderBy("time")
    # filter the DF by the list of ran__out_times given
    filtered_bikes_avaiblable = ordered_by_time_DF.filter("status == 0 and bikesAvailable > 0").where(col("time").isin(ran_out_times))
    # creating partition on time column and ordering it by time
    my_window1 = Window.partitionBy('time').orderBy('time')
    # creating new column 'maxCol' to store max value from each partition and putting a condition that maxCol should equal to bikesAvailable and then finally drop the new column
    df = filtered_bikes_avaiblable.withColumn('maxCol', max('bikesAvailable').over(my_window1)).where(col('maxCol') == col('bikesAvailable')).drop("maxCol")
    df = df.withColumn("prev_time", F.lag(df.time).over(my_window1))
    # selecting required column and filter based on the condition
    required_df = df.filter('prev_time is null').select("name", "dateStatus", "bikesAvailable")
    # we collect output
    collected_rows = required_df.collect()
    # printing output
    print('------------------------------')
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

    # 2. Operation C1: We create the DataFrame from the dataset and the schema
    inputDF = spark.read.format("csv") \
        .option("delimiter", ";") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)
    #  spliting dateStatus column and creating new column for storing date
    split_col = pyspark.sql.functions.split(inputDF['dateStatus'], ' ')
    inputDF = inputDF.withColumn("date", split_col.getItem(0))
    # filtering the DF and selecting required columns
    inputDF = inputDF.filter("status == 0").select("name", "dateStatus", "date", "bikesAvailable")
    # creating a partion on name and date together and ordering by name and dateStatus
    my_window1 = Window.partitionBy("name", "date").orderBy("name", "dateStatus")
    # creating new column prev_bikesAvailable to store previous row's bikesAvailable entry for each partition
    new_col_DF = inputDF.withColumn("prev_bikesAvailable", F.lag(inputDF.bikesAvailable).over(my_window1))
    # creating new column bikes_taken to store whether on the current row a bike has been taken by someone or not
    new_col_DF = new_col_DF.withColumn("bikes_taken", when(col("prev_bikesAvailable").isNull(), 0)
                                                     .otherwise(when(col("prev_bikesAvailable") > col("bikesAvailable"), col("prev_bikesAvailable") - col("bikesAvailable"))
                                                               .otherwise(0)))
    # creating new column bikes_given to store whether on the current row a bike has been given by someone or not
    new_col_DF = new_col_DF.withColumn("bikes_given", when(col("prev_bikesAvailable").isNull(), 0)
                                                      .otherwise(when(col("prev_bikesAvailable") < col("bikesAvailable"), col("bikesAvailable") - col("prev_bikesAvailable"))
                                                                 .otherwise(0)))
    # selecting required rows
    filtered_DF = new_col_DF.select("name", "dateStatus", "bikes_taken", "bikes_given")
    # calculating sum of bikes_taken and bikes_given column 
    total_bikes_taken = new_col_DF.groupBy("name").agg(sum(new_col_DF.bikes_taken), sum(new_col_DF.bikes_given)).withColumnRenamed("sum(bikes_taken)", "bikesTaken").withColumnRenamed("sum(bikes_given)", "bikesGiven")
    # sorting by bikesTaken and bikesGiven together
    required_DF = total_bikes_taken.orderBy("bikesTaken", "bikesGiven", ascending=False).collect()
    # printing output
    print('------------------------------')
    print('Exercise 5:')
    print('------------------------------\n')
    for row in required_DF:
      print(row)
    

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark, my_dataset_dir, option, ran_out_times):
    # Exercise 1: Number of times each station ran out of bikes (sorted decreasingly by station).
    if option == 1:
        ex1(spark, my_dataset_dir)

  # Exercise 2: Pick one busy day with plenty of ran outs -> Sunday 28th August 2017
    #             Average amount of bikes per station and hour window (e.g. [9am, 10am), [10am, 11am), etc. )
    if option == 2:
        ex2(spark, my_dataset_dir)

    # Exercise 3: Pick one busy day with plenty of ran outs -> Sunday 28th August 2017
    #             Get the different ran-outs to attend.
    #             Note: n consecutive measurements of a station being ran-out of bikes has to be considered a single ran-out,
    #                   that should have been attended when the ran-out happened in the first time.
    if option == 3:
        ex3(spark, my_dataset_dir)

    # Exercise 4: Pick one busy day with plenty of ran outs -> Sunday 28th August 2017
    #             Get the station with biggest number of bikes for each ran-out to be attended.
    if option == 4:
        ex4(spark, my_dataset_dir, ran_out_times)

    # Exercise 5: Total number of bikes that are taken and given back per station (sorted decreasingly by the amount of bikes).
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
    option = 1

    ran_out_times = ["06:03:00", "06:03:00", "08:58:00", "09:28:00", "10:58:00", "12:18:00",
                     '12:43:00', '12:43:00', '13:03:00', '13:53:00', '14:28:00', '14:28:00',
                     '15:48:00', '16:23:00', '16:33:00', '16:38:00', '17:09:00', '17:29:00',
                     '18:24:00', '19:34:00', '20:04:00', '20:14:00', '20:24:00', '20:49:00',
                     '20:59:00', '22:19:00', '22:59:00', '23:14:00', '23:44:00'
                     ]

    # 2. Local or Databricks
    local_False_databricks_True = True

    # 3. We set the path to my_dataset and my_result
    my_local_path = "/home/nacho/CIT/Tools/MyCode/Spark/"
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/Assignment1Dataset/"

    if not local_False_databricks_True:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    my_main(spark, my_dataset_dir, option, ran_out_times)
