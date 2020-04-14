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
import pyspark.sql.types

import time
import datetime
import os
import shutil

# ------------------------------------------
# FUNCTION my_get_index
# ------------------------------------------
def my_get_first(my_list, index):
    # 1. We create the output variable
    res = str(my_list[index])

    # 2. We return res
    return res

# ------------------------------------------
# FUNCTION my_model
# ------------------------------------------
def my_model(spark, monitoring_dir, time_step_interval, my_timestamp):
    # 1. We create the DataStreamWritter
    myDSW = None

    # 2. Operation C1: We create the DataFrame from the dataset and the schema

    # 2.1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType([pyspark.sql.types.StructField("sentence", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("number", pyspark.sql.types.IntegerType(), True),
                                              pyspark.sql.types.StructField("bool_val", pyspark.sql.types.BooleanType(), True)
                                             ]
                                            )

    # 2.2. We use it when loading the dataset
    inputSDF = spark.readStream.format("csv") \
                               .option("delimiter", ";") \
                               .option("quote", "") \
                               .option("header", "false") \
                               .schema(my_schema) \
                               .load(monitoring_dir)

    # 3. Operation T1: We add the fixed timestamp at the beginning of the program.
    # Note that the data arriving in Time Steps 1, 2 and so on will still have the timestamp of Time Step 0.
    # This is the way we are simulating the arrival of late data.
    time_inputSDF = inputSDF.withColumn("my_time", pyspark.sql.functions.lit(my_timestamp).cast(pyspark.sql.types.TimestampType())) \
                            .withWatermark("my_time", "100 seconds")

    # 4. Operation T2: We split the String by words
    wordListSDF = time_inputSDF.withColumn("wordList", pyspark.sql.functions.split(time_inputSDF["sentence"], " "))\
                               .drop("sentence")\
                               .drop("number")\
                               .drop("bool_val")

    # 5. Operation T3: We explode the words separately
    wordsSDF = wordListSDF.withColumn("word", pyspark.sql.functions.explode(wordListSDF["wordList"]))\
                          .drop("wordList")

    # 6. We set the frequency for the time steps
    my_frequency = str(time_step_interval) + " seconds"

    # 7. Operation T4: We add the watermark on my_time
    windowSDF = wordsSDF.groupBy(pyspark.sql.functions.window("my_time", my_frequency, my_frequency),
                                 wordsSDF["word"]
                                )\
                        .count()

    # 8. Operation T5: We add the new columns for making the window more clear

    # 8.1. We define the UDF function we will use to get the elements of the list
    my_get_firstUDF = pyspark.sql.functions.udf(my_get_first, pyspark.sql.types.StringType())

    # 8.2. We apply the UDF
    solutionSDF = windowSDF.withColumn("window_start", my_get_firstUDF(windowSDF["window"], pyspark.sql.functions.lit(0))) \
                           .withColumn("window_end", my_get_firstUDF(windowSDF["window"], pyspark.sql.functions.lit(1))) \
                           .drop("window")

    # 9. Operation T6: We sort them by the starting time of the window
    #solutionSDF = formattedSDF.orderBy(formattedSDF["window_start"].asc())

    # 10. Operation O1: We create the DataStreamWritter, to print by console the results in complete mode
    myDSW = solutionSDF.writeStream\
                       .format("console") \
                       .trigger(processingTime=my_frequency) \
                       .outputMode("append")

    # 11. We return the DataStreamWritter
    return myDSW

# ------------------------------------------
# FUNCTION get_source_dir_file_names
# ------------------------------------------
def get_source_dir_file_names(local_False_databricks_True, source_dir, verbose):
    # 1. We create the output variable
    res = []

    # 2. We get the FileInfo representation of the files of source_dir
    fileInfo_objects = []
    if local_False_databricks_True == False:
        fileInfo_objects = os.listdir(source_dir)
    else:
        fileInfo_objects = dbutils.fs.ls(source_dir)

    # 3. We traverse the fileInfo objects, to get the name of each file
    for item in fileInfo_objects:
        # 3.1. We get a string representation of the fileInfo
        file_name = str(item)

        # 3.2. If the file is processed in DBFS
        if local_False_databricks_True == True:
            # 3.2.1. We look for the pattern name= to remove all useless info from the start
            lb_index = file_name.index("name='")
            file_name = file_name[(lb_index + 6):]

            # 3.2.2. We look for the pattern ') to remove all useless info from the end
            ub_index = file_name.index("',")
            file_name = file_name[:ub_index]

        # 3.3. We append the name to the list
        res.append(file_name)
        if verbose == True:
            print(file_name)

    # 4. We sort the list in alphabetic order
    res.sort()

    # 5. We return res
    return res

# ------------------------------------------
# FUNCTION streaming_simulation
# ------------------------------------------
def streaming_simulation(local_False_databricks_True, source_dir, monitoring_dir, time_step_interval, verbose):
    # 1. We get the names of the files on source_dir
    files = get_source_dir_file_names(local_False_databricks_True, source_dir, verbose)

    # 2. We get the starting time of the process
    time.sleep(time_step_interval * 0.01)

    start = time.time()

    # 2.1. If verbose mode, we inform of the starting time
    if (verbose == True):
        print("Start time = " + str(start))

    # 3. We set a counter in the amount of files being transferred
    count = 0

    # 4. We simulate the dynamic arriving of such these files from source_dir to dataset_dir
    # (i.e, the files are moved one by one for each time period, simulating their generation).
    for file in files:
        # 4.1. We copy the file from source_dir to dataset_dir#
        if local_False_databricks_True == False:
            shutil.copyfile(source_dir + file, monitoring_dir + file)
        else:
            dbutils.fs.cp(source_dir + file, monitoring_dir + file)

        # 4.2. We increase the counter, as we have transferred a new file
        count = count + 1

        # 4.3. If verbose mode, we inform from such transferrence and the current time.
        if (verbose == True):
            print("File " + str(count) + " transferred. Time since start = " + str(time.time() - start))

        # 4.3. We rename

        # 4.4. We wait the desired transfer_interval until next time slot.
        time.sleep((start + (count * time_step_interval)) - time.time())

    # 5. We wait a last time_step_interval
    time.sleep(time_step_interval)

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark,
            local_False_databricks_True,
            source_dir,
            monitoring_dir,
            time_step_interval,
            verbose,
            my_timestamp
           ):

    # 1. We get the DataStreamWriter object derived from the model
    dsw = my_model(spark, monitoring_dir, time_step_interval, my_timestamp)

    # 2. We get the StreamingQuery object derived from starting the DataStreamWriter
    ssq = dsw.start()

    # 3. We simulate the streaming arrival of files (i.e., one by one) from source_dir to monitoring_dir
    streaming_simulation(local_False_databricks_True, source_dir, monitoring_dir, time_step_interval, verbose)

    # 4. We set the await termination to 1 time_step_interval
    ssq.awaitTermination(time_step_interval)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. Local or Databricks
    local_False_databricks_True = True

    # 2. We set the path to my_dataset and my_result
    my_local_path = "/home/nacho/CIT/Tools/MyCode/Spark/"
    my_databricks_path = "/"

    source_dir = "FileStore/tables/3_Spark_SQL/3_2_Structured_Streaming/my_dataset/"
    monitoring_dir = "FileStore/tables/3_Spark_SQL/3_2_Structured_Streaming/my_monitoring/"

    if local_False_databricks_True == False:
        source_dir = my_local_path + source_dir
        monitoring_dir = my_local_path + monitoring_dir
    else:
        source_dir = my_databricks_path + source_dir
        monitoring_dir = my_databricks_path + monitoring_dir

    # 3. We set the Spark Structured Streaming parameters

    # 3.1. We specify the time interval each of our micro-batches (files) appear for its processing.
    time_step_interval = 20

    # 3.2. We specify the number of micro-batches (i.e., files) of our dataset.
    dataset_micro_batches = 6

    # 3.3. We configure verbosity during the program run
    verbose = False

    # 3.4. We get the current timestamp at the beginning of the program, to assign it to each Row of a DataFrame
    timestamp_in_milliseconds = time.time()
    my_timestamp = datetime.datetime.fromtimestamp(timestamp_in_milliseconds).strftime('%Y-%m-%d %H:%M:%S')



    # 4. We remove the directories
    if local_False_databricks_True == False:
        # 4.1. We remove the monitoring_dir
        if os.path.exists(monitoring_dir):
            shutil.rmtree(monitoring_dir)
    else:
        # 4.1. We remove the monitoring_dir
        dbutils.fs.rm(monitoring_dir, True)

    # 5. We re-create the directories again
    if local_False_databricks_True == False:
        # 5.1. We re-create the monitoring_dir
        os.mkdir(monitoring_dir)
    else:
        # 5.1. We re-create the monitoring_dir
        dbutils.fs.mkdirs(monitoring_dir)

    # 6. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    # 7. We run my_main
    my_main(spark,
            local_False_databricks_True,
            source_dir,
            monitoring_dir,
            time_step_interval,
            verbose,
            my_timestamp
           )
