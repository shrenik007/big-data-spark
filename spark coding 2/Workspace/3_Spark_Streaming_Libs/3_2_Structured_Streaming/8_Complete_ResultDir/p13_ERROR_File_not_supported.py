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

import os
import shutil
import time

# ------------------------------------------
# FUNCTION my_model
# ------------------------------------------
def my_model(spark, monitoring_dir, checkpoint_dir, result_dir, time_step_interval):
    # 1. We create the DataStreamWritter
    myDSW = None

    # 2. We set the frequency for the time steps
    my_frequency = str(time_step_interval) + " seconds"

    # 3. Operation C1: We create the DataFrame from the dataset and the schema

    # 3.1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType([pyspark.sql.types.StructField("value", pyspark.sql.types.StringType(), True)])

    # 3.2. We use it when loading the dataset
    inputSDF = spark.readStream.format("csv") \
                               .option("header", "false") \
                               .schema(my_schema) \
                               .load(monitoring_dir)

    # 4. Operation T1: We split the String by words
    word_listSDF = inputSDF.withColumn("wordList", pyspark.sql.functions.split(pyspark.sql.functions.col("value"), " ")) \
                           .drop("value")

    # 5. Operation T2: We explode the words separately
    wordsSDF = word_listSDF.withColumn("word", pyspark.sql.functions.explode(pyspark.sql.functions.col("wordList"))) \
                           .drop("wordList")

    # 6. Operation T3: We add the watermark on my_time
    aggSDF = wordsSDF.groupBy(pyspark.sql.functions.col("word")) \
                     .count()

    # 7. Operation T6: We sort them by the starting time of the window
    solutionSDF = aggSDF.orderBy(pyspark.sql.functions.col("count").desc())

    # 8. We create the DataStreamWritter
    myDSW = solutionSDF.writeStream\
                       .format("csv")\
                       .option("delimiter", ";")\
                       .option("path", result_dir) \
                       .option("checkpointLocation", checkpoint_dir) \
                       .trigger(processingTime=my_frequency) \
                       .outputMode("complete")

    # 9. We return the DataStreamWritter
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
def streaming_simulation(local_False_databricks_True,
                         source_dir,
                         monitoring_dir,
                         time_step_interval,
                         verbose,
                         dataset_file_names
                        ):

    # 1. We check what time is it
    start = time.time()

    # 2. We set a counter in the amount of files being transferred
    count = 0

    # 3. If verbose mode, we inform of the starting time
    if (verbose == True):
        print("Start time = " + str(start))

    # 4. We transfer the files to simulate their streaming arrival.
    for file in dataset_file_names:
        # 4.1. We copy the file from source_dir to dataset_dir
        if local_False_databricks_True == False:
            shutil.copyfile(source_dir + file, monitoring_dir + file)
        else:
            dbutils.fs.cp(source_dir + file, monitoring_dir + file)

        # 4.2. If verbose mode, we inform from such transferrence and the current time.
        if (verbose == True):
            print("File " + str(count) + " transferred. Time since start = " + str(time.time() - start))

        # 4.3. We increase the counter, as we have transferred a new file
        count = count + 1

        # 4.4. We wait the desired transfer_interval until next time slot.
        time_to_wait = (start + (count * time_step_interval)) - time.time()
        if (time_to_wait > 0):
            time.sleep(time_to_wait)

    # 5. We wait for another time_interval
    time.sleep(time_step_interval)

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark,
            local_False_databricks_True,
            source_dir,
            monitoring_dir,
            checkpoint_dir,
            result_dir,
            time_step_interval,
            verbose
           ):

    # 1. We get the names of the files of our dataset
    dataset_file_names = get_source_dir_file_names(local_False_databricks_True, source_dir, verbose)

    # 2. We get the DataStreamWriter object derived from the model
    dsw = my_model(spark, monitoring_dir, checkpoint_dir, result_dir, time_step_interval)

    # 3. We get the StreamingQuery object derived from starting the DataStreamWriter
    ssq = dsw.start()

    # 4. We simulate the streaming arrival of files (i.e., one by one) from source_dir to monitoring_dir
    streaming_simulation(local_False_databricks_True,
                         source_dir,
                         monitoring_dir,
                         time_step_interval,
                         verbose,
                         dataset_file_names
                        )

    # 5. We stop the StreamingQuery object
    ssq.stop()

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We use as many input arguments as needed

    # 1.1. We specify the time interval each of our micro-batches (files) appear for its processing.
    time_step_interval = 15

    # 1.2. We configure verbosity during the program run
    verbose = False

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    my_local_path = "/home/nacho/CIT/Tools/MyCode/Spark/"
    my_databricks_path = "/"

    source_dir = "FileStore/tables/3_Spark_Streaming_Libs/3_2_Structured_Streaming/my_dataset/"
    monitoring_dir = "FileStore/tables/3_Spark_Streaming_Libs/3_2_Structured_Streaming/my_monitoring/"
    checkpoint_dir = "FileStore/tables/3_Spark_Streaming_Libs/3_2_Structured_Streaming/my_checkpoint/"
    result_dir = "FileStore/tables/3_Spark_Streaming_Libs/3_2_Structured_Streaming/my_result/"

    if local_False_databricks_True == False:
        source_dir = my_local_path + source_dir
        monitoring_dir = my_local_path + monitoring_dir
        checkpoint_dir = my_local_path + checkpoint_dir
        result_dir = my_local_path + result_dir
    else:
        source_dir = my_databricks_path + source_dir
        monitoring_dir = my_databricks_path + monitoring_dir
        checkpoint_dir = my_databricks_path + checkpoint_dir
        result_dir = my_databricks_path + result_dir

    # 4. We remove the directories
    if local_False_databricks_True == False:
        # 4.1. We remove the monitoring_dir
        if os.path.exists(monitoring_dir):
            shutil.rmtree(monitoring_dir)

        # 4.2. We remove the result_dir
        if os.path.exists(result_dir):
            shutil.rmtree(result_dir)

        # 4.3. We remove the checkpoint_dir
        if os.path.exists(checkpoint_dir):
            shutil.rmtree(checkpoint_dir)
    else:
        # 4.1. We remove the monitoring_dir
        dbutils.fs.rm(monitoring_dir, True)

        # 4.2. We remove the result_dir
        dbutils.fs.rm(result_dir, True)

        # 4.3. We remove the checkpoint_dir
        dbutils.fs.rm(checkpoint_dir, True)

    # 5. We re-create the directories again
    if local_False_databricks_True == False:
        # 5.1. We re-create the monitoring_dir
        os.mkdir(monitoring_dir)

        # 5.2. We re-create the result_dir
        os.mkdir(result_dir)

        # 5.3. We re-create the checkpoint_dir
        os.mkdir(checkpoint_dir)
    else:
        # 5.1. We re-create the monitoring_dir
        dbutils.fs.mkdirs(monitoring_dir)

        # 5.2. We re-create the result_dir
        dbutils.fs.mkdirs(result_dir)

        # 5.3. We re-create the checkpoint_dir
        dbutils.fs.mkdirs(checkpoint_dir)

    # 6. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    # 7. We run my_main
    my_main(spark,
            local_False_databricks_True,
            source_dir,
            monitoring_dir,
            checkpoint_dir,
            result_dir,
            time_step_interval,
            verbose
           )
