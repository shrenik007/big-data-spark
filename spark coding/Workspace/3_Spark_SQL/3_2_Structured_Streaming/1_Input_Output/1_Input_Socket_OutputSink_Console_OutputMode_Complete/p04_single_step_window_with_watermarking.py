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
def my_model(spark, time_step_interval, my_timestamp):
    # 1. We create the DataStreamWritter
    myDSW = None

    # 2. Operation C1: We create an Streaming DataFrame representing the stream of input lines from connection to localhost:9999
    inputSDF = spark.readStream\
                    .format("socket")\
                    .option("host", "localhost")\
                    .option("port", 9999)\
                    .load()

    # 3. Operation T1: We add the fixed timestamp at the beginning of the program.
    # Note that the data arriving in Time Steps 1, 2 and so on will still have the timestamp of Time Step 0.
    # This is the way we are simulating the arrival of late data.
    time_inputSDF = inputSDF.withColumn("my_time", pyspark.sql.functions.lit(my_timestamp).cast(pyspark.sql.types.TimestampType())) \
                            .withWatermark("my_time", "60 seconds")

    # 4. Operation T2: We split the String by words
    wordListSDF = time_inputSDF.withColumn("wordList", pyspark.sql.functions.split(inputSDF["value"], " "))\
                               .drop("value")

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
    formattedSDF = windowSDF.withColumn("window_start", my_get_firstUDF(windowSDF["window"], pyspark.sql.functions.lit(0))) \
                            .withColumn("window_end", my_get_firstUDF(windowSDF["window"], pyspark.sql.functions.lit(1))) \
                            .drop("window")

    # 9. Operation T6: We sort them by the starting time of the window
    solutionSDF = formattedSDF.orderBy(formattedSDF["window_start"].asc())

    # 10. Operation O1: We create the DataStreamWritter, to print by console the results in complete mode
    myDSW = solutionSDF.writeStream\
                       .format("console") \
                       .trigger(processingTime=my_frequency) \
                       .outputMode("complete")

    # 11. We return the DataStreamWritter
    return myDSW

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark, time_step_interval, my_timestamp):
    # 1. We get the DataStreamWriter object derived from the model
    dsw = my_model(spark, time_step_interval, my_timestamp)

    # 2. We get the StreamingQuery object derived from starting the DataStreamWriter
    ssq = dsw.start()

    # 3. We wait for the termination of our StreamingQuery or finish it after 1 minute
    ssq.awaitTermination(time_step_interval * 6)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We get the current timestamp at the beginning of the program, to assign it to each Row of a DataFrame
    timestamp_in_milliseconds = time.time()
    my_timestamp = datetime.datetime.fromtimestamp(timestamp_in_milliseconds).strftime('%Y-%m-%d %H:%M:%S')
    print(my_timestamp)

    # 2. We specify the time interval each new generated batch is processed.
    time_step_interval = 15

    # 3. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    # 4. We run my_main
    my_main(spark, time_step_interval, my_timestamp)
