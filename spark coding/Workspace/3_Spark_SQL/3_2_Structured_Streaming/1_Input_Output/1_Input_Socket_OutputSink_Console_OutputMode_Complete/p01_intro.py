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
# FUNCTION my_model
# ------------------------------------------
def my_model(spark, time_step_interval):
    # 1. We create the DataStreamWritter
    myDSW = None

    # 2. Operation C1: We create an Streaming DataFrame representing the stream of input lines from connection to localhost:9999
    inputSDF = spark.readStream\
                    .format("socket")\
                    .option("host", "localhost")\
                    .option("port", 9999)\
                    .load()

    # 3. Operation T1: We split the String by words
    wordListSDF = inputSDF.withColumn("wordList", pyspark.sql.functions.split(inputSDF["value"], " "))\
                          .drop("value")

    # 4. Operation T2: We explode the words separately
    wordsSDF = wordListSDF.withColumn("word", pyspark.sql.functions.explode(wordListSDF["wordList"]))\
                          .drop("wordList")

    # 6. Operation T3: We add the watermark on my_time
    solutionSDF = wordsSDF.groupBy(wordsSDF["word"])\
                          .count()


    # 5. We set the frequency for the time steps
    my_frequency = str(time_step_interval) + " seconds"

    # 7. Operation O1: We create the DataStreamWritter, to print by console the results in complete mode
    myDSW = solutionSDF.writeStream\
                       .format("console") \
                       .trigger(processingTime=my_frequency) \
                       .outputMode("complete")

    # 8. We return the DataStreamWritter
    return myDSW

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark, time_step_interval):
    # 1. We get the DataStreamWriter object derived from the model
    dsw = my_model(spark, time_step_interval)

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
    # 1. We specify the time interval each new generated batch is processed.
    time_step_interval = 15

    # 2. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    # 3. We run my_main
    my_main(spark, time_step_interval)
