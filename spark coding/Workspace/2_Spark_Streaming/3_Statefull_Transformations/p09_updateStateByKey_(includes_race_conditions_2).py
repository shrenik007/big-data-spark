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
import pyspark.streaming

import os
import shutil
import time


# ------------------------------------------
# FUNCTION process_line
# ------------------------------------------
def process_line(line, bad_chars):
    # 1. We create the output variable
    res = []

    # 2. We clean the line by removing the bad characters
    for c in bad_chars:
        line = line.replace(c, '')

    # 3. We clean the line by removing each tabulator and set of white spaces
    line = line.replace('\t', ' ')
    line = line.replace('  ', ' ')
    line = line.replace('   ', ' ')
    line = line.replace('    ', ' ')

    # 4. We clean the line by removing any initial and final white spaces
    line = line.strip()
    line = line.rstrip()

    # 5. We split the line by words
    words = line.split(" ")

    # 6. We append each valid word to the list
    for word in words:
        if (word != ''):
            if ((ord(word[0]) > 57) or (ord(word[0]) < 48)):
                res.append(word)

    # 7. We return res
    return res


# ------------------------------------------
# FUNCTION my_state_update
# ------------------------------------------
def my_state_update(events_list, current_state):
    # 1. We create the output variable
    res = None

    # 2. If this is the first time we find the key, we initialise it
    if current_state is None:
        current_state = 0

    # 3. We update the state
    res = sum(events_list) + current_state

    # 4. We return res
    return res


# ------------------------------------------
# FUNCTION my_model
# ------------------------------------------
def my_model(ssc, monitoring_dir, result_dir, time_step_interval, window_duration, sliding_duration, bad_chars):
    # 1. Operation C1: Creation 'textFileStream', so as to store the novel content of monitoring_dir for this time step into a new RDD within DStream.
    inputDStream = ssc.textFileStream(monitoring_dir)

    # 2. Operation T1: Transformation 'flatMap', so as to get a new DStream where each underlying RDD contains all the words of its equivalent
    # RDD in inputDStream.
    allWordsDStream = inputDStream.flatMap(lambda x: process_line(x, bad_chars))

    # 3. Operation T2: Transformation 'map', so as to get a new DStream where each underlying RDD contains pair items, versus the single String items of
    # its equivalent RDD in allWordsDStream.
    pairWordsDStream = allWordsDStream.map(lambda x: (x, 1))

    # 4. Opetion T3: Transformation updateStateByKey, to get the increased amount of words appearing.
    # updateStateByKey allows to maintain state across the batches in a DStream by providing access to
    # a state variable for DStreams of key/value pairs. Given a DStream of (key, event) pairs, it lets you construct a new DStream
    # of (key, state) pairs by taking a function that specifies how to update the state for each key given new events.

    # The single argument is a function F on how to update each state given new events.
    # The first argument of F is the list of events that arrived in the current micro-batch (the one of this time step).
    # Please note the micro-batch may be empty if no event has arrived in this time step.
    # The second argument of F is the current state. Please note the state may be 'None' if there was no previous state for the key
    # (i.e., if no previous events with that key have happened).
    # Finally, the function returns the updated state after processing the new events.

    solutionDStream = pairWordsDStream.updateStateByKey(my_state_update)

    # Super interesting topic: RACE CONDITIONS again!
    # Please note if a step does not process anything (i.e., receives an empty RDD),
    # then the current state is still updated (actually by an idle update, as it left in the same state it was).
    # But, this is relevant to us and the way we are collecting the results, as we will still receive a solution, and mark the directory
    # as a valid step. Even if this is not ideal, honestly there is not much we can do about it.

    # 6. Operation P1: We persist solutionDStream, as we are going to use it in more than 1 output operation.
    solutionDStream.cache()

    # 7. Operation OO1: Output Operation pprint() so that we print the content of solutionDStream
    solutionDStream.pprint()

    # 8. Operation OO2: Output Operation saveAsTextFiles so as to Store the DStream solutionDStream into the desired folder from the DBFS.
    solutionDStream.saveAsTextFiles(result_dir)


# ------------------------------------------
# FUNCTION create_ssc
# ------------------------------------------
def create_ssc(sc, monitoring_dir, result_dir, max_micro_batches, time_step_interval, window_duration, sliding_duration,
               bad_chars):
    # 1. We create the new Spark Streaming context.
    # This is the main entry point for streaming functionality. It requires two parameters:
    # (*) The underlying SparkContext that it will use to process the data.
    # (**) A batch interval, specifying how often it will check for the arrival of new data,
    # so as to process it.
    ssc = pyspark.streaming.StreamingContext(sc, time_step_interval)

    # 2. We configure the maximum amount of time the data is retained.
    # Think of it: If you have a SparkStreaming operating 24/7, the amount of data it is processing will
    # only grow. This is simply unaffordable!
    # Thus, this parameter sets maximum time duration past arrived data is still retained for:
    # Either being processed for first time.
    # Being processed again, for aggregation with new data.
    # After the timeout, the data is just released for garbage collection.

    # We set this to the maximum amount of micro-batches we allow before considering data
    # old and dumping it times the time_step_interval (in which each of these micro-batches will arrive).
    ssc.remember(max_micro_batches * time_step_interval)

    # 3. We model the ssc.
    # This is the main function of the Spark application:
    # On it we specify what do we want the SparkStreaming context to do once it receives data
    # (i.e., the full set of transformations and ouptut operations we want it to perform).
    my_model(ssc, monitoring_dir, result_dir, time_step_interval, window_duration, sliding_duration, bad_chars)

    # 4. We return the ssc configured and modelled.
    return ssc


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
    time.sleep(time_step_interval * 0.1)

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

            # 4.4. We wait the desired transfer_interval until next time slot.
        time.sleep((start + (count * time_step_interval)) - time.time())


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc,
            local_False_databricks_True,
            source_dir,
            monitoring_dir,
            checkpoint_dir,
            result_dir,
            max_micro_batches,
            time_step_interval,
            verbose,
            window_duration,
            sliding_duration,
            race_conditions_extra_delay,
            bad_chars
            ):
    # 1. We setup the Spark Streaming context
    # This sets up the computation that will be done when the system receives data.
    ssc = pyspark.streaming.StreamingContext.getActiveOrCreate(checkpoint_dir,
                                                               lambda: create_ssc(sc,
                                                                                  monitoring_dir,
                                                                                  result_dir,
                                                                                  max_micro_batches,
                                                                                  time_step_interval,
                                                                                  window_duration,
                                                                                  sliding_duration,
                                                                                  bad_chars
                                                                                  )
                                                               )

    # 2. We start the Spark Streaming Context in the background to start receiving data.
    # Spark Streaming will start scheduling Spark jobs in a separate thread.

    # Very important: Please note a Streaming context can be started only once.
    # Moreover, it must be started only once we have fully specified what do we want it to do
    # when it receives data (i.e., the full set of transformations and ouptut operations we want it
    # to perform).
    ssc.start()

    # 3. As the jobs are done in a separate thread, to keep our application (this thread) from exiting,
    # we need to call awaitTermination to wait for the streaming computation to finish.
    ssc.awaitTerminationOrTimeout(time_step_interval)

    # 4. Super interesting topic: RACE CONDITIONS
    # (1) The time X in which SparkStreaming checks my_monitoring_dir for first time.
    # This leads to SparkStreaming checking at times:
    # [ X (time_step_interval * 0), X (time_step_interval * 1), ..., X (time_step_interval * k)]

    # (2) The time Y in which my_source_dir starts placing the files in my_monitoring_dir.
    # This leads to files being placed at times:
    # [ Y (time_step_interval * 0), Y (time_step_interval * 1), ..., Y (time_step_interval * k)]

    # This has an impact when we use window() with sliding_duration > 1, as it determines the actual windows being evaluated.
    # Let's suppose we have 6 micro-batch and we set windows_duration = 3 and sliding_duration = 2.

    # CASE 1:
    # On it, Y is in the interval [ X (time_step_interval * 1), X (time_step_interval * 2) ]
    # Please note that the case with 0 and 1 is equvalent to any other example with k and k+1 where k is an odd number.
    # In this case we will process:
    # After X (time_step_interval * 1) : Nothing.
    # After X (time_step_interval * 3) : The micro-batches (files) 1 and 2.
    # After X (time_step_interval * 5) : The micro-batches (files) 2, 3 and 4.
    # After X (time_step_interval * 7): The micro-batches (files) 4, 5 and 6.

    # CASE 2:
    # On it, Y is in the interval [ X (time_step_interval * 0), X (time_step_interval * 1) ]
    # Please note that the case with 0 and 1 is equvalent to any other example with k and k+1 where k is even.
    # In this case we will process:
    # After X (time_step_interval * 1) : The micro-batch (file) 1.
    # After X (time_step_interval * 3) : The micro-batches (files) 1, 2 and 3.
    # After X (time_step_interval * 5) : The micro-batches (files) 3, 4 and 5.
    # After X (time_step_interval * 7): The micro-batches (files) 5 and 6.

    # Indeed, both cases are totally ok!
    # But it is important to have this in mind for debuging our program!

    if (race_conditions_extra_delay == True):
        time.sleep((sliding_duration - 1) * time_step_interval)

    # 5. We simulate the streaming arrival of files (i.e., one by one) from source_dir to monitoring_dir.
    streaming_simulation(local_False_databricks_True, source_dir, monitoring_dir, time_step_interval, verbose)

    # 6. Once we have transferred all files and processed them, we are done.
    # Thus, we stop the Spark Streaming Context
    ssc.stop(stopSparkContext=False)

    # 7. Extra security stop command: It acts directly over the Java Virtual Machine,
    # in case the Spark Streaming context was not fully stopped.

    # This is crucial to avoid a Spark application working on the background.
    # For example, Databricks, on its private version, charges per cluster nodes (virtual machines)
    # and hours of computation. If we, unintentionally, leave a Spark application working, we can
    # end up with an unexpected high bill.
    if (not sc._jvm.StreamingContext.getActive().isEmpty()):
        sc._jvm.StreamingContext.getActive().get().stop(False)


# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. Extra input arguments
    bad_chars = ['?', '!', '.', ',', ';', '_', '-', '\'', '|', '--',
                 '(', ')', '[', ']', '{', '}', ':', '&', '\n']

    # 2. Local or Databricks
    local_False_databricks_True = True

    # 3. We set the path to my_dataset and my_result
    my_local_path = "/home/nacho/CIT/Tools/MyCode/Spark/"
    my_databricks_path = "/"

    source_dir = "FileStore/tables/2_Spark_Streaming/my_dataset/"
    monitoring_dir = "FileStore/tables/2_Spark_Streaming/my_monitoring/"
    checkpoint_dir = "FileStore/tables/2_Spark_Streaming/my_checkpoint/"
    result_dir = "FileStore/tables/2_Spark_Streaming/my_result/"

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

    # 4. We set the Spark Streaming parameters

    # 4.1. We specify the number of micro-batches (i.e., files) of our dataset.
    dataset_micro_batches = 6

    # 4.2. We specify the time interval each of our micro-batches (files) appear for its processing.
    time_step_interval = 5

    # 4.3. We specify the maximum amount of micro-batches that we want to allow before considering data
    # old and dumping it.
    max_micro_batches = dataset_micro_batches + 1

    # 4.4. We configure verbosity during the program run
    verbose = False

    # 4.5. window_duration, i.e., how many previous batches of data are considered on each window.
    window_duration = 3

    # 4.6. sliding duration, i.e., how frequently the new DStream computes results.
    sliding_duration = 2

    # 4.7. RACE Conditions: Discussed above. Basically, in which moment of the sliding_window do I want to start.
    # This performs an extra delay at the start of the file transferred to sync SparkContext with file transferrence.
    race_conditions_extra_delay = True

    # 5. We remove the directories
    if local_False_databricks_True == False:
        # 5.1. We remove the monitoring_dir
        if os.path.exists(monitoring_dir):
            shutil.rmtree(monitoring_dir)

        # 5.2. We remove the result_dir
        if os.path.exists(result_dir):
            shutil.rmtree(result_dir)

        # 5.3. We remove the checkpoint_dir
        if os.path.exists(checkpoint_dir):
            shutil.rmtree(checkpoint_dir)
    else:
        # 5.1. We remove the monitoring_dir
        dbutils.fs.rm(monitoring_dir, True)

        # 5.2. We remove the result_dir
        dbutils.fs.rm(result_dir, True)

        # 5.3. We remove the checkpoint_dir
        dbutils.fs.rm(checkpoint_dir, True)

    # 6. We re-create the directories again
    if local_False_databricks_True == False:
        # 6.1. We re-create the monitoring_dir
        os.mkdir(monitoring_dir)

        # 6.2. We re-create the result_dir
        os.mkdir(result_dir)

        # 6.3. We re-create the checkpoint_dir
        os.mkdir(checkpoint_dir)
    else:
        # 6.1. We re-create the monitoring_dir
        dbutils.fs.mkdirs(monitoring_dir)

        # 6.2. We re-create the result_dir
        dbutils.fs.mkdirs(result_dir)

        # 6.3. We re-create the checkpoint_dir
        dbutils.fs.mkdirs(checkpoint_dir)

    # 7. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 9. We call to my_main
    my_main(sc,
            local_False_databricks_True,
            source_dir,
            monitoring_dir,
            checkpoint_dir,
            result_dir,
            max_micro_batches,
            time_step_interval,
            verbose,
            window_duration,
            sliding_duration,
            race_conditions_extra_delay,
            bad_chars
            )

