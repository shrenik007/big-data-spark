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

import pyspark.mllib.stat

# ------------------------------------------
# FUNCTION process_line
# ------------------------------------------
def process_line(line):
    # 1. We create the output variable
    res = ()

    # 2. We remove the end of line character
    line = line.replace("\n", "")

    # 3. We split the line by tabulator characters
    params = line.split(";")

    # 4. We turn it into a tuple of integers
    size = len(params)
    for index in range(size):
        if (params[index] != ""):
            params[index] = int(params[index])
        else:
            params[index] = 0
    res = tuple(params)

    # 5. We return res
    return res


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, my_dataset_dir, correlation_method):
    # 1. Operation C1: We create the RDD from the dataset
    inputRDD = sc.textFile(my_dataset_dir)

    # 2. Operation T1: We get an RDD of tuples
    infoRDD = inputRDD.map(process_line)

    # 3. Operation A1: We get the correlation matrix
    correlation_matrix = pyspark.mllib.stat.Statistics.corr(infoRDD, method=correlation_method)

    # 4. We display the result
    print(correlation_matrix)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We use as many input arguments as needed
    correlation_method = "pearson"
    #correlation_method = "spearman"

    #dataset_file_name = "pearson_2_vars_dataset.csv"
    dataset_file_name = "pearson_3_vars_dataset.csv"
    #dataset_file_name = "spearman_2_vars_dataset.csv"

    # 2. Local or Databricks
    local_False_databricks_True = True

    # 3. We set the path to my_dataset and my_result
    my_local_path = "/home/nacho/CIT/Tools/MyCode/Spark/"
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/5_Spark_MachineLearning/1_Basic_Statistics/" + dataset_file_name

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    my_main(sc, my_dataset_dir, correlation_method)
