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
from datetime import datetime    

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

    # 4. We assign res

    # ----------
    # Schema:
    # ----------
    # 0 -> station_number
    # 1 -> station_name
    # 2 -> direction
    # 3 -> day_of_week
    # 4 -> date
    # 5 -> query_time
    # 6 -> scheduled_time
    # 7 -> expected_arrival_time

    if (len(params) == 8):
        res = (int(params[0]),
               str(params[1]),
               str(params[2]),
               str(params[3]),
               str(params[4]),
               str(params[5]),
               str(params[6]),
               str(params[7])
               )

    # 5. We return res
    return res

# ------------------------------------------
# FUNCTION ex1
# ------------------------------------------
def ex1(sc, my_dataset_dir):
    input_RDD = sc.textFile(my_dataset_dir) \
                .map(lambda line: process_line(line))
    input_RDD.persist()
    count = input_RDD.count()
    # 3. We print the result
    print('\n------------------------------')
    print('Exercise 1:')
    print('------------------------------\n')
    print(count)

# ------------------------------------------
# FUNCTION ex2
# ------------------------------------------
def ex2(sc, my_dataset_dir, station_number):
    input_RDD = sc.textFile(my_dataset_dir) \
                .map(lambda line: process_line(line))
    input_RDD.persist()
    filtered_data = input_RDD.filter(lambda x: x[0] == station_number)
    filtered_data = filtered_data.map(lambda x: x[4]).distinct()

    count = filtered_data.count()
    # 3. We print the result
    print('\n------------------------------')
    print('Exercise 2:')
    print('------------------------------\n')
    print(count)

# ------------------------------------------
# FUNCTION ex3
# ------------------------------------------
def ex3(sc, my_dataset_dir, station_number):
    input_RDD = sc.textFile(my_dataset_dir) \
                .map(lambda line: process_line(line))
    input_RDD.persist()
    
    filtered_data = input_RDD.filter(lambda x: x[0] == station_number)
    filtered_data = filtered_data.map(lambda x: (datetime.strptime(x[6], '%H:%M:%S') >= datetime.strptime(x[7], '%H:%M:%S'), datetime.strptime(x[7], '%H:%M:%S') > datetime.strptime(x[6], '%H:%M:%S')))
    required_data1 = filtered_data.filter(lambda x: x[0])
    required_data2 = filtered_data.filter(lambda x: x[1])
    
    count1 = required_data1.count()
    count2 = required_data2.count()
    # 3. We print the result
    print('\n------------------------------')
    print('Exercise 3:')
    print('------------------------------\n')
    print((count1, count2))
# ------------------------------------------
# FUNCTION ex4
# ------------------------------------------
def user_definded_map(x):
  reversed_list = list(set(x[1]))
  reversed_list.reverse()
  result = (x[0], reversed_list)
  return result
  
def ex4(sc, my_dataset_dir, station_number):
    input_RDD = sc.textFile(my_dataset_dir).map(lambda line: process_line(line))
    input_RDD.persist()
    filtered_data = input_RDD.filter(lambda x: x[0] == station_number)
    required_RDD = filtered_data.map(lambda x: (x[3], x[6]))
    group_by = required_RDD.groupByKey().map(lambda x: user_definded_map(x))

    required_data = group_by.collect()
    for entry in required_data:
      print(entry)
    
# ------------------------------------------
# FUNCTION ex5
# ------------------------------------------
def user_defined_average(x):
  result_var = list(x[1])
  length = len(result_var)
  time_difference = 0
  for result in result_var:
    expected_time = datetime.strptime(result[0], '%H:%M:%S')
    scheduled_time = datetime.strptime(result[1], '%H:%M:%S')
    time_difference += (expected_time - scheduled_time).total_seconds()
  return (x[0], time_difference/length)
  
def ex5(sc, my_dataset_dir, station_number, month_list):
    input_RDD = sc.textFile(my_dataset_dir) \
                .map(lambda line: process_line(line))
    input_RDD.persist()
    filtered_data = input_RDD.filter(lambda x: x[0] == station_number)
    filter_by_month = filtered_data.filter(lambda x: x[4].split("/")[1] in month_list)
    
    required_column_df = filter_by_month.map(lambda x: ((x[4].split("/")[1], x[3]), (x[7], x[5])))
    
    group_by_df = required_column_df.groupByKey().map(lambda x: user_defined_average(x))
    sorted_by_df = group_by_df.sortBy(lambda x: x[1]).map(lambda x: (x[0][1] + ' ' + x[0][0], x[1]))
    required_data = sorted_by_df.collect()
    for entry in required_data:
      print(entry)
# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, my_dataset_dir, option):
    # Exercise 1:
    # Number of measurements per station number
    if option == 1:
        ex1(sc, my_dataset_dir)

    # Exercise 2: Station 240101 (UCC WGB - Lotabeg):
    # Number of different days for which data is collected.
    if option == 2:
        ex2(sc, my_dataset_dir, 240101)

    # Exercise 3: Station 240561 (UCC WGB - Curraheen):
    # Number of buses arriving ahead and behind schedule.
    if option == 3:
        ex3(sc, my_dataset_dir, 240561)

    # Exercise 4: Station 241111 (CIT Technology Park - Lotabeg):
    # List of buses scheduled per day of the week.
    if option == 4:
        ex4(sc, my_dataset_dir, 241111)

    # Exercise 5: Station 240491 (Patrick Street - Curraheen):
    # Average waiting time per day of the week during the Semester1 months. Sort the entries by decreasing average waiting time.
    if option == 5:
        ex5(sc, my_dataset_dir, 240491, ['09','10','11'])

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
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    my_local_path = "/home/shrenik/ai_course/Big-Data/Assignment2/Assignment2Dataset/"
    my_databricks_path = "/"

    my_dataset_dir = "my_dataset_single_file/"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    my_main(sc, my_dataset_dir, option)

