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
import operator
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf
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
    if (len(params) == 7):
        res = (int(params[0]),
               str(params[1]),
               float(params[2]),
               float(params[3]),
               str(params[4]),
               int(params[5]),
               int(params[6])
               )

    # 5. We return res
    return res

# ------------------------------------------
# FUNCTION ex1
# ------------------------------------------
def ex1(sc, my_dataset_dir):
    # 1. We load the dataset into an inputRDD
    input_RDD = sc.textFile(my_dataset_dir).map(lambda line: line.split(";"))
    input_RDD.persist()
    
    # filtering RDD when bikes available is 0
    filtered_RDD = input_RDD.filter(lambda x: int(x[5]) == 0).filter(lambda x: int(x[0]) == 0)
    
    # select only required values
    required_RDD = filtered_RDD.map(lambda x: (x[1], x[5]))
    
    # group an RDD by its key and call mapValues function to calculate length of each group
    grouped_by_RDD = required_RDD.groupByKey().mapValues(len)
    # counting each group
    count = grouped_by_RDD.count()
    # sorting RDD by bikesAvailable value
    sorted_RDD = grouped_by_RDD.sortBy(lambda x: x[1], ascending=False)
    # Collect such these entries
    collected_RDD = sorted_RDD.collect()
    # printing the result
    print('\n------------------------------')
    print('Exercise 1:')
    print('------------------------------\n')
    print(count)
    for entry in collected_RDD:
      print(entry)
    

# ------------------------------------------
# FUNCTION ex2
# ------------------------------------------
def ex2(sc, my_dataset_dir):
    # 1. We load the dataset into an inputRDD
    input_RDD = sc.textFile(my_dataset_dir+'bikeMon_20170827.csv').map(lambda line: line.split(";"))
    input_RDD.persist()
    
    # filtering RDD and removing entries having status=1
    filtered_RDD = input_RDD.filter(lambda x: int(x[0]) == 0)
    
    # creating separate entry for `hour` for each entry
    separate_month_RDD = filtered_RDD.map(lambda x: (x[1], x[4].split(" ")[1].split(":")[0], x[5]))
    
    # setting 1st column name as `station_name <hour>` For eg: "Bandfield 06" and also selecting required data like bikesAvailable
    new_name_col_RDD = separate_month_RDD.map(lambda x: (x[0] + ' ' + x[1], int(x[2])))
    
    # grouping together all the same key values
    countsByKey = sc.broadcast(new_name_col_RDD.countByKey())
    
    # adding all the values of each key
    rdd1 = new_name_col_RDD.reduceByKey(operator.add)
    
    # calculating average of each key sum calculated using countsByKey variable
    rdd1 = rdd1.map(lambda x: (x[0], x[1]/countsByKey.value[x[0]]))
    
    # sorting the rdd based on the station name and hour
    sort_by_RDD = rdd1.sortBy(lambda x: x[0])
    
    # collecting required RDD
    collected_RDD = sort_by_RDD.collect()
    
    # printing collected RDD
    print('\n------------------------------')
    print('Exercise 2:')
    print('------------------------------\n')
    for entry in collected_RDD:
      print(entry)
      
# ------------------------------------------
# FUNCTION ex3
# ------------------------------------------
global previous_value
previous_value = -1
def user_defined_func(z):
  global previous_value
  if previous_value != 0 and z[1][1] == 0:
    previous_value = z[1][1]
    return (z[1][0], z[0])
  previous_value = z[1][1]

def ex3(sc, my_dataset_dir):
    # 1. We load the dataset into an inputRDD
    input_RDD = sc.textFile(my_dataset_dir+'bikeMon_20170827.csv').map(lambda line: line.split(";"))
    input_RDD.persist()
    # filtering RDD and removing entries having status=1
    filtered_RDD = input_RDD.filter(lambda x: int(x[0]) == 0)
    # adding new column for time and taking only required data
    seperate_time_col_RDD = filtered_RDD.map(lambda x: (x[1], (x[4].split(" ")[1][:5], int(x[5]))))
    # sorting dateD by station name and date 
    sort_by_time_RDD = seperate_time_col_RDD.sortBy(lambda x: (x[0], x[1][0]))
    # calling user defined function to get tuple whose previous value is not equal to current value and current value 
    required_RDD = sort_by_time_RDD.map(lambda z: user_defined_func(z))
    # filter values which are not None
    processed_RDD = required_RDD.filter(lambda x: x)
    # sorting by time
    sort_by_time_RDD = processed_RDD.sortBy(lambda x: x[0])
    collected_RDD = sort_by_time_RDD.collect()
    for entry in collected_RDD:
      print(entry)
    
    
# ------------------------------------------
# FUNCTION ex4
# ------------------------------------------
def ex4(sc, my_dataset_dir, ran_out_times):
    # 1. We load the dataset into an inputRDD
    input_RDD = sc.textFile(my_dataset_dir+'bikeMon_20170827.csv').map(lambda line: line.split(";"))
    input_RDD.persist()
    # filtering RDD and removing entries having status=1
    active_stations_RDD = input_RDD.filter(lambda x: int(x[0]) == 0)
    
    #  considering only required columns and filtering entries based on ran_out_times
    filtered_RDD = active_stations_RDD.map(lambda x: (x[4].split()[1][:5], (x[1], int(x[5])))).filter(lambda x: x[0]+':00' in ran_out_times)
    
    #  reducing the entries by time key and taking max value from each column.
    grouped_RDD = filtered_RDD.reduceByKey(lambda x1, x2: max(x1, x2, key=lambda x: x[1]))
    
    #  soring rows based on time key
    ordered_by_RDD = grouped_RDD.sortBy(lambda x: x[0])
    
    # 6. We print the result
    print('\n------------------------------')
    print('Exercise 4:')
    print('------------------------------\n')
    for e in ordered_by_RDD.collect():
      print(e)
    
# ------------------------------------------
# FUNCTION ex5
# ------------------------------------------

def get_previous_row_val(x):
  
  current_value_list = x[1][0]
  previous_value_list = x[1][1]
  final_list = []
  bikes_given = 0
  bikes_taken = 0
  for index in range(len(current_value_list)):
    previous_value1 = previous_value_list[index]
    current_value = current_value_list[index]
    if (current_value < previous_value1) and previous_value1 != -9:
      new_take_value = previous_value1 - current_value
      bikes_taken += new_take_value
    elif (current_value > previous_value1) and previous_value1 != -9:
      new_give_value = current_value - previous_value1
      bikes_given += new_give_value
  return (x[0][0], (bikes_taken, bikes_given))

def get_updated_rows(row):
  
  bikes_values = list(row[1])
  previous_values = [-9]
  previous_values.extend(bikes_values[0:-1])
  new_value = (row[0], (bikes_values, previous_values))
  return new_value
  
def ex5(sc, my_dataset_dir):
    # 1. We load the dataset into an inputRDD
    input_RDD = sc.textFile(my_dataset_dir).map(lambda line: line.split(";"))
    input_RDD.persist()
    # filtering RDD and removing entries having status=1
#     filtered_RDD = input_RDD.filter(lambda x: int(x[0]) == 0).filter(lambda row: row[1]=="Bandfield" or row[1]=="Kent Station")
    filtered_RDD = input_RDD.filter(lambda x: int(x[0]) == 0)
    # sorting values by the time and adding new column having date values as a new column
    sorted_by_datetatus = filtered_RDD.sortBy(lambda x: (x[1], x[4])).map(lambda x: ((x[1], x[4].split()[0]), int(x[5])))
    sorted_by_datetatus.persist()
    # grouping RDD by key
    grouped_RDD = sorted_by_datetatus.groupByKey()
    # calling user-defined function to add new column for each RDD row which previous bikesAvailable value 
    extra_row_RDD = grouped_RDD.map(lambda row: get_updated_rows(row))
    
    # calling get_previous_row_val() to get whether on current row bike is given or taken or nothing
    required_RDD = extra_row_RDD.map(lambda x: get_previous_row_val(x))
    required_RDD.persist()
    
    # reducing the RDD by key and adding bike given and take value to find sum
    processed_RDD = required_RDD.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
    processed_RDD.persist()
    #  sorting by bikensgiven and bikestaken 
    final_result_RDD = processed_RDD.sortBy(lambda x: (x[1][0], x[1][1]), ascending=False)
    final_result_RDD.persist()
    # 6. We print the result
    print('\n------------------------------')
    print('Exercise 5:')
    print('------------------------------\n')
    collected_RDD = final_result_RDD.collect()
    for row in collected_RDD:
      print(row)
      

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, my_dataset_dir, option, ran_out_times):
    # Exercise 1: Number of times each station ran out of bikes (sorted decreasingly by station).
    if option == 1:
        ex1(sc, my_dataset_dir)

    # Exercise 2: Pick one busy day with plenty of ran outs -> Sunday 28th August 2017
    #             Average amount of bikes per station and hour window (e.g. [9am, 10am), [10am, 11am), etc. )
    if option == 2:
        ex2(sc, my_dataset_dir)

    # Exercise 3: Pick one busy day with plenty of ran outs -> Sunday 28th August 2017
    #             Get the different ran-outs to attend.
    #             Note: n consecutive measurements of a station being ran-out of bikes has to be considered a single ran-out,
    #                   that should have been attended when the ran-out happened in the first time.
    if option == 3:
        ex3(sc, my_dataset_dir)

    # Exercise 4: Pick one busy day with plenty of ran outs -> Sunday 28th August 2017
    #             Get the station with biggest number of bikes for each ran-out to be attended.
    if option == 4:
        ex4(sc, my_dataset_dir, ran_out_times)

    # Exercise 5: Total number of bikes that are taken and given back per station (sorted decreasingly by the amount of bikes).
    if option == 5:
        ex5(sc, my_dataset_dir)

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

    ran_out_times = ['06:03:00', '06:03:00', '08:58:00', '09:28:00', '10:58:00', '12:18:00',
                     '12:43:00', '12:43:00', '13:03:00', '13:53:00', '14:28:00', '14:28:00',
                     '15:48:00', '16:23:00', '16:33:00', '16:38:00', '17:09:00', '17:29:00',
                     '18:24:00', '19:34:00', '20:04:00', '20:14:00', '20:24:00', '20:49:00',
                     '20:59:00', '22:19:00', '22:59:00', '23:14:00', '23:44:00']

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

    # 4. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    my_main(sc, my_dataset_dir, option, ran_out_times)
