
------------------------------------------- 

 EXAMPLES

------------------------------------------- 

These are the same examples as before. However, now we are reading them from input files, rather than by socket.

We provide a short description of each file and its output.

------------------------------------------- 

 09_intro.py

------------------------------------------- 

ERROR: 'Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark'

--------------------------------------------------------- 

 10_single_step_window.py

--------------------------------------------------------- 

ERROR: 'Sorting is not supported on streaming DataFrames/Datasets, unless it is on aggregated DataFrame/Dataset in Complete output mode'

After removing the sorting operation the code works: 
# 9. Operation T6: We sort them by the starting time of the window
# solutionSDF = formattedSDF.orderBy(formattedSDF["window_start"].asc())

However, as we can see, it does not find a solution for the last batch, as it needs more time than "complete" mode to start processing results for the first time step.
Whereas in example 06_single_step_window.py we used "complete" mode and results were produced Batch: 1, now in example 10_single_step_window.py with "append" mode the first results are not produced until Batch: 2.

Even in this case, I still don't understand why it does not spend more time doing computations, produce a final Batch: 7 and process all time steps :(
 
-------------------------------------------
Batch: 0
-------------------------------------------
+----+-----+------------+----------+
|word|count|window_start|window_end|
+----+-----+------------+----------+
+----+-----+------------+----------+

-------------------------------------------
Batch: 1
-------------------------------------------
+----+-----+------------+----------+
|word|count|window_start|window_end|
+----+-----+------------+----------+
+----+-----+------------+----------+

-------------------------------------------
Batch: 2
-------------------------------------------
+-------+-----+-------------------+-------------------+
|   word|count|       window_start|         window_end|
+-------+-----+-------------------+-------------------+
|Goodbye|    3|2018-11-28 17:59:00|2018-11-28 17:59:20|
|  Hello|    3|2018-11-28 17:59:00|2018-11-28 17:59:20|
+-------+-----+-------------------+-------------------+

-------------------------------------------
Batch: 3
-------------------------------------------
+-----+-----+-------------------+-------------------+
| word|count|       window_start|         window_end|
+-----+-----+-------------------+-------------------+
|Again|    3|2018-11-28 17:59:20|2018-11-28 17:59:40|
|Hello|    3|2018-11-28 17:59:20|2018-11-28 17:59:40|
+-----+-----+-------------------+-------------------+

-------------------------------------------
Batch: 4
-------------------------------------------
+-----+-----+-------------------+-------------------+
| word|count|       window_start|         window_end|
+-----+-----+-------------------+-------------------+
|Hello|    6|2018-11-28 17:59:40|2018-11-28 18:00:00|
|Again|    3|2018-11-28 17:59:40|2018-11-28 18:00:00|
+-----+-----+-------------------+-------------------+

-------------------------------------------
Batch: 5
-------------------------------------------
+-------+-----+-------------------+-------------------+
|   word|count|       window_start|         window_end|
+-------+-----+-------------------+-------------------+
|       |    2|2018-11-28 18:00:00|2018-11-28 18:00:20|
|Goodbye|    2|2018-11-28 18:00:00|2018-11-28 18:00:20|
|  Hello|    2|2018-11-28 18:00:00|2018-11-28 18:00:20|
+-------+-----+-------------------+-------------------+

-------------------------------------------
Batch: 6
-------------------------------------------
+-----+-----+-------------------+-------------------+
| word|count|       window_start|         window_end|
+-----+-----+-------------------+-------------------+
|Again|    2|2018-11-28 18:00:20|2018-11-28 18:00:40|
|Hello|    2|2018-11-28 18:00:20|2018-11-28 18:00:40|
|     |    2|2018-11-28 18:00:20|2018-11-28 18:00:40|
+-----+-----+-------------------+-------------------+



--------------------------------------------------------- 

 11_multiple_step_window.py

---------------------------------------------------------

ERROR: 'Sorting is not supported on streaming DataFrames/Datasets, unless it is on aggregated DataFrame/Dataset in Complete output mode'

After removing the sorting operation the code works: 
# 9. Operation T6: We sort them by the starting time of the window
# solutionSDF = formattedSDF.orderBy(formattedSDF["window_start"].asc())

However, as we can see, it does not find a solution for the last batch, as it needs more time than "complete" mode to start processing results for the first time step.
Whereas in example 06_single_step_window.py we used "complete" mode and results were produced Batch: 1, now in example 10_single_step_window.py with "append" mode the first results are not produced until Batch: 2.

Even in this case, I still don't understand why it does not spend more time doing computations, produce a final Batch: 7 and process all time steps :(

-------------------------------------------
Batch: 0
-------------------------------------------
+----+-----+------------+----------+
|word|count|window_start|window_end|
+----+-----+------------+----------+
+----+-----+------------+----------+

-------------------------------------------
Batch: 1
-------------------------------------------
+----+-----+------------+----------+
|word|count|window_start|window_end|
+----+-----+------------+----------+
+----+-----+------------+----------+

-------------------------------------------
Batch: 2
-------------------------------------------
+-------+-----+-------------------+-------------------+
|   word|count|       window_start|         window_end|
+-------+-----+-------------------+-------------------+
|Goodbye|    3|2018-11-28 18:15:00|2018-11-28 18:16:00|
|  Hello|    3|2018-11-28 18:15:00|2018-11-28 18:16:00|
+-------+-----+-------------------+-------------------+

-------------------------------------------
Batch: 3
-------------------------------------------
+-------+-----+-------------------+-------------------+
|   word|count|       window_start|         window_end|
+-------+-----+-------------------+-------------------+
|  Again|    3|2018-11-28 18:15:30|2018-11-28 18:16:30|
|  Hello|    6|2018-11-28 18:15:30|2018-11-28 18:16:30|
|Goodbye|    3|2018-11-28 18:15:30|2018-11-28 18:16:30|
+-------+-----+-------------------+-------------------+

-------------------------------------------
Batch: 4
-------------------------------------------
+-----+-----+-------------------+-------------------+
| word|count|       window_start|         window_end|
+-----+-----+-------------------+-------------------+
|Again|    6|2018-11-28 18:16:00|2018-11-28 18:17:00|
|Hello|    9|2018-11-28 18:16:00|2018-11-28 18:17:00|
+-----+-----+-------------------+-------------------+

-------------------------------------------
Batch: 5
-------------------------------------------
+-------+-----+-------------------+-------------------+
|   word|count|       window_start|         window_end|
+-------+-----+-------------------+-------------------+
|Goodbye|    2|2018-11-28 18:16:30|2018-11-28 18:17:30|
|  Again|    3|2018-11-28 18:16:30|2018-11-28 18:17:30|
|       |    2|2018-11-28 18:16:30|2018-11-28 18:17:30|
|  Hello|    8|2018-11-28 18:16:30|2018-11-28 18:17:30|
+-------+-----+-------------------+-------------------+

-------------------------------------------
Batch: 6
-------------------------------------------
+-------+-----+-------------------+-------------------+
|   word|count|       window_start|         window_end|
+-------+-----+-------------------+-------------------+
|       |    4|2018-11-28 18:17:00|2018-11-28 18:18:00|
|Goodbye|    2|2018-11-28 18:17:00|2018-11-28 18:18:00|
|  Hello|    4|2018-11-28 18:17:00|2018-11-28 18:18:00|
|  Again|    2|2018-11-28 18:17:00|2018-11-28 18:18:00|
+-------+-----+-------------------+-------------------+


--------------------------------------------------------------------- 

 12_single_step_window_with_watermarking.py

--------------------------------------------------------------------- 

ERROR: 'Sorting is not supported on streaming DataFrames/Datasets, unless it is on aggregated DataFrame/Dataset in Complete output mode'

After removing the sorting operation the code works: 
# 9. Operation T6: We sort them by the starting time of the window
# solutionSDF = formattedSDF.orderBy(formattedSDF["window_start"].asc())

This example is very interesting. If we maintain the watermark of 100 seconds it will delay all results 
-producing nothing- until the last batch, where the watermark is over and the results (being now 100% reliable) are finally released. 
However, as we have this problem with reporting the results of the last time step, we have to reduce the watermark by a few seconds, to see the effect of aggregating the results of all time steps but last one and finally resease such results. 

However, even by reducing the watermark, the example doesn't seem to compute any solution. In fairness, I don't understand why. 

-------------------------------------------
Batch: 0
-------------------------------------------
+----+-----+------------+----------+
|word|count|window_start|window_end|
+----+-----+------------+----------+
+----+-----+------------+----------+

-------------------------------------------
Batch: 1
-------------------------------------------
+----+-----+------------+----------+
|word|count|window_start|window_end|
+----+-----+------------+----------+
+----+-----+------------+----------+

-------------------------------------------
Batch: 2
-------------------------------------------
+----+-----+------------+----------+
|word|count|window_start|window_end|
+----+-----+------------+----------+
+----+-----+------------+----------+

-------------------------------------------
Batch: 3
-------------------------------------------
+----+-----+------------+----------+
|word|count|window_start|window_end|
+----+-----+------------+----------+
+----+-----+------------+----------+

-------------------------------------------
Batch: 4
-------------------------------------------
+----+-----+------------+----------+
|word|count|window_start|window_end|
+----+-----+------------+----------+
+----+-----+------------+----------+

-------------------------------------------
Batch: 5
-------------------------------------------
+----+-----+------------+----------+
|word|count|window_start|window_end|
+----+-----+------------+----------+
+----+-----+------------+----------+ 





















