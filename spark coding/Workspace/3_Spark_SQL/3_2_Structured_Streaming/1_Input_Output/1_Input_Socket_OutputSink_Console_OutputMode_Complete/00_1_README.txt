
------------------------------------------- 

 SOCKET INSTRUCTIONS

------------------------------------------- 

---------------------------
 TERMINAL 1
---------------------------
nc -lk 9999

---------------------------
 TERMINAL 2		
---------------------------
python3 YOUR_PROGRAM.py localhost 9999

---------------------------
 TERMINAL 1
---------------------------
Type the following sentences, one each time_step_interval seconds: 

Hello Goodbye
Hello Again
Hello Hello Again

Spark will process them in streaming mode, analysing them one batch at a time. 
Depending on the output mode the result will be one or another. 

------------------------------------------- 

 EXAMPLES

------------------------------------------- 

We provide a short description of each file and its output.

------------------------------------------- 

 01_intro.py

------------------------------------------- 

The StreamingDataFrame is considered as an unbound table, which keeps growing on each time step. 
In Complete mode, the table is considered as a single entity. It achieves an equivalent behavior to the stateful example of 
2_Spark_Streaming/09_updateStateByKey_(includes_race_conditions_2).py 

-------------------------------------------                                     
Batch: 0
-------------------------------------------
+----+-----+
|word|count|
+----+-----+
+----+-----+

-------------------------------------------                                     
Batch: 1
-------------------------------------------
+-------+-----+
|   word|count|
+-------+-----+
|Goodbye|    1|
|  Hello|    1|
+-------+-----+

-------------------------------------------                                     
Batch: 2
-------------------------------------------
+-------+-----+
|   word|count|
+-------+-----+
|Goodbye|    1|
|  Hello|    2|
|  Again|    1|
+-------+-----+

-------------------------------------------                                     
Batch: 3
-------------------------------------------
+-------+-----+
|   word|count|
+-------+-----+
|Goodbye|    1|
|  Hello|    4|
|  Again|    2|
+-------+-----+



--------------------------------------------------------- 

 02_single_step_window.py

--------------------------------------------------------- 

The StreamingDataFrame is considered as an unbound table, which keeps growing on each time step. 
In this example the new rows arriving on each time step are enriched with their timestamp. 
This timestamp is used later on, as we do not only group rows by their word, but also by their timestamp window: 
(timestamp window, words). 

In this example we create windows of size = 1 (which means a window of time_step_interval seconds) and slide = 1 (which means a new window every time_step_interval seconds).
This window configuration is equivalent to the stateless behavior of 2_Spark_Streaming/02_word_count.py

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
+-------+-----+-------------------+-------------------+
|   word|count|       window_start|         window_end|
+-------+-----+-------------------+-------------------+
|Goodbye|    1|2018-11-28 10:33:45|2018-11-28 10:34:00|
|  Hello|    1|2018-11-28 10:33:45|2018-11-28 10:34:00|
+-------+-----+-------------------+-------------------+

-------------------------------------------
Batch: 2
-------------------------------------------
+-------+-----+-------------------+-------------------+
|   word|count|       window_start|         window_end|
+-------+-----+-------------------+-------------------+
|Goodbye|    1|2018-11-28 10:33:45|2018-11-28 10:34:00|
|  Hello|    1|2018-11-28 10:33:45|2018-11-28 10:34:00|
|  Again|    1|2018-11-28 10:34:00|2018-11-28 10:34:15|
|  Hello|    1|2018-11-28 10:34:00|2018-11-28 10:34:15|
+-------+-----+-------------------+-------------------+

-------------------------------------------
Batch: 3
-------------------------------------------
+-------+-----+-------------------+-------------------+
|   word|count|       window_start|         window_end|
+-------+-----+-------------------+-------------------+
|Goodbye|    1|2018-11-28 10:33:45|2018-11-28 10:34:00|
|  Hello|    1|2018-11-28 10:33:45|2018-11-28 10:34:00|
|  Again|    1|2018-11-28 10:34:00|2018-11-28 10:34:15|
|  Hello|    1|2018-11-28 10:34:00|2018-11-28 10:34:15|
|  Hello|    2|2018-11-28 10:34:15|2018-11-28 10:34:30|
|  Again|    1|2018-11-28 10:34:15|2018-11-28 10:34:30|
+-------+-----+-------------------+-------------------+



--------------------------------------------------------- 

 03_multiple_step_window.py

---------------------------------------------------------

The StreamingDataFrame is considered as an unbound table, which keeps growing on each time step. 
In this example the new rows arriving on each time step are enriched with their timestamp. 
This timestamp is used later on, as we do not only group rows by their word, but also by their timestamp window: 
(timestamp window, words). 

In this example we create windows of size = 2 (which means a window of 2 times the time_step_interval seconds) and slide = 1 (which means a new window every time_step_interval seconds).
This window configuration is equivalent to the stateless behavior of 2_Spark_Streaming/04_window_(race_conditions).py

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
+-------+-----+-------------------+-------------------+
|   word|count|       window_start|         window_end|
+-------+-----+-------------------+-------------------+
|Goodbye|    1|2018-11-28 10:36:00|2018-11-28 10:36:30|
|  Hello|    1|2018-11-28 10:36:00|2018-11-28 10:36:30|
|  Hello|    1|2018-11-28 10:36:15|2018-11-28 10:36:45|
|Goodbye|    1|2018-11-28 10:36:15|2018-11-28 10:36:45|
+-------+-----+-------------------+-------------------+

-------------------------------------------
Batch: 2
-------------------------------------------
+-------+-----+-------------------+-------------------+
|   word|count|       window_start|         window_end|
+-------+-----+-------------------+-------------------+
|Goodbye|    1|2018-11-28 10:36:00|2018-11-28 10:36:30|
|  Hello|    1|2018-11-28 10:36:00|2018-11-28 10:36:30|
|  Hello|    2|2018-11-28 10:36:15|2018-11-28 10:36:45|
|Goodbye|    1|2018-11-28 10:36:15|2018-11-28 10:36:45|
|  Again|    1|2018-11-28 10:36:15|2018-11-28 10:36:45|
|  Again|    1|2018-11-28 10:36:30|2018-11-28 10:37:00|
|  Hello|    1|2018-11-28 10:36:30|2018-11-28 10:37:00|
+-------+-----+-------------------+-------------------+

-------------------------------------------
Batch: 3
-------------------------------------------
+-------+-----+-------------------+-------------------+
|   word|count|       window_start|         window_end|
+-------+-----+-------------------+-------------------+
|Goodbye|    1|2018-11-28 10:36:00|2018-11-28 10:36:30|
|  Hello|    1|2018-11-28 10:36:00|2018-11-28 10:36:30|
|  Hello|    2|2018-11-28 10:36:15|2018-11-28 10:36:45|
|Goodbye|    1|2018-11-28 10:36:15|2018-11-28 10:36:45|
|  Again|    1|2018-11-28 10:36:15|2018-11-28 10:36:45|
|  Again|    2|2018-11-28 10:36:30|2018-11-28 10:37:00|
|  Hello|    3|2018-11-28 10:36:30|2018-11-28 10:37:00|
|  Hello|    2|2018-11-28 10:36:45|2018-11-28 10:37:15|
|  Again|    1|2018-11-28 10:36:45|2018-11-28 10:37:15|
+-------+-----+-------------------+-------------------+



--------------------------------------------------------------------- 

 04_single_step_window_with_watermarking.py

--------------------------------------------------------------------- 

The StreamingDataFrame is considered as an unbound table, which keeps growing on each time step. 
In this example the new rows arriving on each time step are enriched with a ---fixed--- timestamp. 
This timestamp is used later on, as we do not only group rows by their word, but also by their timestamp window: 
(timestamp window, words). 

In this example we create windows of size = 1 (which means a window of time_step_interval seconds) and slide = 1 (which means a new window every time_step_interval seconds).
This window configuration is equivalent to the stateless behavior of 2_Spark_Streaming/02_word_count.py

However, what makes this example different from "3_2_Structured_Streaming/02_single_step_window.py" and from "2_Spark_Streaming/02_word_count.py" is its fixed timestamp:
 (i) If all Rows have a unique (previosly fixed) timestamp, then all Rows have to be aggreagated together, as it happened in example "3_2_Structured_Streaming/01_intro.py". This makes sense. 
 (ii) However, if we set windows of size = 1 and slide = 1 then we cannot aggregate together data arriving in different windows (different time steps). This does not make sense. 

How is then possible that we are getting such results? 
Well, while the statement (i) is correct, the statement (ii) is imprecise. 
In Structured Streaming we do not create windows to group Rows by the time they arrive (time step), but by their timestamp! 
Thus, if data arriving in different time steps have indeed the same timestamp then they should be grouped together. 

And what's the point of grouping data by a timestamp rather by their arrival time? To support network delays causing late data arrival!
Let's suppose the timestamp is indeed the time in which the data is being generated. By supporting timestamp windows we support re-adjustment of previously computed results given new late-arrival data that was previously missed. 

Last but not least: If we tolerate late-arrival data then all results being computed are not 100% sure. 
They all are susceptible to be re-adjusted in a further time step by new late-arrival data finally landing. 

This creates two kind of problems: 
(1) Legal security: In law there have to be some pillars that give credibility to a sentence. 
Likewise, our streaming results can not be permanently susceptible of a further "what-if" re-adjusting them. 
(2) Amount of "intermediate results" being stored: To allow a result to be susceptible of being re-adjusted Spark has to keep such results and the metadata for re-computing them (for fault tolerance) in memory. This is very costly, and unnafordable for long periods of time. 

For the aforementioned reasons Spark implements the concept of Watermarking, which is equivalent to the SparkStreamingContext::remember function we saw in Spark Streaming. 
This concept provides a late-arrival upper bound, not considering it after that bound. For example, if 60 seconds watermarking is provided, data arriving 61 seconds or will not be taken into account. Likewise, any "intermediate result" will be promoted to "final result" after 60 seconds the window was officially closed. 

This is what makes sense of the results we obtain for our example. 
On it, watermarking of 60 seconds is provided and, once again, all data is introduced with a fixed timestamp belonging to time step 0. 
In this context, the results we see are observing for Batch 0, Batch 1, Batch 2 and Batch 3 are nothing but the evolution of the results of Time Step 0, re-adjusted over and over as new "simulated" late-arrival data arrives.  

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
+-------+-----+-------------------+-------------------+
|   word|count|       window_start|         window_end|
+-------+-----+-------------------+-------------------+
|  Hello|    1|2018-11-28 10:40:45|2018-11-28 10:41:00|
|Goodbye|    1|2018-11-28 10:40:45|2018-11-28 10:41:00|
+-------+-----+-------------------+-------------------+

-------------------------------------------
Batch: 2
-------------------------------------------
+-------+-----+-------------------+-------------------+
|   word|count|       window_start|         window_end|
+-------+-----+-------------------+-------------------+
|  Again|    1|2018-11-28 10:40:45|2018-11-28 10:41:00|
|  Hello|    2|2018-11-28 10:40:45|2018-11-28 10:41:00|
|Goodbye|    1|2018-11-28 10:40:45|2018-11-28 10:41:00|
+-------+-----+-------------------+-------------------+

-------------------------------------------
Batch: 3
-------------------------------------------
+-------+-----+-------------------+-------------------+
|   word|count|       window_start|         window_end|
+-------+-----+-------------------+-------------------+
|  Again|    2|2018-11-28 10:40:45|2018-11-28 10:41:00|
|  Hello|    4|2018-11-28 10:40:45|2018-11-28 10:41:00|
|Goodbye|    1|2018-11-28 10:40:45|2018-11-28 10:41:00|
+-------+-----+-------------------+-------------------+

