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
import graphframes

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark):
    # 1. Operation C1: We create my_verticesDF
    p1 = pyspark.sql.Row(id=1, value=1)
    p2 = pyspark.sql.Row(id=2, value=2)
    p3 = pyspark.sql.Row(id=3, value=3)
    p4 = pyspark.sql.Row(id=4, value=4)
    p5 = pyspark.sql.Row(id=5, value=5)
    p6 = pyspark.sql.Row(id=6, value=6)
    p7 = pyspark.sql.Row(id=7, value=7)
    p8 = pyspark.sql.Row(id=8, value=8)
    p9 = pyspark.sql.Row(id=9, value=9)

    my_verticesDF = spark.createDataFrame([p1, p2, p3, p4, p5, p6, p7, p8, p9])

    # 2. Operation C2: We create my_edgesDF
    e1 = pyspark.sql.Row(src=1, dst=4, value="1-4")
    e2 = pyspark.sql.Row(src=2, dst=8, value="2-8")
    e3 = pyspark.sql.Row(src=3, dst=6, value="3-6")
    e4 = pyspark.sql.Row(src=4, dst=7, value="4-7")
    e5 = pyspark.sql.Row(src=5, dst=2, value="5-2")
    e6 = pyspark.sql.Row(src=6, dst=9, value="6-9")
    e7 = pyspark.sql.Row(src=7, dst=1, value="7-1")
    e8 = pyspark.sql.Row(src=8, dst=6, value="8-6")
    e9 = pyspark.sql.Row(src=8, dst=5, value="8-5")
    e10 = pyspark.sql.Row(src=9, dst=7, value="9-7")
    e11 = pyspark.sql.Row(src=9, dst=3, value="9-3")

    my_edgesDF = spark.createDataFrame([e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11])

    # 3. Operation C3: We create myGF from my_verticesDF and my_edgesDF
    myGF = graphframes.GraphFrame(my_verticesDF, my_edgesDF)

    # 4. Operation P1: We persist myGF
    myGF.persist()

    # 5. Operation T1: We revert back to sol_verticesDF
    sol_verticesDF = myGF.vertices

    # 6. Operation A1: We display the content of sol_verticesDF
    sol_verticesDF.show()

    # 7. Operation T2: We revert back to sol_edgesDF
    sol_edgesDF = myGF.edges

    # 8. Operation A2: We display the content of sol_edgesDF
    sol_edgesDF.show()


# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    # 2. We run my_main
    my_main(spark)
