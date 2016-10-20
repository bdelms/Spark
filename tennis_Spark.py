#The goal of this script is to find the tennis players rankings that wan less than 1% of the games 
# played during the year 2014.

#Importing the Dataset in PySpark 

tennis = spark.read.format("csv").option("header").load("/home/Downloads/tennis2014.csv")


#Checking the data types in our dataset :

tennis.dtypes 

[('ATP', 'string'), ('Location', 'string'), ('Tournament', 'string'), ('Date', 'string'), ('Series', 'string'), ('Court', 'string'), ('Surface', 'string'), ('Round', 'string'), ('Best of', 'string'), ('Winner', 'string'),
 ('Loser', 'string'), 'WRank', 'string'),... ] 

# The colunm we are interested in is " WRank"(winner ranking), we convert the type from String
# to Integer  

from pyspark.sql.types import IntegerType
tennis = tennis.withColumn("Wrank",tennis["Wrank"].cast(IntegerType()))

tennis.dtypes

[('ATP', 'string'), ('Location', 'string'), ('Tournament', 'string'), ('Date', 'string'), ('Series', 'string'), ('Court', 'string'), ('Surface', 'string'), ('Round', 'string'), ('Best of', 'string'), ('Winner', 'string'),
 ('Loser', 'string'), 'WRank', 'int'),... ] 

# Using the SQL function in Spark to query the Dataframe in SQL

tennis.createOrReplaceTempView("tennisSQL")

# We count the number of elements in the column

count = spark.sql("SELECT COUNT(WRank) FROM tennisSQL").show()

+------------+
|count(WRank)|
+------------+
|        2600|
+------------+

# Finding the players rankings that wan less than 1% of the games

result = spark.sql("SELECT WRank, COUNT(WRank) FROM tennisSQL GROUP BY WRank HAVING COUNT(WRank) < (SELECT COUNT(WRank) FROM tennisSQL)/100 ").show()

+-----+------------+
|WRank|count(WRank)|
+-----+------------+
|  148|           3|
|  243|           1|
|   85|          15|
|  137|           2|
|  251|           1|
|   65|          21|
|   53|          20|
|  133|           3|
|   78|          19|
|  108|          10|
|  155|           3|
|   34|          21|
|  211|           2|
|  101|          10|
|  115|           2|
|  126|           5|
|   81|           7|
|  847|           2|
|  183|           1|
|   76|          11|
+-----+------------+


