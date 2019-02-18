__author__ = "RAVI RANJAN"

from pyspark import SparkContext,SparkConf
import os
os.environ['PYSPARK_PYTHON']="C:\\Users\\Ravi\\AppData\\Local\\Programs\\Python\\Python36-32\\python"
os.environ['SPARK_HOME'] ="C:\\Users\\Ravi\\Downloads\\spark-2.3.2-bin-hadoop2.7\\spark-2.3.2-bin-hadoop2.7\\spark-2.3.2-bin-hadoop2.7"

conf = SparkConf().setAppName("Pyspark Pgm").setMaster("local[*]")
sc = SparkContext(conf = conf)

data=sc.textFile("data")

data_rdd=data.map(lambda x:x.split(","))                                                             # spliting data

row_rdd=data_rdd.map(lambda x:(x[0],x[1],x[2],x[3]))

head=row_rdd.first()

rows_no_head=row_rdd.filter(lambda line: line != head)                                               # removing head

intertest_rdd=rows_no_head.filter(lambda x:"Click"in x[3] or "Purchase" in x[3] or "AddtoCart" in x[3])

fashion_rdd=intertest_rdd.filter(lambda x:"Fashion" in x[2])                                         # fashion rdd
#print(fashion_rdd.collect())
fashion_rdd_key_val=fashion_rdd.map(lambda x:(x[0],list(x[1:])))                                     # converting rdd to a key value pair

electronics_rdd=intertest_rdd.filter(lambda x:"Electronics" in x[2])                                 # electronics rdd

electronics_rdd_key_val=electronics_rdd.map(lambda x:(x[0],list(x[1:])))                             # converting rdd to a key value pair

final_rdd_with_none=fashion_rdd_key_val.fullOuterJoin(electronics_rdd_key_val)                       #Doing a full outer join

final_res=final_rdd_with_none.filter(lambda x:x[1][0] is None or x[1][1] is None)                    #filtering on None

rdd_neww_1=final_res.map(lambda x:(x[0],x[1])).filter(lambda x:x[1][1] is not None).map(lambda x:(x[0],x[1][1]))  #removing none from lefttable
rdd_neww_2=final_res.map(lambda x:(x[0],x[1])).filter(lambda x:x[1][0] is not None).map(lambda x:(x[0],x[1][0]))  #removing none from right table

#print(rdd_neww_1.collect())
#print(rdd_neww_2.collect())
join_rdd_without_none=rdd_neww_1.union(rdd_neww_2).map(lambda x:(x[0],x[1][2]))           # joing both rdd after none is removed
for i in join_rdd_without_none.collect():
    print(i)


