# Databricks notebook source
emp=spark.read.csv("/Volumes/workspace/default/pysparkpractice1/employee.csv",header=True,inferSchema=True)

# COMMAND ----------

emp.show()

# COMMAND ----------

empmiss=emp.fillna({"salary":0})
empmiss.show()

# COMMAND ----------

from pyspark.sql.functions import when
from pyspark.sql.functions import col
empaddcol=emp.withColumn("salary_category",when(col("salary")>=50000,"High").otherwise("Low"))
empaddcol.show()

# COMMAND ----------

empfiltered=emp.filter(col("salary")>0)
empfiltered.show()

# COMMAND ----------

from pyspark.sql.functions import avg
empavgsalary=empfiltered.groupBy("deptid").agg(avg("salary").alias("empavgsalary"))
empavgsalary.show()

# COMMAND ----------

dept=spark.read.csv("/Volumes/workspace/default/pysparkpractice1/department.csv",header=True,inferSchema=True)
dept.show()

# COMMAND ----------

empdept=empaddcol.join(dept,empaddcol.deptid==dept.deptid,"inner")
empdept.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank,desc
window_spec=Window.partitionBy("deptid").orderBy(desc(col("salary")))
empwin=emp.withColumn("salaryrank",dense_rank().over(window_spec))
empwin.show()

# COMMAND ----------

empdept=empaddcol.join(dept,empaddcol.deptid==dept.deptid,"inner").drop(dept.deptid)
empdept.show()


# COMMAND ----------

empdept.write.mode("overwrite").json("/Volumes/workspace/default/pysparkpractice1/output_park")