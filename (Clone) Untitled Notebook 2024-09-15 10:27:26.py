# Databricks notebook source
spark


# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/2010_summary.csv")

# COMMAND ----------

df_flight=spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema","true")\
    .option("mode","FAILFAST")\
    .load("/FileStore/tables/2010_summary.csv")\

df_flight.show()

# COMMAND ----------

from pyspark.sql.types import StructType,StringType,IntegerType,StructField

emp_Schema=StructType([StructField("DEST_COUNTRY_NAME",StringType(),True),
                       StructField("ORIGIN_COUNTRY_NAME",StringType(),True),
                       StructField("count",IntegerType(),True),
                       StructField("_Corrupt_record",StringType(),True)])

# COMMAND ----------

print(emp_Schema)

# COMMAND ----------

df_flights=spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema","true")\
    .option("mode","PERMISSIVE")\
    .schema(emp_Schema)\
    .load("/FileStore/tables/2010_summary.csv")\

df_flights.show()

# COMMAND ----------

df2=spark.read.parquet("/FileStore/tables/part_r_00000_1a9822ba_b8fb_4d8e_844a_ea30d0801b9e_gz.parquet")

# COMMAND ----------

df2.show()

# COMMAND ----------


df.write.format("csv")\
    .option("header", "true")\
    .option("mode", "OVERWRITE")\
    .option("path", "/FileStore/tables/csv_write/")\
    .save()

# COMMAND ----------

dbutils.fs.rm("/FileStore/tables/dframe.csv")

# COMMAND ----------

df_fl=spark.read.format("text")\
    .option("header","true")\
    .option("inferschema","true")\
    .option("mode","FAILFAST")\
    .load("/FileStore/tables/dframe.txt")
df_fl.show()

# COMMAND ----------

df2=spark.read.text("/FileStore/tables/dframe.txt")

# COMMAND ----------

df_fl.write.format("csv")\
    .option("header", "false")\
    .mode("overwrite")\
    .option("path", "/FileStore/tables/csv_me/")\
    .save()

# COMMAND ----------

df_fl.show()

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/csv_me/")

# COMMAND ----------

df_flight.select("ORIGIN_COUNTRY_NAME").show()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
 
emp_data = [
(1,'manish',26,20000,'india','IT'),
(2,'rahul',None,40000,'germany','engineering'),
(3,'pawan',12,60000,'india','sales'),
(4,'roshini',44,None,'uk','engineering'),
(5,'raushan',35,70000,'india','sales'),
(6,None,29,200000,'uk','IT'),
(7,'adam',37,65000,'us','IT'),
(8,'chris',16,40000,'us','sales'),
(None,None,None,None,None,None),
(7,'adam',37,65000,'us','IT')
]

schema= ['id','name','age','salary','country','dept']

df_emp=spark.createDataFrame(emp_data,schema)

df_emp.show()

# COMMAND ----------

df_emp.withColumn("age", when(col("age") < 80, "no")
                  .when(col("age") > 80, "yes")
                  .otherwise("novalue")).show()

# COMMAND ----------

df_emp.count()

# COMMAND ----------

df_emp.select(count("name")).show()

# COMMAND ----------

df_emp.select(max("salary"),min("salary")).show()

# COMMAND ----------

df_emp.select(sum("salary"),count("salary"),avg("salary").cast("int")).show()

# COMMAND ----------

df_emp.groupBy("dept")\
    .agg(sum("salary")).show()

# COMMAND ----------

df_emp.groupBy("dept", "country")\
    .agg(sum("salary"))

emp_df=df_emp.na.drop().show()

# COMMAND ----------

customer_data = [(1,'manish','patna',"30-05-2022"),
(2,'vikash','kolkata',"12-03-2023"),
(3,'nikita','delhi',"25-06-2023"),
(4,'rahul','ranchi',"24-03-2023"),
(5,'mahesh','jaipur',"22-03-2023"),
(6,'prantosh','kolkata',"18-10-2022"),
(7,'raman','patna',"30-12-2022"),
(8,'prakash','ranchi',"24-02-2023"),
(9,'ragini','kolkata',"03-03-2023"),
(10,'raushan','jaipur',"05-02-2023")]

customer_schema=['customer_id','customer_name','address','date_of_joining']

customer_dtail=spark.createDataFrame(data=customer_data,schema=customer_schema)

customer_dtail.createOrReplaceGlobalTempView("customer_dtail")

customer_dtail.show()


# COMMAND ----------

sales_data = [(1,22,10,"01-06-2022"),
(1,27,5,"03-02-2023"),
(2,5,3,"01-06-2023"),
(5,22,1,"22-03-2023"),
(7,22,4,"03-02-2023"),
(9,5,6,"03-03-2023"),
(2,1,12,"15-06-2023"),
(1,56,2,"25-06-2023"),
(5,12,5,"15-04-2023"),
(11,12,76,"12-03-2023"),
(11,45,45,12)]
sales_schema = ['customer_id', 'product_id', 'quantity', 'date_of_purchase']

sales_dtail=spark.createDataFrame(data=sales_data,schema=sales_schema)

sales_dtail.createOrReplaceGlobalTempView("sales_dtail")

sales_dtail.show()

# COMMAND ----------

product_data = [(1, 'fanta',20),
(2, 'dew',22),
(5, 'sprite',40),
(7, 'redbull',100),
(12,'mazza',45),
(22,'coke',27),
(25,'limca',21),
(27,'pepsi',14),
(56,'sting',10)]

product_schema=['id','name','price']

product=spark.createDataFrame(data=product_data,schema=product_schema)

product.createOrReplaceGlobalTempView("product")

product.show()

# COMMAND ----------


df_emp.select('name').show()

# COMMAND ----------

df11=product.select("name")
df11.show()

# COMMAND ----------

df12=customer_dtail.join(sales_dtail,customer_dtail["customer_id"]==sales_dtail["customer_id"],"left_anti").show()
