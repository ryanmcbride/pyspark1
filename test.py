import findspark
findspark.init()
import pyspark # only run after findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import seaborn as sns
import matplotlib.pyplot as plt
spark = SparkSession.builder.getOrCreate()

df = spark.read.csv('./data/Boston.csv', inferSchema=True, header=True)
df.show(5)

#Creating Table
df.registerTempTable('BostonTable')
sqlContext = SQLContext(spark)
#Running Query
df1 = sqlContext.sql("SELECT * from BostonTable").toPandas()
df2 = sqlContext.sql("SELECT AGE, TAX from BostonTable where LSTAT < 2").toPandas()
#Creating Visualization
fig = plt.pie(df2['AGE'], autopct='%1.1f%%', startangle=140,labels=df2['AGE'])
plt.title('No of age group where lstat < 2')
plt.savefig("output.png")