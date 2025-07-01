from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, round as pyspark_round, col

spark = SparkSession.builder.appName("TitanicAnalysis").getOrCreate()
df = spark.read.csv("titanic.csv", header=True, inferSchema=True)


df1 = (
df.groupBy("Pclass", "Sex").agg(pyspark_round(avg("Survived"), 2).alias("SurvivalRate")).orderBy("Pclass", col("SurvivalRate").desc()))
df1.show()


df2 = (
    df.filter((col("Fare").isNotNull()) & (col("Age").isNotNull())).groupBy("Embarked").agg(
    pyspark_round(avg("Fare"), 2).alias("AvgFare"),pyspark_round(avg("Age"), 2).alias("AvgAge")).orderBy(col("AvgFare").desc())
)

df2.show()


df3 = (
df.filter(col("Survived") == 1).select("Name", "Pclass", "Sex", "Fare", "Cabin").orderBy(col("Fare").desc()).limit(5))
df3.show(truncate=False)
