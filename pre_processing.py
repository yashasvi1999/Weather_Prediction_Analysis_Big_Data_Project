from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("WeatherSpark") \
     .master("local[*]") \
    .getOrCreate()

df=spark.read.option("header","true").option("inferSchema","true")\
    .csv("hdfs://localhost:9000/weather/data/raw_data/")
#added now
from pyspark.sql.functions import to_date

df = df.withColumn("date", to_date("date"))


df.printSchema()

df.filter(
    " OR ".join([f"{c} IS NULL" for c in df.columns])
).count()


from pyspark.ml.feature import StringIndexer , OneHotEncoder

indexer = StringIndexer(
    inputCol= "city",
    outputCol = "city_index",
    handleInvalid= "keep"
)

encoder = OneHotEncoder(
    inputCol= "city_index",
    outputCol= "city_vector"
)

df = indexer.fit(df).transform(df)
df = encoder.fit(df).transform(df)


from pyspark.sql.functions import col

outlier_cols = [
    "temperature_2m_max",
    "temperature_2m_min",
    "apparent_temperature_max",
    "apparent_temperature_min",
    "precipitation_sum",
    "rain_sum",
    "wind_speed_10m_max",
    "wind_gusts_10m_max"
]

for c in outlier_cols:
    Q1, Q3 = df.approxQuantile(c,[0.25,0.75],0.01)

    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    outlier_count  = df.filter(
        (col(c) < lower_bound) |(col(c) > upper_bound) 
    ).count()

    print(f"{c}: Outliers = {outlier_count}")



Q1, Q3 = df.approxQuantile(
    "temperature_2m_max", [0.25, 0.75], 0.01
)

IQR = Q3 - Q1
lower_bound = Q1 - 1.5 * IQR
upper_bound = Q3 + 1.5 * IQR

df.filter(
    (col("temperature_2m_max") < lower_bound) |
    (col("temperature_2m_max") > upper_bound)
).select("city", "date", "temperature_2m_max").show(10)

print("Lower:", lower_bound, "Upper:", upper_bound)


from pyspark.sql.functions import when

temp_cols = [
    "temperature_2m_max",
    "temperature_2m_min",
    "apparent_temperature_max",
    "apparent_temperature_min"
]

for c in temp_cols:
    Q1, Q3 = df.approxQuantile(c, [0.25, 0.75], 0.01)
    IQR = Q3 - Q1
    lb = Q1 - 1.5 * IQR
    ub = Q3 + 1.5 * IQR

    df = df.withColumn(
        c,
        when(col(c) < lb, lb)
        .when(col(c) > ub, ub)
        .otherwise(col(c))
    )
from pyspark.sql.functions import when

wind_cols = ["wind_speed_10m_max", "wind_gusts_10m_max"]

for c in wind_cols:
    Q1,Q3 = df.approxQuantile(c,[0.25,0.75],0.01)
    IQR = Q3 - Q1
    lb = Q1 - 1.5 * IQR
    ub = Q3 + 1.5 * IQR

    df = df.withColumn(
        c,
        when(col(c) < lb,lb)
        .when(col(c) > ub,ub)
        .otherwise(col(c))
    )

from pyspark.sql.functions import log1p

df = df.withColumn("rain_sum_log", log1p(col("rain_sum")))
df = df.withColumn("precipitation_sum_log", log1p(col("precipitation_sum")))

for c in outlier_cols:
    Q1, Q3 = df.approxQuantile(c, [0.25, 0.75], 0.01)
    IQR = Q3 - Q1
    lb = Q1 - 1.5 * IQR
    ub = Q3 + 1.5 * IQR

    count = df.filter((col(c) < lb) | (col(c) > ub)).count()
    print(f"{c}: Outliers after handling = {count}")

df.groupBy("city").count().orderBy("count",ascending = False).show()


# 21)
from pyspark.sql.functions import dayofmonth, dayofweek, month,year

df = df.withColumn("day" , dayofmonth("date"))\
       .withColumn("month" , month("date"))\
       .withColumn("day_of_week" , dayofweek("date"))\
       .withColumn("year",year("date"))

# Why?
# Weather is seasonal
# Models learn weekly/monthly patterns



# 22)
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("date")


# 23)
from pyspark.sql.functions import lag

df = df.withColumn("temp_lag_1", lag("temperature_2m_max", 1).over(w)) \
       .withColumn("temp_lag_2", lag("temperature_2m_max", 2).over(w))


# 24)
from pyspark.sql.functions import avg

rolling_window = w.rowsBetween(-2, 0)

df = df.withColumn(
    "temp_rolling_3",
    avg("temperature_2m_max").over(rolling_window))


df.select(
    "city", "date",
    "temperature_2m_max",
    "temp_lag_1", "temp_lag_2",
    "temp_rolling_3",
    "day", "month", "day_of_week"
).show(5)

# 26)
from pyspark.ml.feature import VectorAssembler

feature_cols = [
    "temperature_2m_max",
    "temperature_2m_min",
    "apparent_temperature_max",
    "apparent_temperature_min",
    "wind_speed_10m_max",
    "wind_gusts_10m_max",
    "rain_sum_log",
    "temp_lag_1",
    "temp_lag_2",
    "temp_rolling_3"
]

assembler = VectorAssembler(
    inputCols= feature_cols,
    outputCol= "features"
)
df = assembler.transform(df)

from pyspark.sql.functions import col, count, when

df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in feature_cols
]).show(truncate=False)


# 28)
df = df.dropna(subset=feature_cols)



df = df.drop("features", "scaled_features")

# 31)
df = df.dropna(subset=[
    "temperature_2m_max",
    "temperature_2m_min",
    "apparent_temperature_max",
    "apparent_temperature_min",
    "wind_speed_10m_max",
    "wind_gusts_10m_max",
    "rain_sum_log",
    "temp_lag_1",
    "temp_lag_2",
    "temp_rolling_3"
])


# 32)
from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(
    inputCols=[
        "temperature_2m_max",
        "temperature_2m_min",
        "apparent_temperature_max",
        "apparent_temperature_min",
        "wind_speed_10m_max",
        "wind_gusts_10m_max",
        "rain_sum_log",
        "temp_lag_1",
        "temp_lag_2",
        "temp_rolling_3"
    ],
    outputCol="features"
)

df = assembler.transform(df)



# 33)
df.select("features").show(5, truncate=False)

# 35)
from pyspark.ml.feature import MinMaxScaler

scaler = MinMaxScaler(
    inputCol="features",
    outputCol="scaled_features"
)

scaler_model = scaler.fit(df)
df = scaler_model.transform(df)


# 37)
critical_cols = [
    "city",
    "date",
    "temperature_2m_max",
    "temperature_2m_min"
]

# 38)
from pyspark.sql.functions import col

null_count = df.filter(
    " OR ".join([f"{c} IS NULL" for c in critical_cols])
).count()

print("Critical NULL rows:", null_count)


# 39)
MIN_TEMP = -10
MAX_TEMP = 60

invalid_temp_count = df.filter(
    (col("temperature_2m_max") < MIN_TEMP) |
    (col("temperature_2m_max") > MAX_TEMP) |
    (col("temperature_2m_min") < MIN_TEMP) |
    (col("temperature_2m_min") > MAX_TEMP)
).count()

print("Invalid temperature rows:", invalid_temp_count)


# 40)
from pyspark.sql.window import Window
from pyspark.sql.functions import lag

w = Window.partitionBy("city").orderBy("date")

df_with_lag = df.withColumn(
    "prev_date",
    lag("date").over(w)
)

non_monotonic_count = df_with_lag.filter(
    col("prev_date").isNotNull() &
    (col("date") < col("prev_date"))
).count()

print("Non-monotonic timestamp rows:", non_monotonic_count)


# 41)
if null_count > 0:
    raise Exception("Data Quality Failed: NULLs in critical columns")

if invalid_temp_count > 0:
    raise Exception("Data Quality Failed: Temperature out of range")

if non_monotonic_count > 0:
    raise Exception("Data Quality Failed: Timestamp not monotonic")

# 42)
print({
    "null_critical": null_count,
    "invalid_temp": invalid_temp_count,
    "non_monotonic_time": non_monotonic_count
})

# 43)
df.write.mode("overwrite").parquet("hdfs://localhost:9000/weather/data/processed/weather_processed.parquet")




spark.stop()