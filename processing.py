from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window

spark=SparkSession.builder.appName("WeatherSystem").master("local[*]").getOrCreate()

df=spark.read.parquet("hdfs://localhost:9000/weather/data/processed/weather_processed.parquet")




#average temperature per city

#df.createOrReplcarTempView("w_view")
df.createOrReplaceTempView("weather")



city_summary_yearly=spark.sql("""
SELECT
  city,
  year,
  ROUND(AVG(temperature_2m_max),2) AS avg_max_temp,
  ROUND(AVG(temperature_2m_min),2) AS avg_min_temp,
  ROUND(MAX(temperature_2m_max),2) AS highest_temp,
  ROUND(MIN(temperature_2m_min),2) AS lowest_temp,
ROUND(AVG(temperature_2m_max - temperature_2m_min),2) as temp_range,
AVG(temperature_2m_max - apparent_temperature_max) AS heat_stress,
 CASE
                       when heat_stress < 0
                       then "cold"
                       when heat_stress > 0
                       then "heat"
                       ELSE 'neutral'
END AS heat_stress_type,
ROUND(SUM(rain_sum),2) as yearly_rainfall,
MAX(wind_gusts_10m_max) AS max_wind_gust
                       
FROM weather
GROUP BY city, year
ORDER BY city, year;
                  
"""
)


city_summary_monthly=spark.sql("""
SELECT
  city,
  year,
  month,
  ROUND(AVG(temperature_2m_max),2) AS avg_max_temp,
  ROUND(AVG(temperature_2m_min),2) AS avg_min_temp,
  ROUND(MAX(temperature_2m_max),2) AS highest_temp,
  ROUND(MIN(temperature_2m_min),2) AS lowest_temp,
ROUND(AVG(temperature_2m_max - temperature_2m_min),2) as temp_range,
 CASE
                       when AVG(temperature_2m_max-apparent_temperature_max) < 0
                       then "cold"
                       when AVG(temperature_2m_max-apparent_temperature_max)> 0
                       then "heat"
                       ELSE 'neutral'
END AS heat_stress_type,
ROUND(SUM(rain_sum),2) as monthly_rainfall,
MAX(wind_gusts_10m_max) AS max_wind_gust
                       
FROM weather
GROUP BY city, year,month
ORDER BY city, year,month;
                  
"""
)



city_summary_weekly=spark.sql("""
SELECT
  city,
  year,
  weekofyear(date),
  ROUND(AVG(temperature_2m_max),2) AS avg_max_temp,
  ROUND(AVG(temperature_2m_min),2) AS avg_min_temp,
  ROUND(MAX(temperature_2m_max),2) AS highest_temp,
  ROUND(MIN(temperature_2m_min),2) AS lowest_temp,
ROUND(AVG(temperature_2m_max - temperature_2m_min),2) as temp_range,
 CASE
                       when AVG(temperature_2m_max-apparent_temperature_max) < 0
                       then "cold"
                       when AVG(temperature_2m_max-apparent_temperature_max)> 0
                       then "heat"
                       ELSE 'neutral'
END AS heat_stress_type,
ROUND(SUM(rain_sum),2) as weekly_rainfall,
MAX(wind_gusts_10m_max) AS max_wind_gust
                       
FROM weather
GROUP BY city, year,weekofyear(date)
ORDER BY city, year,weekofyear(date);
                  
"""
)

city_year_rainfall=spark.sql("""
                             
SELECT
  city,
  year,
  ROUND(SUM(rain_sum), 2) AS total_rainfall,
  ROUND(AVG(rain_sum), 2) AS avg_daily_rainfall,
  COUNT(*) AS total_days,
  SUM(CASE WHEN rain_sum > 0 THEN 1 ELSE 0 END) AS num_rainy_days_per_year
FROM weather
GROUP BY city, year
ORDER BY city, year;         
""")

city_seasonal_rainfall_summary=spark.sql(
"""
SELECT
  city,
  year,
  CASE
    WHEN month IN (12,1,2) THEN 'Winter'
    WHEN month IN (3,4,5,6) THEN 'Summer'
    WHEN month IN (7,8,9) THEN 'Monsoon'
    WHEN month IN (10,11) THEN 'Post-Monsoon'
  END AS season,
  ROUND(SUM(rain_sum), 2) AS seasonal_rainfall,
  SUM(CASE WHEN rain_sum > 0 THEN 1 ELSE 0 END) AS rainy_days
FROM weather
GROUP BY city, year, season
ORDER BY city, year, season;
"""
)


city_seasonal_temp_summary = spark.sql("""
SELECT
  city,
  year,
  CASE
    WHEN month IN (12, 1, 2) THEN 'Winter'
    WHEN month IN (3, 4, 5, 6) THEN 'Summer'
    WHEN month IN (7, 8, 9) THEN 'Monsoon'
    WHEN month IN (10, 11) THEN 'Post-Monsoon'
  END AS season,
  ROUND(AVG(temperature_2m_max), 2) AS avg_max_temp,
  ROUND(AVG(temperature_2m_min), 2) AS avg_min_temp
FROM weather
GROUP BY city, year, season
ORDER BY city, year, season
""")

city_seasonal_wind_summary = spark.sql("""
SELECT
  city,
  year,
  CASE
    WHEN month IN (12,1,2) THEN 'Winter'
    WHEN month IN (3,4,5,6) THEN 'Summer'
    WHEN month IN (7,8,9) THEN 'Monsoon'
    WHEN month IN (10,11) THEN 'Post-Monsoon'
  END AS season,
  ROUND(AVG(wind_speed_10m_max), 2) AS avg_wind_speed,
  MAX(wind_gusts_10m_max) AS max_wind_gust
FROM weather
GROUP BY city, year, season
ORDER BY city, year, season
""")


# result=df.select(col("day"))
# #result=weather_code.filter(col("city")=="Delhi")
# city_summary_yearly.show()
# city_summary_monthly.show()
# city_summary_weekly.show()
# city_seasonal_temp_summary.show()
# city_seasonal_rainfall_summary.show()


city_summary_yearly.write.mode("overwrite") \
    .parquet("hdfs://localhost:9000/weather/analytics/city_summary_yearly")

city_summary_monthly.write.mode("overwrite") \
    .parquet("hdfs://localhost:9000/weather/analytics/city_summary_monthly")

city_summary_weekly.write.mode("overwrite") \
    .parquet("hdfs://localhost:9000/weather/analytics/city_summary_weekly")

city_seasonal_temp_summary.write.mode("overwrite") \
    .parquet("hdfs://localhost:9000/weather/analytics/city_seasonal_temp")

city_seasonal_rainfall_summary.write.mode("overwrite") \
    .parquet("hdfs://localhost:9000/weather/analytics/city_seasonal_rainfall")

city_seasonal_wind_summary.write.mode("overwrite") \
    .parquet("hdfs://localhost:9000/weather/analytics/city_seasonal_wind")


spark.stop()


