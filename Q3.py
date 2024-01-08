#Q3
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as fc
spark = SparkSession.builder.appName("App").getOrCreate()


# Using the country data set, write a Spark RDD script for each of the following questions.
# You use “import pyspark.sql.functions as fc”. Show the output of your script.
# Note your script should start by turning dataframe into rdd, e.g., country.rdd, and work on the rdd to answer the question.
city_rdd = spark.read.json('city.json').rdd
country_rdd = spark.read.json('country.json').rdd
countryLanguage_rdd = spark.read.json('countrylanguage.json').rdd


#a. Find top-10 most populated cities. Return the cities and their populations in the descending order of population.

#Method 1
city_rdd.map(lambda r: (r['Population'], r['Name'])).sortByKey(ascending=False).take(10)
    #[(10500000, 'Mumbai (Bombay)'), (9981619, 'Seoul'), (9968485, 'SÃ£o Paulo'), (9696300, 'Shanghai'), (9604900, 'Jakarta'), (9269265, 'Karachi'), (8787958, 'Istanbul'), (8591309, 'Ciudad de MÃ©xico'), (8389200, 'Moscow'), (8008278, 'New York')]


#b. Find out the top-3 most popular official languages ranked by the number of countries which speak the language.
countryLanguage_rdd.filter(lambda r: (r['IsOfficial']) == 'T').map(lambda r: (r['Language'], 1))\
    .reduceByKey(lambda x, y: x + y).sortBy(lambda r: r[1], ascending=False).take(3)
    #[('English', 44), ('Arabic', 22), ('Spanish', 20)]


#c. Find out names of countries in North America which have at least two official languages.
country_rdd.filter(lambda r: (r['Continent']) == 'North America').map(lambda r: (r['Code'], r['Name']))\
    .join(countryLanguage_rdd.filter(lambda r: (r['IsOfficial']) == 'T').map(lambda r: (r['CountryCode'], r['Language']))\
          .distinct().map(lambda r: (r[0], 1)).reduceByKey(lambda x, y: x + y).filter(lambda r: (r[1]) >= 2)\
          .map(lambda r: (r[0], 1))).map(lambda r: (r[0], r[1])).join(country_rdd.map(lambda r: (r['Code'], r['Name']))).map(lambda r: (r[1][1])).collect()
    #['Netherlands Antilles', 'Canada', 'Greenland']


#d.  Find out how many countries in Europe have English as their official language.
country_rdd.filter(lambda r: (r['Continent']) == 'Europe').map(lambda r: (r['Code'], r['Name']))\
    .join(countryLanguage_rdd.filter(lambda r: (r['Language']) == 'English').filter(lambda r: (r['IsOfficial']) == 'T')\
          .map(lambda r: (r['CountryCode'], r['Language'])).distinct().map(lambda r: (r[0], 1))).map(lambda r: (r[0], r[1]))\
    .join(country_rdd.map(lambda r: (r['Code'], r['Name']))).map(lambda r: (r[1][1])).distinct().count()
    #4


#e.  Find out names of the countries where English is an official language and French is an unofficial language.
english_official_rdd = countryLanguage_rdd.filter(lambda r: (r['Language']) == 'English')\
    .filter(lambda r: (r['IsOfficial']) == 'T').map(lambda r: (r['CountryCode'], r['Language']))
french_unofficial_rdd = countryLanguage_rdd.filter(lambda r: (r['Language']) == 'French')\
    .filter(lambda r: (r['IsOfficial']) == 'F').map(lambda r: (r['CountryCode'], r['Language']))
country_rdd.map(lambda r: (r['Code'], r['Name'])).join(english_official_rdd).map(lambda r: (r[0], r[1][1]))\
    .join(french_unofficial_rdd).map(lambda r: (r[0], r[1][1])).join(country_rdd.map(lambda r: (r['Code'], r['Name']))).map(lambda r: (r[1][1])).collect()
    #['United States', 'Virgin Islands, U.S.']
