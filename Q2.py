#Q2
# User
# [Spark DataFrame, 30 points] Using the country data set which contains country.json, city.json, and countrylanguage.json (see in class demo), write a Spark DataFrame script for each of the following questions. Show the output of your script.
#   a.  Find the top 10 most populated cities. Return the cities and their populations in the descending order of population.
# 	b.  Find out the top-3 most popular official languages based on the number of countries which speak the language.
# 	c.  Find out names of countries in North America which have at least two official languages.
# 	d.  Find out names of countries in Europe which have English as their official language.
# 	e.  Find out names of the countries where English is an official language and French is an  unofficial language.

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as fc
spark = SparkSession.builder.appName("App").getOrCreate()

city = spark.read.json('city.json')
country = spark.read.json('country.json')
countryLanguage = spark.read.json('countrylanguage.json')


#a.  Find the top 10 most populated cities. Return the cities and their populations in the descending order of population.
city.select('Name', 'Population').orderBy(fc.desc('Population')).show(10)
    # +-----------------+----------+
    # |             Name|Population|
    # +-----------------+----------+
    # |  Mumbai (Bombay)|  10500000|
    # |            Seoul|   9981619|
    # |       SÃ£o Paulo|   9968485|
    # |         Shanghai|   9696300|
    # |          Jakarta|   9604900|
    # |          Karachi|   9269265|
    # |         Istanbul|   8787958|
    # |Ciudad de MÃ©xico|   8591309|
    # |           Moscow|   8389200|
    # |         New York|   8008278|
    # +-----------------+----------+
    # only showing top 10 rows


#b.  Find out the top-3 most popular official languages based on the number of countries which speak the language.
countryLanguage.filter(countryLanguage['IsOfficial'] == 'T').groupBy('Language').count().orderBy(fc.desc('count')).show(3)
    # +--------+-----+
    # |Language|count|
    # +--------+-----+
    # | English|   44|
    # |  Arabic|   22|
    # | Spanish|   20|
    # +--------+-----+
    # only showing top 3 rows


#c.  Find out names of countries in North America which have at least two official languages.
country.join(countryLanguage, country['Code'] == countryLanguage['CountryCode'])\
    .filter(country['Continent'] == 'North America')\
    .filter(countryLanguage['IsOfficial'] == 'T')\
    .groupBy('Name').agg(fc.countDistinct('Language').alias('LanguageCount'))\
    .filter('LanguageCount >= 2').select('Name').show()
    # +--------------------+
    # |                Name|
    # +--------------------+
    # |Netherlands Antilles|
    # |              Canada|
    # |           Greenland|
    # +--------------------+


#d.  Find out names of countries in Europe which have English as their official language.
country.join(countryLanguage, country['Code'] == countryLanguage['CountryCode'])\
    .filter(countryLanguage['Language'] == 'English')\
    .filter(countryLanguage['IsOfficial'] == 'T')\
    .filter(country['Continent'] == 'Europe').select('Name').show()
    # +--------------+
    # |          Name|
    # +--------------+
    # |United Kingdom|
    # |     Gibraltar|
    # |       Ireland|
    # |         Malta|
    # +--------------+


#e.  Find out names of the countries where English is an official language and French is an unofficial language.
english_official = countryLanguage.filter((countryLanguage['Language'] == 'English') & (countryLanguage['IsOfficial'] == 'T'))
french_unofficial = countryLanguage.filter((countryLanguage['Language'] == 'French') & (countryLanguage['IsOfficial'] == 'F'))
country.join(english_official, country['Code'] == english_official['CountryCode'])\
    .join(french_unofficial, country['Code'] == french_unofficial['CountryCode'], 'inner')\
    .select('Name').show()
    # +--------------------+
    # |                Name|
    # +--------------------+
    # |       United States|
    # |Virgin Islands, U.S.|
    # +--------------------+