#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()


# In[2]:


import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.context import SparkContext
from pyspark.sql.functions import * 
from pyspark.sql.types import * 
from pyspark.sql import SparkSession


# In[14]:


spark = SparkSession     .builder     .appName("Patient Condition DataFrame")     .config("spark.some.config.option", "some-value")     .getOrCreate()

conditionSchema = StructType([
    StructField("START", StringType(), True),
    StructField("STOP", StringType(), True),
    StructField("PATIENT", StringType(), True),
    StructField("ENCOUNTER", StringType(), True),
    StructField("CODE", StringType(), True),
    StructField("DESCRIPTION", StringType(), True),
])

patientDataframe = spark.read.csv(
    r"C:\Users\My Computer\OneDrive\Documents\TUGAS ABD 4\dataset\conditions.csv", 
    header=True, schema=conditionSchema, sep=",")
patientDataframe.show(50)


# In[15]:


spark = SparkSession     .builder     .appName("Patient DataFrame")     .config("spark.some.config.option", "some-value")     .getOrCreate()

patientSchema = StructType([
    StructField("Id", StringType(), True),
    StructField("BIRTHDATE", StringType(), True),
    StructField("DEATHDATE", StringType(), True),	
    StructField("SSN", StringType(), True),	
    StructField("DRIVERS", StringType(), True),	
    StructField("PASSPORT", StringType(), True),	
    StructField("PREFIX", StringType(), True),	
    StructField("FIRST", StringType(), True),	
    StructField("LAST", StringType(), True),	
    StructField("SUFFIX", StringType(), True),	
    StructField("MAIDEN", StringType(), True),
    StructField("MARITAL", StringType(), True),	
    StructField("RACE", StringType(), True),
    StructField("ETHNICITY", StringType(), True),	
    StructField("GENDER", StringType(), True),	
    StructField("BIRTHPLACE", StringType(), True),	
    StructField("ADDRESS", StringType(), True),	
    StructField("CITY", StringType(), True),
    StructField("STATE", StringType(), True),	
    StructField("COUNTY", StringType(), True),	
    StructField("ZIP", StringType(), True),	
    StructField("LAT", StringType(), True),	
    StructField("LON", StringType(), True),	
    StructField("HEALTHCARE_EXPENSES", StringType(), True),	
    StructField("HEALTHCARE_COVERAGE", StringType(), True),
])


patientInfo = spark.read.csv(
    r"C:\Users\My Computer\OneDrive\Documents\TUGAS ABD 4\dataset\patients_covid.csv", 
    header=True, schema=patientSchema, sep=",")
patientInfo.show()


# In[29]:


spark = SparkSession     .builder     .appName("Patient DataFrame")     .config("spark.some.config.option", "some-value")     .getOrCreate()


patientcareplans = spark.read.csv(
    r"C:\Users\My Computer\OneDrive\Documents\TUGAS ABD 4\dataset\careplans.csv", 
    inferSchema=True, header=True)
patientcareplans.show()


# In[32]:



DataFrame=patientDataframe.join(patientInfo,patientDataframe.PATIENT == patientInfo.Id,"inner").join(patientcareplans,patientDataframe.PATIENT == patientcareplans.PATIENT_cp,"inner")

DataFrame.show()


# In[34]:


DataFrame.select(DataFrame['LAST'], DataFrame['DESCRIPTION'], DataFrame['DESCRIPTION_cp'], DataFrame['REASONDESCRIPTION']).where (DataFrame.REASONDESCRIPTION=='COVID-19').show()


# In[ ]:




