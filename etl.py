import pandas as pd
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, when, isnan, count
from datetime import datetime
from pyspark.sql.functions import year, month, dayofmonth, hour,\
    weekofyear, date_format, monotonically_increasing_id
from pyspark.sql.types import StructType as R, StructField as Fld,\
    DecimalType, DoubleType as Dbl, StringType as Str, IntegerType as Int,\
    TimestampType as Timestamp, DateType as Date, LongType as Long,\
    datetime as DateTime

#----------------------------------------------------------------------------

def create_spark_session():
    """
    This procedure creates a Spark session to process data.
    """
    spark = SparkSession\
        .builder\
        .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .getOrCreate() 
    print("*** Spark session created.")
    return spark

#----------------------------------------------------------------------------

def process_immigration_data(spark, input_data_immigration, output_data_staging):
    """
    This procedure loads data from the input filepath, processes it using Spark and then writes parquet files to the output filepath.
      
    INPUTS:
    * spark - the Spark session
    * input_data_immigration - location of the input data of sas7bdat immigration files
    * output_data_staging - location of where the output staging parquet files will be stored
    
     OUTPUTS:
    * immigration staging table - output folder with parquet files of immigration data 
    """
    
    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')
    start = datetime.now()
    print("\n *** Load immigration data: {}\n".format(start))
    
    # get filepath to immigration data file
    immigration_data = input_data_immigration        
    
    # read immigration data file
    immigration_schema = R([
        Fld("cicid", Dbl(), False),
        Fld("i94yr", Dbl(), False),
        Fld("i94mon", Dbl(), False),
        Fld("i94cit", Dbl(), False),
        Fld("i94res", Dbl(), False),
        Fld("i94port", Str()),
        Fld("arrdate", Dbl()),
        Fld("i94mode", Dbl()),
        Fld("i94addr", Str()), 
        Fld("depdate", Dbl()), 
        Fld("i94bir", Dbl()),
        Fld("i94visa", Dbl()),
        Fld("count", Dbl()),
        Fld("dtadfile", Str()),
        Fld("visapost", Str()),
        Fld("occup", Str()),
        Fld("entdepa", Str()),
        Fld("entdepd", Str()),
        Fld("entdepu", Str()),
        Fld("matflag", Str()),
        Fld("biryear", Dbl()),
        Fld("dtaddto", Str()),
        Fld("gender", Str()),
        Fld("insnum", Str()),
        Fld("airline", Str()),
        Fld("admnum", Dbl()),
        Fld("fltno", Str()),
        Fld("visatype", Str())
    ])
    immigration_df_spark = spark.read\
                           .format('com.github.saurfang.sas.spark')\
                           .load(immigration_data, schema=immigration_schema)
    
    # show schema and data sample 
    immigration_df_spark.printSchema() 
    immigration_df_spark.show(3)
     
    # write immigration staging table to parquet
    immigration_path = output_data_staging\
         + "immigration_staging.parquet" + "_" + timestamp
    print("*** Write immigration staging parquet file")     
    print(f"*** Output Path: {immigration_path}")
    immigration_df_spark.write.parquet(immigration_path, mode="overwrite")
    
    # read immigration parquet file back to Spark 
    print("*** Read immigration staging parquet file")     
    immigration_df_spark = spark.read.parquet(immigration_path) 
    
    end = datetime.now()
    print("\n *** Load immigration data complete: {}\n".format(end)) 
    print("*** Total time: {}".format(end-start)) 
 
    return immigration_df_spark

#----------------------------------------------------------------------------

def process_country_data(spark, input_data_countries, output_data_staging):
    """
    This procedure loads data from the input filepath, processes it using Spark and then writes parquet files to the output filepath.
      
    INPUTS:
    * spark - the Spark session
    * input_data_countries - location of the input data of csv country files
    * output_data_staging - location of where the output staging parquet files will be stored
    
     OUTPUTS:
    * countries staging table - output folder with parquet files of countries data 
    """
    
    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')
    start = datetime.now()
    print("\n *** Load country data: {}\n".format(start))
    
    # get filepath to countries data file
    countries_data = input_data_countries        
    
    # read countries data file
    country_schema = R([
        Fld("i94cit", Dbl()),
        Fld("country_name", Str()) 
    ])
    countries_df_spark = spark.read\
                         .csv(countries_data, schema=country_schema, header="true", sep=";") 
    
    # show schema and data sample 
    countries_df_spark.printSchema() 
    countries_df_spark.show(3)
     
    # write countries staging table to parquet
    countries_path = output_data_staging\
         + "countries_staging.parquet" + "_" + timestamp
    print("*** Write countries staging parquet file") 
    print(f"*** Output Path: {countries_path}")
    countries_df_spark.write.parquet(countries_path, mode="overwrite")
    
    # read countries parquet file back to Spark     
    print("*** Read countries staging parquet file") 
    countries_df_spark = spark.read.parquet(countries_path) 
    
    end = datetime.now()
    print("\n *** Load country data complete: {}\n".format(end)) 
    print("*** Total time: {}".format(end-start)) 
 
    return countries_df_spark

#----------------------------------------------------------------------------

def process_port_data(spark, input_data_ports, output_data_staging):
    """
    This procedure loads data from the input filepath, processes it using Spark and then writes parquet files to the output filepath.
      
    INPUTS:
    * spark - the Spark session
    * input_data_ports - location of the input data of csv port files
    * output_data_staging - location of where the output staging parquet files will be stored
    
     OUTPUTS:
    * ports staging table - output folder with parquet files of ports data 
    """
    
    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')    
    start = datetime.now()
    print("\n *** Load port data: {}\n".format(start))
    
    # get filepath to ports data file
    ports_data = input_data_ports        
    
    # read ports data file
    port_schema = R([ 
        Fld("i94port", Str()), 
        Fld("port_name", Str()),
        Fld("port_state_code", Str()) 
    ])
    ports_df_spark = spark.read\
                     .csv(ports_data, schema=port_schema, header="true") 
    
    # show schema and data sample 
    ports_df_spark.printSchema() 
    ports_df_spark.show(3)
     
    # write ports staging table to parquet
    ports_path = output_data_staging\
         + "ports_staging.parquet" + "_" + timestamp 
    print("*** Write ports staging parquet file")
    print(f"*** Output Path: {ports_path}")
    ports_df_spark.write.parquet(ports_path, mode="overwrite")
    
    # read ports parquet file back to Spark 
    print("*** Read ports staging parquet file")
    ports_df_spark = spark.read.parquet(ports_path) 
    
    end = datetime.now()
    print("\n *** Load port data complete: {}\n".format(end)) 
    print("*** Total time: {}".format(end-start))
 
    return ports_df_spark

#----------------------------------------------------------------------------

def process_city_demographic_data(spark, input_data_cities, output_data_staging):
    """
    This procedure loads data from the input filepath, processes it using Spark and then writes parquet files to the output filepath.
      
    INPUTS:
    * spark - the Spark session
    * input_data_cities - location of the input data of csv city demographic files
    * output_data_staging - location of where the output staging parquet files will be stored
    
     OUTPUTS:
    * city demographics staging table - output folder with parquet files of city demographics data 
    """
    
    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')
    start = datetime.now()
    print("\n *** Load city demographic data: {}\n".format(start))
    
    # get filepath to city demographics data file
    cities_data = input_data_cities        
    
    # read city demographics data file
    city_demographics_schema = R([
        Fld("city", Str(), False),
        Fld("state", Str(), False),
        Fld("median_age", Dbl()),
        Fld("male_population", Int()),
        Fld("female_population", Int()),
        Fld("total_population", Int()),
        Fld("number_of_veterans", Int()),
        Fld("foreign_born", Int()),
        Fld("average_household_size", Dbl()),
        Fld("state_code", Str()),
        Fld("race", Str()),
        Fld("count", Int()) 
    ])
    city_demographics_df_spark = spark.read\
        .csv(cities_data, schema=city_demographics_schema, header="true", sep=";") 
    
    # show schema and data sample 
    city_demographics_df_spark.printSchema() 
    city_demographics_df_spark.show(3)
     
    # write city demographics staging table to parquet
    cities_path = output_data_staging\
         + "city_demographics_staging.parquet" + "_" + timestamp 
    print("*** Write city demographics staging parquet file")
    print(f"*** Output Path: {cities_path}")
    city_demographics_df_spark.write.parquet(cities_path, mode="overwrite")
    
    # read cities parquet file back to Spark 
    print("*** Read city demographics staging parquet file")
    city_demographics_df_spark = spark.read.parquet(cities_path) 
        
    end = datetime.now()
    print("\n *** Load city demographic data complete: {}\n".format(end)) 
    print("*** Total time: {}".format(end-start))
 
    return city_demographics_df_spark

#----------------------------------------------------------------------------

def clean_immigration_data(spark, immigration_df_spark):
    """
    This procedure cleans immigration data:
    Null values will be replaced by either NA (string), or 0.0 (double)
    i94addr field with Null values will be replaced by 99, which represents 'All Other Codes'
    date fields will be converted from SAS format to YYYY-MM-DD format
   
    INPUTS:
    * spark - the Spark session 
    
     OUTPUTS:
    * immigration_df_spark_clean - cleaned immigration spark dataframe
    """
    
    start = datetime.now()
    print("\n *** Clean immigration data: {}\n".format(start))

    # Clean immigration data
    immigration_df_spark_clean = immigration_df_spark.withColumn('depdate',\
                             when(col('depdate').isNull(),0.0).otherwise(col("depdate")))
    immigration_df_spark_clean = immigration_df_spark_clean.withColumn('i94addr',\
                             when(col('i94addr').isNull(),'99').otherwise(col("i94addr")))

    columns = ['dtadfile','visapost','occup','entdepd','entdepu','matflag','insnum']
    for column in columns:
        immigration_df_spark_clean = immigration_df_spark_clean.withColumn(column,\
                                 when(col(column).isNull(),'NA').otherwise(col(column)))
        
    immigration_df_spark_clean = immigration_df_spark_clean.filter(immigration_df_spark_clean.admnum != 0.0)
    immigration_df_spark_clean.show(3)   
    
    #Convert date fields from SAS format to YYYY-MM-DD format
    epoch = DateTime.datetime(1960, 1, 1) 
    get_date = udf(lambda x: epoch + DateTime.timedelta(days=x),Date())

    immigration_df_spark_clean = immigration_df_spark_clean\
                             .withColumn('arrdate', get_date('arrdate'))\
                             .withColumn('depdate', get_date('depdate'))

    #Reset previous depdate Null values
    immigration_df_spark_clean = immigration_df_spark_clean\
                             .withColumn('depdate', when(col("depdate") == "1960-01-01", "NULL")\
                             .otherwise(col("depdate"))) 
    immigration_df_spark_clean.show(3)    
    
    end = datetime.now()
    print("\n *** Clean immigration data complete: {}\n".format(end)) 
    print("*** Total time: {}".format(end-start))
 
    return immigration_df_spark_clean

#----------------------------------------------------------------------------

def clean_ports_data(spark, ports_df_spark):
    """
    This procedure cleans ports data: 
    port_state_code field with NaN values will be replaced by XXX, which represents 'NOT REPORTED/UNKNOWN'
   
    INPUTS:
    * spark - the Spark session 
    
     OUTPUTS:
    * ports_df_spark_clean - cleaned ports spark dataframe
    """

    start = datetime.now()
    print("\n *** Clean port data: {}\n".format(start))

    # Clean ports data
    ports_df_spark_clean = ports_df_spark\
                           .withColumn('port_state_code',when(isnan('port_state_code'),'XXX')\
                           .otherwise(col("port_state_code")))
    ports_df_spark_clean.show(3) 
  
    end = datetime.now()
    print("\n *** Clean port data complete: {}\n".format(end)) 
    print("*** Total time: {}".format(end-start))
  
    return ports_df_spark_clean

#----------------------------------------------------------------------------

def clean_city_demographics_data(spark, city_demographics_df_spark):
    """
    This procedure cleans city demogrpahics data: 
    Null values will be replaced by either NA (string), 0 (integer) or 0.0 (double)
   
    INPUTS:
    * spark - the Spark session 
    
     OUTPUTS:
    * city_demographics_df_spark_clean - cleaned city demographics spark dataframe
    """

    start = datetime.now()
    print("\n *** Clean city demographic data: {}\n".format(start))
    
    # Clean city demographics data
    city_demographics_df_spark_clean = city_demographics_df_spark\
                                   .withColumn('average_household_size',when(col('average_household_size').isNull(),0.0)\
                                   .otherwise(col("average_household_size")))

    columns = ['male_population','female_population','foreign_born','number_of_veterans']
    for column in columns:
        city_demographics_df_spark_clean = city_demographics_df_spark_clean\
                                           .withColumn(column,when(col(column).isNull(),0)\
                                           .otherwise(col(column)))
    city_demographics_df_spark_clean.show(3)  
    
    end = datetime.now()
    print("\n *** Clean city demographic data complete: {}\n".format(end)) 
    print("*** Total time: {}".format(end-start)) 
  
    return city_demographics_df_spark_clean

#----------------------------------------------------------------------------

def process_dim_state_demographics_data(spark, city_demographics_df_spark_clean, output_data_model):
    """
    This procedure processes city demographic staging data using Spark and aggregates the data to create a state demographics 
    dimension table and then writes parquet files to the output filepath.
      
    INPUTS:
    * spark - the Spark session
    * city_demographics_df_spark_clean - spark dataframe of cleaned city demographic staging data
    * output_data_model - location of where the output dimension parquet files will be stored
    
     OUTPUTS:
    * state demographics dimension table - output folder with parquet files of state demographics dimensional data 
    """
    
    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')    
    start = datetime.now()
    print("\n *** Create dim_state_demographics dimension table: {}\n".format(start))
    
    # Create dimension table
    city_demographics_df_spark_clean.createOrReplaceTempView("dim_state_demographics")
    dim_state_demographics = spark.sql("""
        SELECT distinct
            State_Code,
            State,
            avg(median_age) as median_age,
            sum(male_population) as male_population,
            sum(female_population) as female_population,
            sum(total_population) as total_population,
            avg(average_household_size) as average_household_size
        FROM 
            dim_state_demographics
        group by
            State_Code,
            State
        order by State
    """) 
    dim_state_demographics = dim_state_demographics\
                             .withColumn("median_age", dim_state_demographics["median_age"]\
                             .cast(DecimalType(9,2)))\
                             .withColumn("average_household_size", dim_state_demographics["average_household_size"]\
                             .cast(DecimalType(9,2))) 
     
    # show schema and data sample 
    dim_state_demographics.printSchema()
    dim_state_demographics.show(3)
     
    # write state demographics dimension table to parquet
    dim_state_demographics_path = output_data_model\
         + "dim_state_demographics.parquet" + "_" + timestamp 
    print("*** Write state demographics dimension table parquet file")
    print(f"*** Output Path: {dim_state_demographics_path}")
    dim_state_demographics.write.parquet(dim_state_demographics_path, mode="overwrite")
    
    # read state demographics parquet file back to Spark 
    print("*** Read state demographics dimension table parquet file")
    dim_state_demographics = spark.read.parquet(dim_state_demographics_path) 
     
    end = datetime.now()
    print("\n *** Create dim_state_demographics dimension table complete: {}\n".format(end)) 
    print("*** Total time: {}".format(end-start)) 
 
    return dim_state_demographics

#----------------------------------------------------------------------------

def process_dim_admissions_data(spark, immigration_df_spark_clean, countries_df_spark, output_data_model):
    """
    This procedure processes immigration and country staging data using Spark and creates an 
    admissions dimension table and then writes parquet files to the output filepath.
      
    INPUTS:
    * spark - the Spark session
    * immigration_df_spark_clean - spark dataframe of cleaned immigration staging data
    * countries_df_spark - spark dataframe of country staging data
    * output_data_model - location of where the output dimension parquet files will be stored
    
     OUTPUTS:
    * admissions dimension table - output folder with parquet files of admissions dimensional data 
    """
    
    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')
    start = datetime.now()
    print("\n *** Create dim_admissions dimension table: {}\n".format(start))
    
    # Create dimension table
    admissions_df_spark = immigration_df_spark_clean.join(countries_df_spark, \
    (immigration_df_spark_clean.i94cit == countries_df_spark.i94cit))
    
    admissions_df_spark.createOrReplaceTempView("dim_admissions")
    dim_admissions = spark.sql("""
        SELECT distinct
            admnum as admission_number,
            country_name,
            i94bir as age,
            i94visa as visa_code,
            gender,
            visatype
        FROM 
            dim_admissions
    """) 
     
    # show schema and data sample 
    dim_admissions.printSchema()
    dim_admissions.show(3)
     
    # write admissions dimension table to parquet
    dim_admissions_path = output_data_model\
         + "dim_admissions.parquet" + "_" + timestamp 
    print("*** Write admissions dimension table parquet file")
    print(f"*** Output Path: {dim_admissions_path}")
    dim_admissions.write.parquet(dim_admissions_path, mode="overwrite")
    
    # read admissions parquet file back to Spark 
    print("*** Read admissions dimension table parquet file")
    dim_admissions = spark.read.parquet(dim_admissions_path) 
    
    end = datetime.now()
    print("\n *** Create dim_admissions dimension table complete: {}\n".format(end)) 
    print("*** Total time: {}".format(end-start)) 
 
    return dim_admissions

#----------------------------------------------------------------------------

def process_dim_date_data(spark, immigration_df_spark_clean, output_data_model):
    """
    This procedure processes immigration staging data using Spark and creates a date 
    dimension table and then writes parquet files to the output filepath.
      
    INPUTS:
    * spark - the Spark session
    * immigration_df_spark_clean - spark dataframe of cleaned immigration staging data 
    * output_data_model - location of where the output dimension parquet files will be stored
    
     OUTPUTS:
    * date dimension table - output folder with parquet files of date dimensional data 
    """
    
    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')
    start = datetime.now()
    print("\n *** Create dim_date dimension table: {}\n".format(start))
    
    # Create dimension table 
    immigration_df_spark_clean.createOrReplaceTempView("dim_date")
    dim_date = spark.sql("""
        SELECT distinct
            arrdate as date,
            day(arrdate) as day, 
            month(arrdate) as month,
            year(arrdate) as year,
            weekofyear(arrdate) as week,
            dayofweek(arrdate) as weekday
        FROM 
            dim_date
    """) 
     
    # show schema and data sample 
    dim_date.printSchema()
    dim_date.show(3)
     
    # write date dimension table to parquet
    dim_date_path = output_data_model\
         + "dim_date.parquet" + "_" + timestamp 
    print("*** Write date dimension table parquet file")
    print(f"*** Output Path: {dim_date_path}")
    dim_date.write.parquet(dim_date_path, mode="overwrite")
    
    # read date parquet file back to Spark 
    print("*** Read date dimension table parquet file")
    dim_date = spark.read.parquet(dim_date_path) 
    
    end = datetime.now()
    print("\n *** Create dim_date dimension table complete: {}\n".format(end)) 
    print("*** Total time: {}".format(end-start)) 
 
    return dim_date

#----------------------------------------------------------------------------

def process_fact_immigrations_data(spark, immigration_df_spark_clean, ports_df_spark_clean, output_data_model):
    """
    This procedure processes immigration and ports staging data using Spark and creates an 
    immigrations fact table and then writes parquet files to the output filepath.
      
    INPUTS:
    * spark - the Spark session
    * immigration_df_spark_clean - spark dataframe of cleaned immigration staging data
    * ports_df_spark_clean - spark dataframe of port staging data
    * output_data_model - location of where the output fact parquet files will be stored
    
     OUTPUTS:
    * immigrations fact table - output folder with parquet files of immigrations fact data 
    """
    
    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')
    start = datetime.now()
    print("\n *** Create fact_immigrations fact table: {}\n".format(start))
    
    # Create fact table
    immigration_df_spark_joined = immigration_df_spark_clean.join(ports_df_spark_clean, \
                                  (immigration_df_spark_clean.i94port == ports_df_spark_clean.i94port))
    immigration_df_spark_joined = immigration_df_spark_joined\
                                  .withColumn("immigration_id", monotonically_increasing_id()) 
    
    immigration_df_spark_joined.createOrReplaceTempView("fact_immigrations")
    fact_immigrations = spark.sql("""
        SELECT distinct
            immigration_id,
            arrdate as arrival_date,
            port_name,
            port_state_code,
            i94mode as mode_of_transport,
            i94addr local_address_state_code,
            depdate as departure_date,
            admnum as admission_number,
            airline,
            fltno as flight_number
        FROM 
            fact_immigrations
    """) 
     
    # show schema and data sample 
    fact_immigrations.printSchema()
    fact_immigrations.show(3)
     
    # write immigrations fact table to parquet
    fact_immigrations_path = output_data_model\
         + "fact_immigrations.parquet" + "_" + timestamp     
    print("*** Write immigrations fact table parquet file")
    print(f"*** Output Path: {fact_immigrations_path}")
    fact_immigrations.write.parquet(fact_immigrations_path, mode="overwrite")
    
    # read immigrations parquet file back to Spark     
    print("*** Read immigrations fact table parquet file")
    fact_immigrations = spark.read.parquet(fact_immigrations_path) 
 
    end = datetime.now()
    print("\n *** Create fact_immigrations fact table complete: {}\n".format(end)) 
    print("*** Total time: {}".format(end-start)) 
    
    return fact_immigrations

#----------------------------------------------------------------------------

def check_data_quality(spark, dim_state_demographics, dim_admissions, dim_date, fact_immigrations):
    """
    This procedure checks the data quality of all dimension and fact tables
      
    INPUTS:
    * spark - the Spark session
    * dim_state_demographics - state demographics dimension table
    * dim_admissions - admissions dimension table
    * dim_date - date dimension table
    * fact_immigrations - immigrations fact table  
    """
    
    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')
    start = datetime.now()
    print("\n *** Data quality check: {}\n".format(start))
    
    data_quality_check = { "timestamp": timestamp,
            "dim_state_demographics_count": 0,
            "dim_state_demographics_check": "",
            "dim_admissions_count": 0,
            "dim_admissions_check": "",
            "dim_date_count": 0, 
            "dim_date_check": "",
            "fact_immigrations_count": 0,
            "fact_immigrations_check": "" }
            
    #check state demographics dimension table data quality
    #check state demographics dimension table should have data populated (Rowcount > 0)
    dim_state_demographics.createOrReplaceTempView("dim_state_demographics_check")
    dim_state_demographics_check_count = spark.sql("""
        SELECT
            count(1) as count
        FROM 
            dim_state_demographics_check
    """)
    print("*** Check state demographics dimension table should have data populated (Rowcount > 0)")
    
    #check integrity constraints - foreign keys and their associated primary keys should be populated 
    dim_state_demographics.createOrReplaceTempView("dim_state_demographics_check")
    dim_state_demographics_check_missing = spark.sql("""
        SELECT distinct
            count(1) as count
        FROM 
            dim_state_demographics_check
        where
            state_code IS NULL or state_code == "" or state_code == "NaN"
    """)
    print("*** Check integrity constraints - foreign keys and their associated primary keys should be populated")
    
    if dim_state_demographics_check_count.collect()[0][0] < 1 or \
       dim_state_demographics_check_missing.collect()[0][0] > 0:
        data_quality_check['dim_state_demographics_count'] = dim_state_demographics_check_count.collect()[0][0]
        data_quality_check['dim_state_demographics_check'] = "NOK"
    else:
        data_quality_check['dim_state_demographics_count'] = dim_state_demographics_check_count.collect()[0][0]
        data_quality_check['dim_state_demographics_check'] = "OK"
        
    #check admissions dimension table data quality
    #check admissions dimension table should have data populated (Rowcount > 0)
    dim_admissions.createOrReplaceTempView("dim_admissions_check")
    dim_admissions_check_count = spark.sql("""
        SELECT
            count(1) as count
        FROM 
            dim_admissions_check
    """)
    print("*** Check admissions dimension table should have data populated (Rowcount > 0)")
        
    #check integrity constraints - foreign keys and their associated primary keys should be populated
    dim_admissions.createOrReplaceTempView("dim_admissions_check")
    dim_admissions_check_missing = spark.sql("""
        SELECT distinct
            count(1) as count
        FROM 
            dim_admissions_check
        where
            admission_number IS NULL or admission_number == "" or admission_number == "NaN" or admission_number = 0.0
    """)
    print("*** Check integrity constraints - foreign keys and their associated primary keys should be populated")
    
    if dim_admissions_check_count.collect()[0][0] < 1 or \
       dim_admissions_check_missing.collect()[0][0] > 0:
        data_quality_check['dim_admissions_count'] = dim_admissions_check_count.collect()[0][0]
        data_quality_check['dim_admissions_check'] = "NOK"
    else:
        data_quality_check['dim_admissions_count'] = dim_admissions_check_count.collect()[0][0]
        data_quality_check['dim_admissions_check'] = "OK"
     
    #check date dimension table data quality
    #check date dimension table should have data populated (Rowcount > 0)
    dim_date.createOrReplaceTempView("dim_date_check")
    dim_date_check_count = spark.sql("""
        SELECT
            count(1) as count
        FROM 
           dim_date_check
    """)
    print("*** Check date dimension table should have data populated (Rowcount > 0)")
    
    if dim_date_check_count.collect()[0][0] < 1:
        data_quality_check['dim_date_count'] = dim_date_check_count.collect()[0][0]
        data_quality_check['dim_date_check'] = "NOK"
    else:
        data_quality_check['dim_date_count'] = dim_date_check_count.collect()[0][0]
        data_quality_check['dim_date_check'] = "OK"  
    
    #check immigrations fact table data quality
    #check immigrations fact table should have data populated (Rowcount > 0)
    fact_immigrations.createOrReplaceTempView("fact_immigrations_check")
    fact_immigrations_check_count = spark.sql("""
        SELECT
            count(1) as count
        FROM 
            fact_immigrations_check
    """)
    print("*** Check immigrations fact table should have data populated (Rowcount > 0")
    
    #check integrity constraints - foreign keys and their associated primary keys should be populated. 
    fact_immigrations.createOrReplaceTempView("fact_immigrations_check")
    fact_immigrations_check_missing = spark.sql("""
        SELECT distinct
            count(1) as count
        FROM 
            fact_immigrations_check
        where
            arrival_date IS NULL or arrival_date == "" or arrival_date == "NaN" or
            port_state_code IS NULL or port_state_code == "" or port_state_code == "NaN" or
            admission_number IS NULL or admission_number == "" or admission_number == "NaN" or admission_number = 0.0
    """)
    print("*** Check integrity constraints - foreign keys and their associated primary keys should be populated")
    
    if fact_immigrations_check_count.collect()[0][0] < 1 or\
       fact_immigrations_check_missing.collect()[0][0] > 0:
        data_quality_check['fact_immigrations_count'] = fact_immigrations_check_count.collect()[0][0]
        data_quality_check['fact_immigrations_check'] = "NOK"
    else:
        data_quality_check['fact_immigrations_count'] = fact_immigrations_check_count.collect()[0][0]
        data_quality_check['fact_immigrations_check'] = "OK"
    
    print("\n *** Data quality check results: ")
    print(data_quality_check) 
    
    end = datetime.now()
    print("\n *** Data quality check complete: {}\n".format(end)) 
    print("*** Total time: {}".format(end-start)) 
    
#----------------------------------------------------------------------------

def main():
    spark = create_spark_session()
    
    #setup config
    config = configparser.ConfigParser()
    config.read('dl.cfg')
    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_KEY']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_KEY']['AWS_SECRET_ACCESS_KEY']
    
    #to use AWS storage, change the config paths below to use S3_DATA instead of DATA:
    input_data_immigration = config['DATA']['INPUT_DATA_IMMIGRATION'] 
    input_data_countries = config['DATA']['INPUT_DATA_COUNTRIES']
    input_data_ports = config['DATA']['INPUT_DATA_PORTS']
    input_data_cities = config['DATA']['INPUT_DATA_CITIES']
    output_data_staging = config['DATA']['OUTPUT_DATA_STAGING']
    output_data_model = config['DATA']['OUTPUT_DATA_MODEL']
    
    start_time = datetime.now()
    print("\n *** Start ETL pipeline: {}\n".format(start_time))
    
    #process input files
    immigration_df_spark = process_immigration_data(spark, input_data_immigration, output_data_staging)
    countries_df_spark = process_country_data(spark, input_data_countries, output_data_staging)
    ports_df_spark = process_port_data(spark, input_data_ports, output_data_staging)
    city_demographics_df_spark = process_city_demographic_data(spark, input_data_cities, output_data_staging)
    
    #clean the data
    immigration_df_spark_clean = clean_immigration_data(spark, immigration_df_spark)
    ports_df_spark_clean = clean_ports_data(spark, ports_df_spark)
    city_demographics_df_spark_clean = clean_city_demographics_data(spark, city_demographics_df_spark)
    
    #create dimension tables
    dim_state_demographics = process_dim_state_demographics_data(spark, city_demographics_df_spark_clean, output_data_model)
    dim_admissions = process_dim_admissions_data(spark, immigration_df_spark_clean, countries_df_spark, output_data_model)
    dim_date = process_dim_date_data(spark, immigration_df_spark_clean, output_data_model)
    
    #create fact table
    fact_immigrations = process_fact_immigrations_data(spark, immigration_df_spark_clean, ports_df_spark_clean, output_data_model)

    #process data quality checks
    data_quality_check = check_data_quality(spark, dim_state_demographics, dim_admissions, dim_date, fact_immigrations)
    
    end_time = datetime.now() 
    print("\n *** End ETL pipeline: {}\n".format(end_time))
    print("*** ETL pipeline total time: {}".format(end_time-start_time)) 

if __name__ == "__main__":
    main()
