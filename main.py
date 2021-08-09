import pyspark
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from pathlib import Path
import argparse
import sys


def args_parser(args):
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-r', type=str, help='Run date script')
    parser.add_argument('-o', type=str, help='Start date of execute')

    args_data = parser.parse_args(args=args)

    read_path = ''
    output_path = ''

    if args_data.r:
        read_path = args_data.r
    if args_data.o:
        output_path = args_data.o
    return read_path, output_path


def read_dataframes(path, sq):
    path = Path(path)
    offense_codes_df = sq.read.option("header", "true") \
        .csv(str(path / "offense_codes.csv"))
    crime_df = sq.read.option("header", "true") \
        .csv(str(path / "crime.csv"))

    return offense_codes_df, crime_df

def write_result_parquet(df, path):
    path = Path(path)
    df.repartition(1).write.mode('overwrite').parquet(str(path / "parquet"))


def run_spark_job(read_path,output_path):

    sc = pyspark.SparkContext(appName = "Homework")
    sq = SQLContext(sc)

    offense_codes_df, crime_df = read_dataframes(read_path, sq)

    offense_codes_df = offense_codes_df.withColumn("CODE", offense_codes_df["CODE"].cast(IntegerType()))
    crime_df = crime_df.withColumn("OFFENSE_CODE", crime_df["OFFENSE_CODE"].cast(IntegerType()))

    print("Before deleting duplicates {0} rows left".format(offense_codes_df.count()))
    offense_codes_df.createOrReplaceTempView("offense_codes_df")
    offense_codes_df = sq.sql("SELECT CODE, MAX(NAME) as NAME FROM OFFENSE_CODES_DF GROUP BY CODE")
    offense_codes_df = offense_codes_df.withColumn("NAME", split("NAME", '-')[0])
    print("After deleting duplicates {0} rows left".format(offense_codes_df.count()))


    crime_df_distinct = crime_df.dropDuplicates(["DISTRICT", "INCIDENT_NUMBER", "YEAR", "MONTH", "LAT", "LONG"]) \
        .select(["DISTRICT", "INCIDENT_NUMBER", "YEAR", "MONTH", "LAT", "LONG"])

    crime_df_distinct = crime_df_distinct.withColumn("MONTH", lpad("MONTH", 2, '0'))
    crime_df_distinct = crime_df_distinct.withColumn("DAY_F_DATE", lit("01"))
    crime_df_distinct = crime_df_distinct.withColumn("MONTH_DATE", \
                                                     expr("make_date(YEAR, MONTH, DAY_F_DATE)"))

    crimes_total_df = crime_df_distinct.groupBy("DISTRICT") \
        .agg(countDistinct("INCIDENT_NUMBER").alias("crimes_total") \
             , avg("LAT").alias("lat") \
             , avg("LONG").alias("long") \
             )


    crimes_monthly_df = crime_df_distinct.groupBy("DISTRICT", "MONTH_DATE") \
        .agg(countDistinct("INCIDENT_NUMBER") \
             .alias("cnt_inc"))

    crimes_monthly_df = crimes_monthly_df.groupBy("DISTRICT") \
        .agg(expr("percentile(cnt_inc, 0.5)") \
             .alias("crimes_monthly"))


    freq_crimes_df = crime_df.dropDuplicates(["DISTRICT", "INCIDENT_NUMBER", "OFFENSE_CODE"]) \
        .select(["DISTRICT", "INCIDENT_NUMBER", "OFFENSE_CODE"])

    freq_crimes_df = freq_crimes_df.join(offense_codes_df, \
                                         freq_crimes_df["offense_code"] == offense_codes_df["code"], \
                                         'left')

    freq_crimes_df = freq_crimes_df.groupBy("DISTRICT", "NAME") \
        .agg(count("INCIDENT_NUMBER") \
             .alias("CNT_BY_NAME"))

    window_spec = Window.partitionBy("DISTRICT").orderBy(col("CNT_BY_NAME").desc())

    freq_crimes_df = freq_crimes_df.withColumn("dense_rank", dense_rank().over(window_spec))
    freq_crimes_df = freq_crimes_df.filter("dense_rank <= 3")
    freq_crimes_df = freq_crimes_df.groupby("DISTRICT").agg(collect_list('NAME').alias('frequent_crime_types'))


    crimes_total_df1 = crimes_total_df.join(crimes_monthly_df\
                                            , ["DISTRICT"]\
                                            , 'inner')
    crimes_total_df1 = crimes_total_df1.join(freq_crimes_df\
                                             , ["DISTRICT"]\
                                             , 'inner')

    write_result_parquet(crimes_total_df1, output_path)

def main():
    result = 0
    try:
        read_path,output_path = args_parser(sys.argv[1:])
        run_spark_job(read_path,output_path)
    except Exception as e:
        print(e)
        result = 1
    return result

main()
