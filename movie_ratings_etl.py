from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import  Window
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
ACCESS_KEY = "AWS ACCESS KEY"
ACCESS_SECRET_KEY= "AWS SECRET KEY"
def create_spark_session():
    spark = SparkSession.builder \
             .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
             .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
             .config("spark.hadoop.fs.s3a.secret.key", ACCESS_SECRET_KEY) \
             .config("spark.local.dir", "./logs") \
             .master("local[*]") \
             .appName("spark-movie-ratings-analysis") \
             .getOrCreate()

    logger.info("Spark session started!")
    return spark


class MovieRatingsETL:
    def __init__(self):
        self.spark = None
        self.last_processed_id = 0
        self.schema = None

    #read parquet files from s3
    def read_parquet(self, path, partition_col):
        try:
            logger.info(f"Reading file from path:{path}")
            df = self.spark.read.option("parititionColumn", partition_col).parquet(path, inferSchema=True)
            return df
        except Exception as e:
            logger.error(f"Encountered exception while reading file: {path}: {e}")
            if True:
                logger.info(f"No data present at source [Infer schema issue]: {path}")
                return None
            else:
                # raise Exception("Error while reading file.")
                return None



    def save_details(self):
        with open("schema.json","w") as f:
            if self.schema:
                f.write(self.schema.json())
            f.close()

        with open("last_processed_data.txt","w") as f:

            f.write(f"last_processed_id={self.last_processed_id}")
            f.close()

    #configuration setup
    def initialize(self):
        try:
            if not self.spark:
                self.spark = create_spark_session()

            with open("schema.json","r") as f:
                self.schema = StructType.fromJson(json.loads(f.read()))
                f.close()

            with open("last_processed_data.txt","r") as f:
                data = f.read()
                self.last_processed_id = int(data.split("=")[1])

            logger.info("Initialised schema and last_processed_id!")
        except Exception as e:
            self.last_processed_id = 0
            logger.error(f"Exception occured during intialization: {e}")

    def process_raw_data(self, source, raw_output_path):

        try:
            # load data from source
            df = self.spark.read.format("csv").option("header", True).option("delimeter", ',').option("inferSchema",
                                                                                                      True).load(source)
            logger.info(f"Total Entries in data:{df.count()}")
            logger.info(f"Last processed id: {self.last_processed_id}")
            max_id = df.agg({"id": "max"}).collect()[0]["max(id)"]
            logger.info(f"Max id to be processed: {max_id}")

            # data type casting
            df = df.withColumn("vote_average", col("vote_average").cast("float")) \
                .withColumn("vote_count", col("vote_count").cast("integer")) \
                .withColumn("release_date", to_date("release_date")) \
                .withColumn("revenue", col("revenue").cast("float")) \
                .withColumn("budget", col("budget").cast("float")) \
                .withColumn("runtime", col("runtime").cast("integer")) \
                .withColumn("popularity", col("popularity").cast("float"))

            df = df.withColumn("year", year(col("release_date"))) \
                .withColumn("month", month(col("release_date")))


            # partitioning
            df = df.repartition(col("year"))

            #read existing data from source
            schema = self.schema if self.schema else df.schema
            existing_df = self.read_parquet(raw_output_path, "year")
            if not existing_df:
                existing_df = self.spark.createDataFrame([], schema)

            #filter out new data
            new_df = df.join(existing_df, df["id"] == existing_df["id"], "left_anti")
            print(f"Rows needs to be processed: {new_df.count()}")

            #write raw data to s3
            new_df.write.mode("append") \
                .partitionBy("year") \
                .parquet(raw_output_path)

            logger.info(f"Processed data successfully with max_id: {max_id}")

            # update final details
            self.schema = new_df.schema
            self.last_processed_id = int(max_id)


        except Exception as e:
            logger.info(f"Encountered exception while processing data: {e}")
            raise Exception("Encountered exception in data processing")
        finally:
            self.save_details()


    def transform_data(self,source_path ,output_path):
        try:
            df = self.read_parquet(source_path, partition_col="year")
            if not df:
                logger.error(f"Error reading data from s3 path: {source_path}")
                return

            #remove null data based on few columns
            df = df.dropna(subset=["release_date", "revenue", "budget"])

            #data formatting
            df = df.withColumn("year", col("year").cast("string")).withColumn("month", format_string("%02d", col("month")))

            #clean status, genres, production_companies columns
            status_list = ["Released", "Planned", "In Production", "Post Production", "Cancelled", "Rumoured", "NA"]
            cleaned_df = df.repartition(col("month")).withColumn("genres", regexp_replace(col("genres"), "[^a-zA-Z\s,]", "")) \
                .withColumn("production_companies", regexp_replace(col("production_companies"), "[^a-zA-Z\s,.]", "")) \
                .withColumn("status", when(lower(trim(col("status"))).isin([s.lower() for s in status_list]),
                                           trim(col("status"))).otherwise("Others")) \
                .withColumn("production_companies", trim(lower(col("production_companies"))))

            #selective filtering
            cleaned_df = cleaned_df.filter(
                cleaned_df["release_date"].isNotNull() & cleaned_df["revenue"].isNotNull() & cleaned_df[
                    "budget"].isNotNull()) \
                .select("id", "status", "genres", "release_date", "runtime", "year", "month", "budget", "revenue",
                        "vote_count", "vote_average", "title", "production_companies")
            #fill nulls
            cleaned_df = cleaned_df.fillna({"genres": "NA", "title": "", "production_companies": "NA"})

            # cleaning outliers
            runtime_otl = cleaned_df.filter((col("runtime") > 300) | (col("runtime") < 0))
            # cleaning runtime outliers via anti join
            cleaned_df = cleaned_df.join(runtime_otl, runtime_otl.id == cleaned_df.id, how="left_anti")
            vote_avg_otl = cleaned_df.filter((col("vote_average") > float(90)) | (col("vote_average") < float(0)))
            # cleaning runtime outliers via anti join
            cleaned_df = cleaned_df.join(runtime_otl, vote_avg_otl.id == cleaned_df.id, how="left_anti")
            budget_revenue_otl = cleaned_df.filter((col("budget") < float(0)) | (col("revenue") < float(0)))
            # cleaning budget and revenue outliers via anti join
            cleaned_df = cleaned_df.join(runtime_otl, budget_revenue_otl.id == cleaned_df.id, how="left_anti")

            #writes data to s3
            cleaned_df.write.mode("overwrite").partitionBy("month").parquet(output_path)
            logger.info(f"Successfully transformed data and writes to s3 path :{output_path}")

        except Exception as e:
            logger.info(f"Encountered exception while processing data: {e}")
            raise Exception("Encountered exception in data transformation")


    def eda(self, df, cleaned_df):
        print(f"Total Entries in original data: {df.count()}")
        print("*" * 50)
        print(f"Schema of original data: {df.schema}")
        print("*" * 50)

        # Count the null values in the original df
        for col_name in df.columns:
            null_count = df.filter(col(col_name).isNull()).count()
            print(f"Column '{col_name}' has {null_count} null values.")

        print(f"Distinct status in original data: {df.select('status').distinct().count()}")
        print("\n")
        print(f"Distinct genres in original data: {df.select('genres').distinct().count()}")
        print("\n")
        print(f"Distinct production companies in original data: {df.select('production_companies').distinct().count()}")

        print(f"Total entries in cleaned data: {cleaned_df.count()}")

        print(f"Distinct status in cleaned data: {cleaned_df.select('status').distinct().count()}")
        print("\n")
        print(f"Distinct genres in cleaned data: {cleaned_df.select('genres').distinct().count()}")
        print("\n")
        print(
            f"Distinct production companies in cleaned data: {cleaned_df.select('production_companies').distinct().count()}")

        # Count the null values in the cleaned df
        for col_name in cleaned_df.columns:
            null_count = cleaned_df.filter(col(col_name).isNull()).count()
            print(f"Column '{col_name}' has {null_count} null values.")

        # genre level data
        keywords = ["action", "thriller", "crime", "comedy", "drama", "romance", "horror", "adventure", "documentary",
                    "tv movie", "fantasy", "mystery", "history", "science fiction", "family", "war", "western",
                    "animation", "na"]
        genre_df = cleaned_df.withColumn("genres", lower(col("genres"))) \
            .withColumn("unique_genres", explode(split(col("genres"), ", "))).drop("genres").withColumn("genres", trim(
            col("unique_genres"))).drop("unique_genres").filter(col("genres") \
                                                                .isin(keywords))
        yoy_genre_df = genre_df.groupBy("year", "genres").agg(count(col("id")).alias("total_movies"),
                                                              sum(col("budget")).alias("total_budget"),
                                                              sum(col("revenue")).alias("total_revenue"),
                                                              corr(col("budget"), col("revenue")).alias(
                                                                  "budget_revenue_correlation"))

        mby_genre_df = genre_df.groupBy("month", "genres").agg(count(col("id")).alias("total_movies"),
                                                               sum(col("budget")).alias("total_budget"),
                                                               sum(col("revenue")).alias("total_revenue"),
                                                               corr(col("budget"), col("revenue")).alias(
                                                                   "budget_revenue_correlation"))

        genre_agg_df = genre_df.groupBy("genres").agg(count(col("id")).alias("total_movies"),
                                                      sum(col("budget")).alias("total_budget"),
                                                      sum(col("revenue")).alias("total_revenue"),
                                                      corr(col("budget"), col("revenue")).alias(
                                                          "budget_revenue_correlation"))

        # Yearly Growth by genre
        yoy_genre_df.orderBy(col("total_movies").desc()).show(5, False)

        # Genre based agg data
        genre_agg_df.orderBy(col("total_budget").desc(), col("total_revenue").desc(),
                             col("budget_revenue_correlation").desc()).show(truncate=False)

        print(f"Rows in genre df: {genre_df.count()}")
        print("*" * 50)
        temp = genre_df.filter((col("budget") == float(0)) & (col("revenue") == float(0))).count()
        print(f"Entries where budget and revenue is 0: {temp}")

        # production company filtering
        keywords = ["action", "thriller", "crime", "comedy", "drama", "romance", "horror", "adventure", "documentary",
                    "tv movie", "fantasy", "mystery", "history", "science fiction", "family", "war", "western", "na",
                    "", "animation"]

        pc1 = cleaned_df.withColumn("pc", explode(split(col("production_companies"), ", "))).withColumn("pc", trim(
            lower(col("pc")))).filter(~col("pc").isin(keywords)).filter(length(col("pc")) > 5).filter(
            regexp_like(col("production_company"), '^[a-zA-Z]+(\.[a-zA-Z]+)*$'))

        ws = Window.partitionBy("production_company").orderBy("year")
        pc_ranked_df = pc1.withColumn("pc_rank", dense_rank().over(ws))
        pc_ranked_df = pc_ranked_df.filter(col("pc_rank") == 1)
        pc_distinct_df = pc_ranked_df.groupBy("year").agg(countDistinct("pc").alias("pc_distinct"))

        window = Window.orderBy("year").rowsBetween(Window.unboundedPreceding, -1)

        pc_distinct_df = pc_distinct_df.withColumn("prev_yr_pc_count", sum("pc_distinct").over(window)).withColumn(
            "prev_yr_pc_count", when(col("prev_yr_pc_count").isNull(), 0).otherwise(col("prev_yr_pc_count")))

        pc_yoy_df = pc_distinct_df.withColumn("pc_yoy_growth_count", col("pc_distinct") + col("prev_yr_pc_count"))

        pc_yoy_df.orderBy(col("year").desc()).show(truncate=False)




if __name__ == '__main__':
    etl = MovieRatingsETL()
    source = "TMDB_all_movies.csv"
    raw_s3_destination_path = "s3a://tmdb-movies-datalake/movie-ratings-data/"
    filtered_data_s3_path = "s3a://tmdb-movies-datalake/movie-ratings-filtered/"

    etl.initialize()
    etl.process_raw_data(source, raw_s3_destination_path)
    etl.transform_data(raw_s3_destination_path,filtered_data_s3_path)