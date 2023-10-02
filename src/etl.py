from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, avg, round


class ChargePointsETLJob:
    def __init__(self):
        self.spark_session = (
            SparkSession.builder.master("local[*]")
            .appName("ElectricChargePointsETLJob")
            .getOrCreate()
        )
        self.input_path = "./input/electric-chargepoints-2017.csv"
        self.output_path = "./output/chargepoints-2017-analysis.parquet"

    def extract(self):
        df = (
            self.spark_session.read.format("csv")
            .option("header", True)
            .load(self.input_path)
        )
        return df

    # Versão com SPARK SQL
    # def transform(self, df):
    #     df.createOrReplaceTempView("chargepoints")
    #     script = """
    #         SELECT
    #             cpid AS chargepoint_id,
    #             ROUND(MAX(pluginduration),2) AS max_duration,
    #             ROUND(AVG(pluginduration),2) AS avg_duration
    #         FROM
    #             chargepoints
    #         GROUP BY
    #             1
    #         """
    #     newdf = self.spark_session.sql(script)
    #     return newdf

    def transform(self, df):
        newdf = (
            df.groupBy("cpid")
            .agg(
                round(max(col("pluginduration").cast("decimal(17,2)")), 2).alias(
                    "max_duration"
                ),
                round(avg(col("pluginduration").cast("decimal(17,2)")), 2).alias(
                    "avg_duration"
                ),
            )
            .withColumnRenamed("cpid", "chargepoint_id")
        )

        return newdf

    def load(self, df):
        newdf = df.write.mode("overwrite").parquet(self.output_path)
        return newdf

    def run(self):
        e = self.extract()
        t = self.transform(e)
        self.load(t)


if __name__ == "__main__":
    etl = ChargePointsETLJob()
    etl.run()
