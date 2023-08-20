from pyspark.sql import SparkSession
from pyspark.sql import functions as F

class ChargePointsETLJob:
    input_path = 'data/input/electric-chargepoints-2017.csv'
    output_path = 'data/output/chargepoints-2017-analysis'

    def __init__(self):
        self.spark_session = (SparkSession.builder
                                          .master("local[*]")
                                          .appName("ElectricChargePointsETLJob")
                                          .getOrCreate())

    def extract(self):
        # Carregar os dados do arquivo CSV
        df = self.spark_session.read.csv(self.input_path, header=True, inferSchema=True)
        return df

    def transform(self, df):
        # Agrupar por CPID e calcular max e avg do PluginDuration
        df_transformed = (df.groupBy("CPID")
                          .agg(F.max("PluginDuration").alias("max_duration"),
                               F.avg("PluginDuration").alias("avg_duration"))
                          .withColumnRenamed("CPID", "chargepoint_id"))
        
        # Arredondar os resultados para duas casas decimais
        df_transformed = df_transformed.withColumn("max_duration", F.round(df_transformed["max_duration"], 2))
        df_transformed = df_transformed.withColumn("avg_duration", F.round(df_transformed["avg_duration"], 2))
        
        return df_transformed

    def load(self, df):
        # Salvar o dataframe em formato parquet
        df.write.parquet(self.output_path, mode="overwrite")

    def run(self):
        self.load(self.transform(self.extract()))
    
    def read_output(self):
        # Lendo o arquivo Parquet do output_path
        df_output = self.spark_session.read.parquet(self.output_path)
        
        # Mostrando as primeiras 20 linhas do dataframe
        df_output.show()

# Para executar o pipeline
if __name__ == "__main__":
    job = ChargePointsETLJob()
    job.run()
    job.read_output()