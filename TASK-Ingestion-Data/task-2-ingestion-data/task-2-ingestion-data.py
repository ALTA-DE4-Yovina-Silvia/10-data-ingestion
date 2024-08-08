import pandas as pd
from fastparquet import ParquetFile
from sqlalchemy import create_engine, text
from sqlalchemy.types import BigInteger, DateTime, Boolean, Float, Integer
from sqlalchemy.exc import SQLAlchemyError
from prettytable import PrettyTable

# 1. Buat DataFrame dari file parquet di folder datasets kita.
class Extraction:
    def __init__(self) -> None:
        self.path = ""
        self.dataframe = pd.DataFrame()

    def local_file(self, path: str):
        self.path = path
        self.__read_parquetfile()
        print("1. DataFrame created from parquet file:")
        print(self.dataframe.head())  # Print the first few rows of the DataFrame
        self.investigate_schema()
        self.cast_data()
        return self.dataframe
    
    def __read_parquetfile(self):
        parquetfile = ParquetFile(self.path)
        self.dataframe = parquetfile.to_pandas()

# 2. Memuat file parquet ke dalam DataFrame menggunakan pustaka fastparquet
    def investigate_schema(self):
        pd.set_option('display.max_columns', None)
        print("\n2. Schema of the DataFrame:")
        print(self.dataframe.dtypes)  # Print the schema of the DataFrame

# 3. Bersihkan dataset Yellow Trip.
    def cast_data(self):
        self.dataframe["passenger_count"] = self.dataframe["passenger_count"].astype("Int8")
        self.dataframe["store_and_fwd_flag"] = self.dataframe["store_and_fwd_flag"].map({"N": False, "Y": True}).astype("boolean")
        self.dataframe["tpep_pickup_datetime"] = pd.to_datetime(self.dataframe["tpep_pickup_datetime"])
        self.dataframe["tpep_dropoff_datetime"] = pd.to_datetime(self.dataframe["tpep_dropoff_datetime"])
        print("\n3. Cleaned DataFrame with correct data types:")
        print(self.dataframe.head())  # Print the cleaned DataFrame

class Load:
    def __init__(self) -> None:
        self.engine = None

    def __create_connection(self):
        user = "postgres"
        password = "admin"
        host = "localhost"
        database = "mydb"
        port = 5437
        conn_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        self.engine = create_engine(conn_string) 

# 4. Tentukan skema tipe data saat menggunakan metode to_sql.
    def to_postgres(self, db_name: str, data: pd.DataFrame) -> None:
        self.__create_connection()
        df_schema = {
            "VendorID": BigInteger,
            "tpep_pickup_datetime": DateTime,
            "tpep_dropoff_datetime": DateTime,
            "passenger_count": BigInteger,
            "trip_distance": Float,
            "RatecodeID": Float,
            "store_and_fwd_flag": Boolean,
            "PULocationID": Integer,
            "DOLocationID": Integer,
            "payment_type": Integer,
            "fare_amount": Float,
            "extra": Float,
            "mta_tax": Float,
            "tip_amount": Float,
            "tolls_amount": Float,
            "improvement_surcharge": Float,
            "total_amount": Float,
            "congestion_surcharge": Float,
            "airport_fee": Float
        }
        print("\n4. Data type schema defined for to_sql method.")
        print(data.info())
        try:
# 5. Masukkan dataset Yellow Trip ke PostgreSQL.
            data.to_sql(name=db_name, con=self.engine, if_exists="replace", index=False, schema="public", dtype=df_schema, method=None, chunksize=5000)
            print("\n5. Dataset ingested to PostgreSQL, check the result on PostgreSQL")
        except SQLAlchemyError as err:
            print(f"error >> {err}") 

def main():
    extract = Extraction()
    file_path = "C:/Users/USER/Alta/belajar-bc/ingestion-demo/dataset/yellow_tripdata_2023-01.parquet"
    df_result = extract.local_file(file_path)

    load = Load()
    db_name = "data_parquet"
    load.to_postgres(db_name, df_result)

# 6. Hitung berapa baris yang dimasukkan.
    row_count = len(df_result)
    table = PrettyTable()
    table.field_names = ["Number of rows ingested"]
    table.add_row([row_count])
    print("\n6. Number of rows ingested:")
    print(table)

if __name__ == "__main__":
    main()
