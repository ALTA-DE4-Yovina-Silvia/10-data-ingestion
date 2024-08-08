import pandas as pd

# Step 1: Read the CSV file with low_memory=False to avoid DtypeWarning
print("1. Membaca file CSV")
df = pd.read_csv('C:/Users/USER/Alta/belajar-bc/ingestion-demo/dataset/yellow_tripdata_2020-07.csv', low_memory=False)
print(df.head())
print("-----------------------------------------------------------------------\n")

# Step 2: Rename all the columns with snake_case format
print("2. Mengganti nama kolom dengan format snake_case")
df.columns = [col.lower().replace(' ', '_') for col in df.columns]
df = df.rename(columns={'vendorid': 'vendor_id'})
print(df.head())
print("-----------------------------------------------------------------------\n")

# Step 3: Select the relevant columns
print("3. Memilih kolom yang relevan")
selected_columns = [
    'vendor_id', 'passenger_count', 'trip_distance', 'payment_type', 
    'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 
    'improvement_surcharge', 'total_amount', 'congestion_surcharge'
]
df_selected = df[selected_columns]
print(df_selected.head())
print("-----------------------------------------------------------------------\n")

# Step 4: Cast the data types to the appropriate values using .loc to avoid SettingWithCopyWarning
print("4. Mengubah tipe data ke nilai yang sesuai")
df_selected.loc[:, 'vendor_id'] = df_selected['vendor_id'].fillna(0).astype(int)
df_selected.loc[:, 'passenger_count'] = df_selected['passenger_count'].fillna(0).astype(int)
df_selected.loc[:, 'trip_distance'] = df_selected['trip_distance'].fillna(0).astype(float)
df_selected.loc[:, 'payment_type'] = df_selected['payment_type'].fillna(0).astype(int)
df_selected.loc[:, 'fare_amount'] = df_selected['fare_amount'].fillna(0).astype(float)
df_selected.loc[:, 'extra'] = df_selected['extra'].fillna(0).astype(float)
df_selected.loc[:, 'mta_tax'] = df_selected['mta_tax'].fillna(0).astype(float)
df_selected.loc[:, 'tip_amount'] = df_selected['tip_amount'].fillna(0).astype(float)
df_selected.loc[:, 'tolls_amount'] = df_selected['tolls_amount'].fillna(0).astype(float)
df_selected.loc[:, 'improvement_surcharge'] = df_selected['improvement_surcharge'].fillna(0).astype(float)
df_selected.loc[:, 'total_amount'] = df_selected['total_amount'].fillna(0).astype(float)
df_selected.loc[:, 'congestion_surcharge'] = df_selected['congestion_surcharge'].fillna(0).astype(float)
print(df_selected.head())
print("-----------------------------------------------------------------------\n")

# Step 5: Select the top 10 rows with the highest passenger_count
print("5. Memilih 10 baris teratas dengan jumlah penumpang terbanyak")
top_passenger_data = df_selected.nlargest(10, 'passenger_count')
print(top_passenger_data)
