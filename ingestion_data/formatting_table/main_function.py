import pandas as pd
import sys
import os

# Menambahkan path direktori 'src' ke sys.path
sys.path.append(os.path.dirname(__file__))

from table_formatting import print_table_with_index, print_table_with_summary

def main():
    # 1. Memuat dataset dengan low_memory=False untuk menangani peringatan tipe campuran
    df = pd.read_csv(os.path.join(os.path.dirname(__file__), "../dataset/yellow_tripdata_2020-07.csv"), low_memory=False)

    # Menampilkan beberapa baris pertama dari data yang dimuat
    print("1. DataFrame setelah memuat dataset:")
    print_table_with_index(df.head())
    print("\n")

    # 2. Mengganti nama kolom menjadi snake_case
    df.columns = df.columns.str.lower().str.replace(' ', '_')

    # Menampilkan nama-nama kolom setelah diganti menjadi snake_case
    print("2. Nama kolom setelah diganti menjadi snake_case:")
    print(df.columns.tolist())
    print("\n")

    # 3. Memilih kolom-kolom yang relevan dan 10 baris teratas dengan jumlah penumpang tertinggi
    selected_columns = [
        'vendorid', 'passenger_count', 'trip_distance', 'payment_type',
        'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
        'improvement_surcharge', 'total_amount', 'congestion_surcharge'
    ]
    df_selected = df[selected_columns]

    # Memilih 10 baris teratas dengan jumlah penumpang tertinggi
    top_10_passenger_count = df_selected.nlargest(10, 'passenger_count')

    print("3. 10 baris teratas dengan jumlah penumpang tertinggi:")
    print_table_with_summary(top_10_passenger_count)
    print("\n")

    # 4. Mengubah tipe data dan menampilkan DataFrame setelah casting
    print("4. Nilai NaN sebelum mengubah tipe data:")
    print(df_selected.isna().sum())
    print("\n")

    # Menggunakan .loc untuk mengisi nilai NaN dan mengubah tipe
    df_selected.loc[:, 'vendorid'] = df_selected['vendorid'].fillna(0).astype(int)
    df_selected.loc[:, 'passenger_count'] = df_selected['passenger_count'].fillna(0).astype(int)

    float_columns = [
        'trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount',
        'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge'
    ]
    for col in float_columns:
        df_selected.loc[:, col] = df_selected[col].fillna(0.0).astype(float)

    # Menampilkan DataFrame setelah mengubah tipe data
    print("4. DataFrame setelah mengubah tipe data:")
    print_table_with_summary(df_selected.head())
    print("\n")

if __name__ == "__main__":
    main()