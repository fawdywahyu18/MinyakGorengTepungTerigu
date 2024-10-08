"""
Data Formatting

@author: fawdywahyu18
"""


import pandas as pd
import numpy as np

class DataFormattingHarga:
    def __init__(self, harga_2018_2021, harga_2022, harga_2023, harga_2024):
        self.harga_2018_2021 = harga_2018_2021
        self.harga_2022 = harga_2022
        self.harga_2023 = harga_2023
        self.harga_2024 = harga_2024

    # Fungsi untuk membuat singkatan provinsi
    def create_abbreviation(self, province):
        words = province.split()
        if len(words) == 1:
            return words[0][:3].lower()  # Ambil tiga huruf pertama jika hanya satu kata
        else:
            return ''.join([word[0] for word in words]).lower()  # Ambil huruf pertama dari setiap kata
    
    # Fungsi untuk membersihkan dan memproses data
    def clean_and_process_data(self, df):
        # Mengubah format tanggal
        df['tanggal'] = pd.to_datetime(df['tanggal'], format='%d/%m/%y', errors='coerce')

        # Cek apakah kolom harga adalah string, dan jika ya, lakukan replace koma
        if pd.api.types.is_string_dtype(df['harga']):
            df['harga'] = df['harga'].str.replace(',', '').astype(float)
        
        # Membersihkan data dari nilai inf dan NaN
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.dropna(subset=['harga'])

        return df
    
    # Fungsi untuk mengubah data ke format pivot wide
    def process_data(self, df):
        # Membuat daftar provinsi unik
        provinces = list(df['provinsi'].unique())

        # Membuat DataFrame singkatan provinsi
        df_provinces = pd.DataFrame(provinces, columns=['provinsi'])
        df_provinces['singkatan'] = df_provinces['provinsi'].apply(self.create_abbreviation)

        # Buat dictionary untuk memetakan nama provinsi ke singkatan
        provinsi_to_singkatan = pd.Series(df_provinces['singkatan'].values, index=df_provinces['provinsi']).to_dict()

        # Mengubah nama provinsi menjadi singkatan dengan menggunakan map()
        df['provinsi_singkat'] = df['provinsi'].map(provinsi_to_singkatan)

        # Mapping untuk membuat nama kolom singkatan
        komoditas_mapping = {
            'Minyak Goreng Sawit Curah': 'mgsc',
            'Minyak Goreng Sawit Kemasan Premium': 'mgskp',
            'Tepung Terigu': 'tt'
        }

        # Buat kolom baru dengan singkatan berdasarkan published_name
        df['commodity_short'] = df['published_name'].map(komoditas_mapping)

        # Buat kolom nama gabungan dari provinsi dan singkatan komoditas
        df['nama_gabungan'] = df['provinsi_singkat'].str.lower() + '_' + df['commodity_short']

        # Lakukan pivot untuk membuat data menjadi bentuk lebar (wide format)
        df_pivot = df.pivot_table(index='tanggal', columns='nama_gabungan', values='harga', aggfunc='mean').reset_index()

        return df_pivot

    # Fungsi utama untuk menjalankan seluruh proses
    def run(self):
        # Membersihkan dan memproses setiap dataframe
        self.harga_2018_2021 = self.clean_and_process_data(self.harga_2018_2021)
        self.harga_2022 = self.clean_and_process_data(self.harga_2022)
        self.harga_2023 = self.clean_and_process_data(self.harga_2023)
        self.harga_2024 = self.clean_and_process_data(self.harga_2024)

        # Mengelompokkan data di level provinsi dan bulanan
        self.harga_2018_2021.set_index('tanggal', inplace=True)
        self.harga_2018_2021_bulanan = self.harga_2018_2021.groupby(['kode_provinsi', 'provinsi', 'published_name']).resample('M')['harga'].mean().reset_index()
        
        self.harga_2022.set_index('tanggal', inplace=True)
        self.harga_2022_bulanan = self.harga_2022.groupby(['kode_provinsi', 'provinsi', 'published_name']).resample('M')['harga'].mean().reset_index()
        
        self.harga_2023.set_index('tanggal', inplace=True)
        self.harga_2023_bulanan = self.harga_2023.groupby(['kode_provinsi', 'provinsi', 'published_name']).resample('M')['harga'].mean().reset_index()
        
        self.harga_2024.set_index('tanggal', inplace=True)
        self.harga_2024_bulanan = self.harga_2024.groupby(['kode_provinsi', 'provinsi', 'published_name']).resample('M')['harga'].mean().reset_index()

        # Proses data ke dalam format pivot wide
        self.harga_2018_2021_bulanan_pivot = self.process_data(self.harga_2018_2021_bulanan)
        self.harga_2022_bulanan_pivot = self.process_data(self.harga_2022_bulanan)
        self.harga_2023_bulanan_pivot = self.process_data(self.harga_2023_bulanan)
        self.harga_2024_bulanan_pivot = self.process_data(self.harga_2024_bulanan)

        # Menggabungkan semua DataFrame secara vertikal
        gabungan_harga_bulanan = pd.concat([self.harga_2018_2021_bulanan_pivot, 
                                            self.harga_2022_bulanan_pivot, 
                                            self.harga_2023_bulanan_pivot, 
                                            self.harga_2024_bulanan_pivot], 
                                           ignore_index=True)
        
        gabungan_harga_bulanan = gabungan_harga_bulanan[gabungan_harga_bulanan['tanggal'] != '2024-10-31']

        
        return gabungan_harga_bulanan

# Menggunakan class DataFormattingHarga
# Membaca data dari file
harga_2018_2021 = pd.read_csv('Data/Data dari Kemendag/Migor dan Tepung 2018-2021.csv', sep=';')
harga_2022 = pd.read_csv('Data/Data dari Kemendag/Migor dan Tepung 2022.csv', sep=';')
harga_2023 = pd.read_csv('Data/Data dari Kemendag/Migor dan Tepung 2023.csv', sep=';')
harga_2024 = pd.read_excel('Data/Data dari Kemendag/Migor dan Tepung 2024.xlsx')

# Inisialisasi class
processor = DataFormattingHarga(harga_2018_2021, harga_2022, harga_2023, harga_2024)

# Menjalankan seluruh proses dan mendapatkan hasil gabungan
gabungan_harga_bulanan = processor.run()

# Menampilkan hasil
print(gabungan_harga_bulanan)

gabungan_harga_bulanan.to_excel('Data\harga_provinsi.xlsx', index=False)
