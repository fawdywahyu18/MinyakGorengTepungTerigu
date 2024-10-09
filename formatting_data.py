"""
Data Formatting untuk Proyeksi Menggunakan DFM
@author: fawdywahyu18
"""

import pandas as pd
import numpy as np

class DataFormattingHarga:
    def __init__(self, *args):
        # Menyimpan semua dataset dalam satu list
        self.datasets = args

    # Fungsi untuk membuat singkatan provinsi
    def create_abbreviation(self, province):
        province_abbr_dict = {
            'Aceh': 'aceh',
            'Sumatera Utara': 'sumut',
            'Sumatera Barat': 'sumbar',
            'Riau': 'riau',
            'Jambi': 'jambi',
            'Sumatera Selatan': 'sumsel',
            'Bengkulu': 'bengk',
            'Lampung': 'lamp',
            'Bangka Belitung': 'babel',
            'Kepulauan Bangka Belitung': 'babel',
            'Kepulauan Riau': 'kepri',
            'DKI Jakarta': 'jakar',
            'D.K.I. Jakarta': 'jakar',
            'Jawa Barat': 'jawab',
            'Jawa Tengah': 'jawat',
            'D.I. Yogyakarta': 'jogja',
            'Jawa Timur': 'jatim',
            'Banten': 'bant',
            'Bali': 'bali',
            'Nusa Tenggara Barat': 'ntbar',
            'Nusa Tenggara Timur': 'nttim',
            'Kalimantan Barat': 'kalbar',
            'Kalimantan Tengah': 'kalteng',
            'Kalimantan Selatan': 'kalsel',
            'Kalimantan Timur': 'kaltim',
            'Kalimantan Utara': 'kalut',
            'Sulawesi Utara': 'sulut',
            'Sulawesi Tengah': 'sulteng',
            'Sulawesi Selatan': 'sulsel',
            'Sulawesi Tenggara': 'sultra',
            'Gorontalo': 'goro',
            'Sulawesi Barat': 'sulbar',
            'Maluku': 'maluku',
            'Maluku Utara': 'malut',
            'Papua Barat': 'papbar',
            'Papua Barat Daya': 'papbar',
            'Papua': 'papua',
            'Papua Selatan': 'papua',
            'Papua Tengah': 'papua',
            'Papua Pegunungan': 'papua'
        }
        return province_abbr_dict.get(province, province)
    
    # Fungsi untuk membersihkan dan memproses data
    def clean_and_process_data(self, df):
        # Mengubah format tanggal
        df['tanggal'] = pd.to_datetime(df['tanggal'], format='%d/%m/%y', errors='coerce')

        # Cek apakah kolom harga adalah string, dan jika ya, lakukan replace koma
        if pd.api.types.is_string_dtype(df['harga']):
            df['harga'] = df['harga'].str.replace(',', '').astype(float)
        
        # Membersihkan data dari nilai inf dan NaN
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.dropna(subset=['harga', 'provinsi'])
        return df

    # Fungsi untuk menghitung harga bulanan dan koefisien variasi (kv)
    def process_annual_data(self, df):
        df.set_index('tanggal', inplace=True)
        df_bulanan = df.groupby(['kode_provinsi', 'provinsi', 'published_name']).resample('M')['harga'].agg(['mean', 'std']).reset_index()
        df_bulanan['kv'] = df_bulanan['std'] / df_bulanan['mean']
        df_bulanan = df_bulanan.rename(columns={'mean': 'harga'}).drop(columns=['std'])
        return df_bulanan
    
    # Fungsi untuk memproses semua dataset tahunan
    def process_all_datasets(self):
        results = []
        for df in self.datasets:
            clean_df = self.clean_and_process_data(df)
            bulanan_df = self.process_annual_data(clean_df)
            results.append(bulanan_df)
        return pd.concat(results, ignore_index=True)
    
    # Fungsi untuk mengubah data ke format pivot wide
    def process_data(self, df):
        # Mapping untuk membuat nama kolom singkatan
        komoditas_mapping = {
            'Minyak Goreng Sawit Curah': 'mgsc',
            'Minyak Goreng Sawit Kemasan Premium': 'mgskp',
            'Tepung Terigu': 'tt',
            'Minyak Goreng Curah': 'mgsc',
            'Minyak Goreng Kemasan Premium': 'mgskp',
            'Minyak Goreng Kemasan': 'mgskp'
        }

        df['commodity_short'] = df['published_name'].map(komoditas_mapping)
        df['provinsi_singkat'] = df['provinsi'].apply(self.create_abbreviation)
        df['nama_gabungan'] = df['provinsi_singkat'].str.lower() + '_' + df['commodity_short']

        # Lakukan pivot untuk membuat data menjadi bentuk lebar (wide format)
        df_pivot = df.pivot_table(index='tanggal', columns='nama_gabungan', values=['harga', 'kv'], aggfunc='mean').reset_index()
        return df_pivot

    # Fungsi utama untuk menjalankan seluruh proses
    def run(self):
        # Memproses semua dataset
        gabungan_harga_bulanan = self.process_all_datasets()

        # Memproses data ke dalam format pivot wide
        gabungan_harga_bulanan_pivot = self.process_data(gabungan_harga_bulanan)

        # Menghapus baris dengan tanggal tertentu
        gabungan_harga_bulanan_pivot = gabungan_harga_bulanan_pivot[gabungan_harga_bulanan_pivot['tanggal'] != '2024-10-31']

        return gabungan_harga_bulanan_pivot

# Menggunakan class DataFormattingHarga
# Membaca data dari file
harga_2018_2021 = pd.read_csv('Data/Data dari Kemendag/Migor dan Tepung 2018-2021.csv', sep=';')
harga_2022 = pd.read_csv('Data/Data dari Kemendag/Migor dan Tepung 2022.csv', sep=';')
harga_2023 = pd.read_csv('Data/Data dari Kemendag/Migor dan Tepung 2023.csv', sep=';')
harga_2024 = pd.read_excel('Data/Data dari Kemendag/Migor dan Tepung 2024.xlsx')

# Inisialisasi class dengan semua dataset
processor = DataFormattingHarga(harga_2018_2021, harga_2022, harga_2023, harga_2024)

# Menjalankan seluruh proses dan mendapatkan hasil gabungan
gabungan_harga_bulanan = processor.run()

# Menyimpan data ke dalam Excel
harga_bulanan = gabungan_harga_bulanan.xs('harga', axis=1, level=0)
kv_bulanan = gabungan_harga_bulanan.xs('kv', axis=1, level=0)

n_rows = len(harga_bulanan)
tanggal_range = pd.date_range(start='2018-01-01', periods=n_rows, freq='M')

harga_bulanan['tanggal'] = tanggal_range
kv_bulanan['tanggal'] = tanggal_range
kv_bulanan = kv_bulanan.rename(columns=lambda x: x + '_kv' if x != 'tanggal' else x)

# Menyimpan hasil ke file Excel
harga_bulanan.to_excel('Data/Data Peramalan/harga_provinsi test.xlsx', index=False)
kv_bulanan.to_excel('Data/Data Peramalan/kv_provinsi test.xlsx', index=False)
