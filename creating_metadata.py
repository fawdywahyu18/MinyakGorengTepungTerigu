import pandas as pd

class MetadataCreator:
    def __init__(self, harga_provinsi_file):
        # Load DataFrame dari file Excel
        self.harga_provinsi = pd.read_excel(harga_provinsi_file)
        
        # Simpan kolom-kolom kecuali 'tanggal'
        self.colnames = list(self.harga_provinsi.columns)
        self.colnames.remove('tanggal')

        # Dictionary singkatan provinsi
        self.province_abbr_dict = {
            'aceh': 'Aceh',
            'sumut': 'Sumatera Utara',
            'sumbar': 'Sumatera Barat',
            'riau': 'Riau',
            'jambi': 'Jambi',
            'sumsel': 'Sumatera Selatan',
            'bengk': 'Bengkulu',
            'lamp': 'Lampung',
            'babel': 'Bangka Belitung',
            'kepri': 'Kepulauan Riau',
            'jakar': 'DKI Jakarta',
            'jawab': 'Jawa Barat',
            'jawat': 'Jawa Tengah',
            'jogja': 'D.I. Yogyakarta',
            'jatim': 'Jawa Timur',
            'bant': 'Banten',
            'bali': 'Bali',
            'ntbar': 'Nusa Tenggara Barat',
            'nttim': 'Nusa Tenggara Timur',
            'kalbar': 'Kalimantan Barat',
            'kalteng': 'Kalimantan Tengah',
            'kalsel': 'Kalimantan Selatan',
            'kaltim': 'Kalimantan Timur',
            'kalut': 'Kalimantan Utara',
            'sulut': 'Sulawesi Utara',
            'sulteng': 'Sulawesi Tengah',
            'sulsel': 'Sulawesi Selatan',
            'sultra': 'Sulawesi Tenggara',
            'goro': 'Gorontalo',
            'sulbar': 'Sulawesi Barat',
            'maluku': 'Maluku',
            'malut': 'Maluku Utara',
            'papbar': 'Papua Barat',
            'papua': 'Papua'
        }

    # Fungsi untuk mencocokkan singkatan provinsi dengan nama provinsi
    def find_province_from_series(self, series_name):
        for abbr, province in self.province_abbr_dict.items():
            if series_name.startswith(abbr):
                return province
        return 'Unknown Province'

    # Fungsi untuk membuat label berdasarkan nama series
    def create_label(self, series_name):
        province_name = self.find_province_from_series(series_name)
        commodity = series_name.split('_')[-1]
        
        commodity_mapping = {
            'mgsc': 'Minyak Goreng Sawit Curah',
            'mgskp': 'Minyak Goreng Sawit Kemasan Premium',
            'tt': 'Tepung Terigu'
        }
        
        return f"{province_name} - {commodity_mapping.get(commodity, 'Unknown Commodity')}"
    
    # Fungsi untuk membuat metadata
    def create_metadata(self):
        # Buat DataFrame metadata
        df = pd.DataFrame({'series': self.colnames})

        # Buat kolom 'label'
        df['label'] = df['series'].apply(self.create_label)

        # Kolom 'freq' diisi dengan 'M' (bulanan)
        df['freq'] = 'M'

        # Kolom 'log_trans' diisi dengan True (asumsi perlu transformasi log)
        df['log_trans'] = True

        # Kolom 'tepung_terigu': TRUE jika series berakhiran '_tt'
        df['tepung_terigu'] = df['series'].apply(lambda x: True if x.endswith('_tt') else False)

        # Kolom 'minyak_goreng': TRUE jika series berakhiran '_mgsc' atau '_mgskp'
        df['minyak_goreng'] = df['series'].apply(lambda x: True if x.endswith('_mgsc') or x.endswith('_mgskp') else False)

        # Kolom unit diisi dengan 'IDR'
        df['unit'] = 'IDR'

        return df

    # Fungsi untuk menyimpan metadata ke file Excel
    def save_metadata(self, output_file):
        df_metadata = self.create_metadata()
        df_metadata.to_excel(output_file, index=False)
        print(f"Metadata saved to {output_file}")

# Penggunaan class MetadataCreator
metadata_creator = MetadataCreator('Data\Data Peramalan\harga_provinsi.xlsx')

# Membuat metadata dan menyimpannya ke file Excel
metadata_creator.save_metadata('Data\Metadata\metadata_harga test.xlsx')
