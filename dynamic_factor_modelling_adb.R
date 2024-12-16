# Load Library
library(dfms)
library(xts)
library(readxl)
library(magrittr)
library(vars)
library(writexl)

# Fungsi untuk mempersiapkan data
prepare_data <- function(wd, 
                         file_harga_provinsi, file_kv_provinsi, file_data_eksternal, file_harga_nasional,
                         file_metadata_harga, file_metadata_kv, file_metadata_eksternal, file_metadata_nasional,
                         n_test = 6) {
  setwd(wd)
  
  # Memuat data
  harga_provinsi <- read_excel(file_harga_provinsi)
  kv_provinsi <- read_excel(file_kv_provinsi)
  data_eksternal <- read_excel(file_data_eksternal)
  harga_nasional <- read_excel(file_harga_nasional)
  
  # Memuat metadata
  metadata_harga <- read_excel(file_metadata_harga)
  metadata_kv <- read_excel(file_metadata_kv)
  metadata_eksternal <- read_excel(file_metadata_eksternal)
  metadata_nasional <- read_excel(file_metadata_nasional)
  
  # Menumpuk data ke samping
  data_utama <- cbind(harga_nasional, harga_provinsi, kv_provinsi, data_eksternal)
  data_utama <- data_utama[, -c(1, 204, 205)]
  data_utama$tanggal <- as.Date(data_utama$tanggal)
  
  xts_utama <- xts(data_utama[, -which(names(data_utama) == "tanggal")], 
                   order.by = data_utama$tanggal)
  
  n_total <- nrow(xts_utama)
  
  # Data training dan test
  xts_training <- xts_utama[1:(n_total - n_test), ]
  xts_test <- xts_utama[(n_total - n_test + 1):n_total, ]
  
  # Menumpuk metadata ke bawah
  metadata <- rbind(metadata_nasional, metadata_harga, metadata_kv, metadata_eksternal)
  
  list(xts_training = xts_training, 
       xts_test = xts_test, 
       metadata = metadata)
}


# Fungsi untuk menyiapkan data subset sesuai komoditas (contoh: minyak goreng atau tepung terigu)
prepare_subset <- function(xts_training, metadata, filter_col) {
  kolom_terpilih <- metadata$series[metadata[[filter_col]] == TRUE]
  metadata_subset <- metadata[metadata[[filter_col]] == TRUE, ]
  
  xts_training_subset <- xts_training[, kolom_terpilih]
  
  # Transformasi log untuk series yang memerlukan
  if (any(metadata_subset$log_trans)) {
    xts_training_subset[, metadata_subset$log_trans] %<>% log()
  }
  
  # Differencing
  xts_training_subset_diff = diff(xts_training_subset)
  xts_training_subset_diff[is.infinite(xts_training_subset_diff) & xts_training_subset_diff < 0] <- NA
  xts_training_subset_diff[is.nan(xts_training_subset_diff)] <- NA
  
  list(xts_diff = xts_training_subset_diff, xts_orig = xts_training_subset, metadata_subset = metadata_subset)
}


# Fungsi untuk menentukan jumlah faktor optimal
determine_factors <- function(xts_diff) {
  ic = ICr(xts_diff)
  return(ic)
}


# Fungsi untuk menentukan lag optimal dengan VAR
determine_lag <- function(F_pca, r) {
  lag_op = vars::VARselect(F_pca[, 1:r])
  # Ambil salah satu kriteria (di contoh ini SC(n) atau HQ(n))
  # Sesuaikan dengan preferensi Anda.
  lag_chosen = unname(lag_op$selection['HQ(n)'])
  return(lag_chosen)
}


# Fungsi untuk estimasi model DFM
estimate_DFM_model <- function(xts_diff, r, p) {
  model = DFM(xts_diff, r = r, p = p)
  return(model)
}


# Fungsi untuk melakukan Robustness Check
robustness_check <- function(xts_diff, r_values, p_values, output_file) {
  hasil_model <- list()
  
  for (r in r_values) {
    for (p in p_values) {
      model_iterasi <- DFM(X = xts_diff, r = r, p = p)
      hasil_model[[paste("r", r, "p", p, sep = "_")]] <- as.data.frame(model_iterasi$A)
      cat("Model dengan r =", r, "dan p =", p, "selesai.\n")
    }
  }
  
  writexl::write_xlsx(hasil_model, output_file)
}


# Fungsi untuk peramalan
forecast_DFM_model <- function(xts_diff, r, p, h = 6, em_method = 'BM', standardized = TRUE) {
  model_fc <- DFM(xts_diff, r = r, p = p, em.method = em_method)
  fc <- predict(model_fc, h = h, standardized = standardized)
  fc_ori <- predict(model_fc, h = h, standardized = FALSE)
  
  list(model_fc = model_fc, fc = fc, fc_ori = fc_ori)
}


# Fungsi untuk mengekstrak hasil ramalan ke skala asli
# Asumsi: Data awal adalah log, lalu di-difference. Kita kembalikan ke skala asli.
recover_original_scale <- function(fc_values, last_observed) {
  # log_transformed adalah akumulasi dari differencing
  log_transformed <- cumsum(c(last_observed, fc_values))
  original_scale <- exp(log_transformed)[-1]
  return(original_scale)
}


# Fungsi untuk evaluasi peramalan
evaluate_forecast <- function(test_data, fc_data, output_file) {
  test_data$fc <- fc_data
  test_data$abs_error <- abs(test_data[,1] - test_data$fc)
  test_data$pct_error <- abs(test_data[,1] - test_data$fc)*100 / test_data$fc
  out_df <- data.frame(tanggal = index(test_data), coredata(test_data))
  writexl::write_xlsx(out_df, output_file)
}


# =======================================================================================
# Contoh penggunaan fungsi-fungsi di atas (sesuaikan path direktori dan file)
# =======================================================================================

wd_laptop <- ''

data_list <- prepare_data(
  wd = wd_laptop,
  file_harga_provinsi = 'Data/Data Peramalan/harga_provinsi.xlsx',
  file_kv_provinsi = 'Data/Data Peramalan/kv_provinsi.xlsx',
  file_data_eksternal = 'Data/Data Peramalan/data_eksternal.xlsx',
  file_harga_nasional = 'Data/Data Peramalan/harga_nasional_pdsi.xlsx',
  file_metadata_harga = 'Data/Metadata/metadata_harga.xlsx',
  file_metadata_kv = 'Data/Metadata/metadata_kv.xlsx',
  file_metadata_eksternal = 'Data/Metadata/metadata_eksternal.xlsx',
  file_metadata_nasional = 'Data/Metadata/metadata_nasional.xlsx',
  n_test = 6
)

xts_training <- data_list$xts_training
xts_test <- data_list$xts_test
metadata <- data_list$metadata


# Contoh untuk Minyak Goreng
subset_migor <- prepare_subset(xts_training, metadata, "minyak_goreng")
ic_migor <- determine_factors(subset_migor$xts_diff)
# Tentukan jumlah faktor dan lag dipilih (misal r=2)
rank_dipilih_migor = 2
lag_dipilih_migor = determine_lag(ic_migor$F_pca, rank_dipilih_migor)

model_migor <- estimate_DFM_model(subset_migor$xts_diff, r = rank_dipilih_migor, p = lag_dipilih_migor)
summary(model_migor)

# Robustness Check Migor
robustness_check(subset_migor$xts_diff, r_values = c(2,3,4,5), p_values = c(1,2,3), 
                 output_file = 'Laporan/Robustness Check/Robustness Check Training Migor.xlsx')


# Forecast Migor
fc_migor_list <- forecast_DFM_model(subset_migor$xts_diff, r = rank_dipilih_migor, p = lag_dipilih_migor, h = 6)
df_fc_migor <- fc_migor_list$fc_ori$X_fcst[, c("nas_mgckc", "nas_mgskp")]

# Kembalikan ke skala asli untuk nas_mgckc
last_mgckc <- tail(subset_migor$xts_orig[,"nas_mgckc"], 1)
original_scale_mgckc <- recover_original_scale(df_fc_migor[,"nas_mgckc"], as.numeric(last_mgckc))

# Kembalikan ke skala asli untuk nas_mgskp
last_mgskp <- tail(subset_migor$xts_orig[,"nas_mgskp"], 1)
original_scale_mgskp <- recover_original_scale(df_fc_migor[,"nas_mgskp"], as.numeric(last_mgskp))

# Evaluasi peramalan Migor
xts_test_migor = xts_test[, metadata$series[metadata$minyak_goreng == TRUE]]
test_nas_migor = xts_test_migor[, c("nas_mgckc", "nas_mgskp")]

evaluate_forecast(test_nas_migor$nas_mgckc, original_scale_mgckc, 
                  'Laporan/Evaluasi Peramalan/Evaluasi Peramalan 6 bulan MGCKC.xlsx')
evaluate_forecast(test_nas_migor$nas_mgskp, original_scale_mgskp, 
                  'Laporan/Evaluasi Peramalan/Evaluasi Peramalan 6 bulan MGSKP.xlsx')


# Contoh untuk Tepung Terigu (TT)
subset_TT <- prepare_subset(xts_training, metadata, "tepung_terigu")
ic_TT <- determine_factors(subset_TT$xts_diff)

rank_dipilih_TT = 3
lag_dipilih_TT = determine_lag(ic_TT$F_pca, rank_dipilih_TT)

model_TT <- estimate_DFM_model(subset_TT$xts_diff, r = rank_dipilih_TT, p = lag_dipilih_TT)
summary(model_TT)

# Robustness Check TT
robustness_check(subset_TT$xts_diff, r_values = c(1,2,3,4,5), p_values = c(1,2,3), 
                 output_file = 'Laporan/Robustness Check/Robustness Check Training TT.xlsx')


# Forecast TT
fc_TT_list <- forecast_DFM_model(subset_TT$xts_diff, r = rank_dipilih_TT, p = lag_dipilih_TT, h = 6)
df_fc_TT <- fc_TT_list$fc_ori$X_fcst[,'nas_tt']

last_tt <- tail(subset_TT$xts_orig[, "nas_tt"], 1)
original_scale_TT <- recover_original_scale(df_fc_TT, as.numeric(last_tt))

xts_test_TT = xts_test[, metadata$series[metadata$tepung_terigu == TRUE]]
test_nas_TT = xts_test_TT[, 'nas_tt']
evaluate_forecast(test_nas_TT, original_scale_TT, 
                  'Laporan/Evaluasi Peramalan/Evaluasi Peramalan 6 bulan TT.xlsx')

# Selesai.
