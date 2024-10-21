# Estimasi Dynamic Factor Modelling
# Riset Kerjasama ADB dan Kemendag Indonesia
# author: fawdywahyu18

library(dfms)
library(xts)
library(readxl)
library(magrittr)
library(vars)
library(writexl)

# Fungsi untuk log-transformasi dan first-differencing data
log_transform_diff <- function(data, metadata) {
  data[, metadata$log_trans] %<>% log()
  data_diff <- diff(data)
  data_diff[is.infinite(data_diff) & data_diff < 0] <- NA
  data_diff[is.nan(data_diff)] <- NA
  return(data_diff)
}

# Fungsi untuk plot dan simpan gambar
save_plot <- function(data, filename, plot_func, width = 800, height = 600) {
  png(filename, width = width, height = height)
  plot_func(data)
  dev.off()
}

# Fungsi untuk estimasi Dynamic Factor Model (DFM)
estimate_dfm <- function(xts_training_diff, rank, lag) {
  model <- DFM(xts_training_diff, r = rank, p = lag)
  return(model)
}

# Fungsi untuk robustness check
robustness_check <- function(xts_training_diff, r_values, p_values, output_file) {
  hasil_model <- list()
  for (r in r_values) {
    for (p in p_values) {
      model_iterasi <- DFM(X = xts_training_diff, r = r, p = p)
      hasil_model[[paste("r", r, "p", p, sep = "_")]] <- as.data.frame(model_iterasi$A)
      cat("Model dengan r =", r, "dan p =", p, "selesai.\n")
    }
  }
  writexl::write_xlsx(hasil_model, output_file)
}

# Fungsi untuk memproses prediksi dan menghitung error
process_forecast <- function(xts_training_diff, test_data, kolom_terpilih, rank, lag, output_file) {
  model_fc <- DFM(xts_training_diff, r = rank, p = lag, em.method = 'BM')
  fc_ori <- predict(model_fc, h = 6, standardized = FALSE)
  df_fc <- fc_ori$X_fcst[, kolom_terpilih]
  
  nilai_terakhir <- tail(xts_training_diff[, kolom_terpilih], 1)
  fc_last <- as.numeric(nilai_terakhir)
  
  log_transformed <- cumsum(c(fc_last, df_fc))
  original_scale <- exp(log_transformed)[-1]
  
  test_data$fc <- original_scale
  test_data$abs_error <- abs(test_data - original_scale)
  test_data$pct_error <- abs(test_data - original_scale) * 100 / original_scale
  
  writexl::write_xlsx(data.frame(tanggal = index(test_data), coredata(test_data)), output_file)
}

# Memuat data
load_data <- function() {
  list(
    harga_provinsi = read_excel('Data/Data Peramalan/harga_provinsi.xlsx'),
    kv_provinsi = read_excel('Data/Data Peramalan/kv_provinsi.xlsx'),
    data_eksternal = read_excel('Data/Data Peramalan/data_eksternal.xlsx'),
    harga_nasional = read_excel('Data/Data Peramalan/harga_nasional_pdsi.xlsx'),
    metadata_harga = read_excel('Data/Metadata/metadata_harga.xlsx'),
    metadata_kv = read_excel('Data/Metadata/metadata_kv.xlsx'),
    metadata_eksternal = read_excel('Data/Metadata/metadata_eksternal.xlsx'),
    metadata_nasional = read_excel('Data/Metadata/metadata_nasional.xlsx')
  )
}

# Inisialisasi data
data_list <- load_data()
data_utama <- cbind(data_list$harga_nasional, data_list$harga_provinsi, data_list$kv_provinsi, data_list$data_eksternal)[, -c(1, 204, 205)]
data_utama$tanggal <- as.Date(data_utama$tanggal)
xts_utama <- xts(data_utama[, -which(names(data_utama) == "tanggal")], order.by = data_utama$tanggal)

# Set training dan test set
n_test <- 6
n_total <- nrow(xts_utama)
xts_training <- xts_utama[1:(n_total - n_test), ]
xts_test <- xts_utama[(n_total - n_test + 1):n_total, ]

# Metadata
metadata <- rbind(data_list$metadata_nasional, data_list$metadata_harga, data_list$metadata_kv, data_list$metadata_eksternal)

# Estimasi untuk Minyak Goreng
kolom_terpilih_migor <- metadata$series[metadata$minyak_goreng == TRUE]
xts_training_migor <- xts_training[, kolom_terpilih_migor]
metadata_migor <- metadata[metadata$minyak_goreng == TRUE, ]

xts_training_migor_diff <- log_transform_diff(xts_training_migor, metadata_migor)

save_plot(scale(xts_training_migor_diff), "Laporan/Temuan Utama/grafik_xts_training_diff_migor.png", plot)

# Struktur model dan estimasi faktor untuk Minyak Goreng
ic_migor <- ICr(xts_training_migor_diff)
rank_dipilih_migor <- 2
lag_op_migor <- vars::VARselect(ic_migor$F_pca[, 1:rank_dipilih_migor])
lag_dipilih_migor <- unname(lag_op_migor$selection['HQ(n)'])

model_migor <- estimate_dfm(xts_training_migor_diff, rank_dipilih_migor, lag_dipilih_migor)

save_plot(model_migor, "Laporan/Temuan Utama/plot_training_dfm_migor.png", function(data) plot(model_migor, method = "all", type = "individual"))

# Robustness check
robustness_check(xts_training_migor_diff, c(2, 3, 4, 5), c(1, 2, 3), 'Laporan/Robustness Check/Robustness Check Training Migor.xlsx')

# Peramalan dan evaluasi untuk Minyak Goreng
process_forecast(xts_training_migor_diff, xts_test[, kolom_terpilih_migor], c("nas_mgckc", "nas_mgskp"), rank_dipilih_migor, lag_dipilih_migor, 'Laporan/Evaluasi Peramalan/Evaluasi Peramalan 6 bulan MGCKC.xlsx')

# Estimasi untuk Tepung Terigu
kolom_terpilih_TT <- metadata$series[metadata$tepung_terigu == TRUE]
xts_training_TT <- xts_training[, kolom_terpilih_TT]
metadata_TT <- metadata[metadata$tepung_terigu == TRUE, ]

xts_training_TT_diff <- log_transform_diff(xts_training_TT, metadata_TT)

save_plot(scale(xts_training_TT_diff), "Laporan/Temuan Utama/grafik_xts_training_diff_TT.png", plot)

# Struktur model dan estimasi faktor untuk Tepung Terigu
ic_TT <- ICr(xts_training_TT_diff)
rank_dipilih_TT <- 3
lag_op_TT <- vars::VARselect(ic_TT$F_pca[, 1:rank_dipilih_TT])
lag_dipilih_TT <- unname(lag_op_TT$selection['SC(n)'])

model_TT <- estimate_dfm(xts_training_TT_diff, rank_dipilih_TT, lag_dipilih_TT)

save_plot(model_TT, "Laporan/Temuan Utama/plot_training_dfm_TT.png", function(data) plot(model_TT, method = "all", type = "individual"))

# Robustness check untuk Tepung Terigu
robustness_check(xts_training_TT_diff, c(1, 2, 3, 4, 5), c(1, 2, 3), 'Laporan/Robustness Check/Robustness Check Training TT.xlsx')

# Peramalan dan evaluasi untuk Tepung Terigu
process_forecast(xts_training_TT_diff, xts_test[, kolom_terpilih_TT], 'nas_tt', rank_dipilih_TT, lag_dipilih_TT, 'Laporan/Evaluasi Peramalan/Evaluasi Peramalan 6 bulan TT.xlsx')
