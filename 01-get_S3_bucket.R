
library(CAbioacoustics)
library(tidyverse)
library(arrow)

cb_check_s3()


# get S3 bucket contents; convert to arrow table --------------------------

s3_df <- cb_get_s3_df()

s3_df |>
  write_rds(here::here('s3_df.rds'))

# arrow table makes querying much faster (and smaller than csv)
s3_arrow <-
  as_arrow_table(s3_df) |>
  select(Key)


# save arrow data frame ---------------------------------------------------

s3_arrow |>
  write_dataset(
    path = str_c(here::here('s3_bucket_arrow'), '_', Sys.Date()),
    format = 'parquet'
  )


s3_pq <- open_dataset(str_c(here::here('s3_bucket_arrow'), '_', Sys.Date()))

s3_pq |>
  filter(str_detect(Key, '.flac')) |>
  collect()
