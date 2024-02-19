
library(CAbioacoustics)
library(tidyverse)
library(arrow)

# if TRUE, connection is working
cb_check_s3()


# get S3 bucket contents; convert to arrow table --------------------------

# get s3 contents in data frame form
# this can take a long time (>2 hours)
s3_df <- cb_get_s3_df()

# converting to arrow table makes querying much faster (and smaller than csv)
s3_arrow <-
  as_arrow_table(s3_df) |>
  # just keep S3 file paths for now
  select(Key)


# save arrow data frame ---------------------------------------------------

s3_arrow |>
  write_dataset(
    # save with date in the filename
    path = str_c(here::here('s3_bucket_arrow'), '_', Sys.Date()),
    format = 'parquet'
  )
