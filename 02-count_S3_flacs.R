

# load libraries ----------------------------------------------------------

library(tidyverse)
library(arrow)
library(CAbioacoustics)


# open parquet dataset ----------------------------------------------------

# read in parquet data (most recent S3 bucket contents)
s3_pq <- open_dataset(here::here('s3_bucket_arrow_2024-02-18'))


# get flac counts ---------------------------------------------------------

# tally flacs by study and year
s3_pq |> 
  filter(str_detect(Key, '.flac')) |> 
  mutate(
    study = case_when(
      # str_extract doesn't work with arrow so use str_detect instead
      str_detect(Key, 'ARU_Coastal_Barred') ~ 'Coastal Barred',
      str_detect(Key, 'ARU_Modoc_Projects') ~ 'Modoc Projects',
      str_detect(Key, 'ARU_Sierra_Removal') ~ 'Sierra Removal',
      str_detect(Key, 'ARU_Sierra_Monitoring') ~ 'Sierra Monitoring',
      str_detect(Key, 'ARU_Sierra_Projects') ~ 'Sierra Projects'
    ),
    year = case_when(
      # probably more elegant way to do this with base R functions...
      str_detect(Key, '/2018/') ~ '2018',
      str_detect(Key, '/2019/') ~ '2019',
      str_detect(Key, '/2020/') ~ '2020',
      str_detect(Key, '/2021/') ~ '2021',
      str_detect(Key, '/2022/') ~ '2022',
      str_detect(Key, '/2023/') ~ '2023',
    )
  ) |> 
  group_by(study, year) |> 
  tally(name = 'n_flacs') |> 
  # pull into memory now
  collect()
