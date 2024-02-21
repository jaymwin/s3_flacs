

# load libraries ----------------------------------------------------------

library(tidyverse)
library(arrow)
library(CAbioacoustics)


# open parquet dataset ----------------------------------------------------

# read in parquet data (most recent S3 bucket contents)
s3_pq <- open_dataset(here::here('s3_bucket_arrow_2024-02-19'))


# get flac counts ---------------------------------------------------------

# tally flacs by study and year
studies <- 
  c(
    'ARU_Coastal_Barred', 
    'ARU_Modoc_Projects', 
    'ARU_Sierra_Removal', 
    'ARU_Sierra_Monitoring', 
    'ARU_Sierra_Projects'
    ) |> 
  str_flatten(collapse = '|')

# perform tally
s3_pq |> 
  # convert from arrow to duck db for easier wrangling
  to_duckdb() |> 
  # look at flac files only
  filter(str_detect(Key, '.flac')) |> 
  mutate(
    study = regexp_extract(Key, studies),
    # e.g., '/2021/'
    year = regexp_extract(Key, "/[0-9]{4}/"),
    # get actual year out of that
    year = regexp_extract(year, "[0-9]{4}"), 
  ) |> 
  group_by(study, year) |> 
  tally(name = 'n_flacs') |> 
  # pull into memory now
  collect() |> 
  arrange(study, year)
