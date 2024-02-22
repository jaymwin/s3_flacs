

# load libraries ----------------------------------------------------------

library(tidyverse)
library(arrow)
library(CAbioacoustics)


# open parquet dataset ----------------------------------------------------

# read in parquet data (most recent S3 bucket contents)
s3_filepaths_pq <- open_dataset(here::here('s3_filepath_arrow_2024-02-22'))
s3_size_pq <- open_dataset(here::here('s3_size_arrow_2024-02-22'))


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

s3_filepaths_duck_db <- 
  s3_filepaths_pq |> 
  # convert from arrow to duck db for easier wrangling
  to_duckdb() |> 
  mutate(id = row_number())

s3_size_duck_db <- 
  s3_size_pq |> 
  # convert from arrow to duck db for easier wrangling
  to_duckdb() |> 
  mutate(id = row_number())

s3_filepaths_duck_db <-
  s3_filepaths_duck_db |> 
  left_join(s3_size_duck_db) |> 
  select(-id)

s3_filepaths_duck_db |> 
  mutate(
    study = regexp_extract(Key, studies),
    # e.g., '/2021/'
    year = regexp_extract(Key, "/[0-9]{4}/"),
    # get actual year out of that
    year = regexp_extract(year, "[0-9]{4}"),
    Size = as.numeric(Size)
  ) |> 
  group_by(study, year) |> 
  summarise(
    n_flacs = n(),
    size_bytes = sum(Size)
  ) |> 
  # pull into memory now
  collect() |> 
  arrange(study, year)
