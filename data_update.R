library(readr)
library(dplyr)
library(stringr)
library(arrow)
library(lubridate)
library(jsonlite)
library(here)
library(fs)

# Data download
# Folder with parquet database
link_dir <- here("raw-data/parquet")

# Download data passport from source
pp <- 'https://data.gov.ua/dataset/06779371-308f-42d7-895e-5a39833375f0/datapackage'
pass <- jsonlite::read_json(pp, simplifyVector = TRUE)
urls <- pass$resources

year <- urls$name |> tail(2) |> str_extract("([0-9]{4})$", group = 1) |> as.numeric()
path <- urls$path |> tail(2) 
file <- tempfile()
download.file(path, file)
new_file <- unzip(file, exdir = tempdir())

# Read in data
new_data <- read_delim(new_file, 
                       delim=";", 
                       col_types = cols_only(
                         PERSON = col_character(),
                         REG_ADDR_KOATUU = col_character(),
                         OPER_CODE = col_integer(),
                         OPER_NAME = col_character(), 
                         D_REG = col_character(),
                         DEP_CODE = col_character(),
                         DEP = col_character(),
                         BRAND = col_character(), 
                         MODEL = col_character(), 
                         VIN = col_character(),
                         MAKE_YEAR = col_integer(), 
                         COLOR = col_character(),
                         KIND = col_character(), 
                         BODY = col_character(),
                         PURPOSE = col_character(),
                         FUEL = col_character(),
                         CAPACITY = col_double(),
                         OWN_WEIGHT = col_double(),
                         TOTAL_WEIGHT = col_double(),
                         N_REG_NEW = col_character(),
                       ),
                       locale = locale(encoding = "UTF-8", decimal_mark = ",")
                       ) |> 
  select(-VIN)

# Function to write parquet file to correct folder creating one if it doesn't exist
write_parquet_dir <- function(df, year, link){
  
  # Find folder with the latest available year
  current_max_year <- dir_ls(here(link), type = "directory") |> 
    str_extract("([0-9]{4})$", group = 1) |> 
    as.numeric() |> 
    max()  
  
  # Year of new data
  new_year <- df |> select({{year}}) |> max()
  
  # Write file to folder 
  if(new_year == current_max_year - 1) {
    write_parquet(df, 
                  here(link, paste0("YEAR=", new_year), "part-0.parquet"))
  } else {
    # create new directory if it doesn't exist
    dir_create(here(link, paste0("YEAR=", new_year)))
    write_parquet(df, 
                  here(link, paste0("YEAR=", new_year), "part-0.parquet"))
  }
}

new_data %>% 
  mutate(D_REG = lubridate::parse_date_time2(D_REG, orders = c("%d.%m.%Y", "%d.%m.%y"))) |> 
  distinct() |> 
  mutate(YEAR = year(D_REG)) |>  
  write_parquet_dir(year = YEAR, 
                    link = link_dir)




