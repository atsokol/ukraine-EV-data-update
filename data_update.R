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

# Resources in the datapackage are NOT listed in chronological order (and the
# source appends duplicate entries for older years), so tail(1) can grab a
# stale year's file. Select the resource by the latest year parsed from its name.
res_year <- urls$name |> str_extract("([0-9]{4})$", group = 1) |> as.numeric()
latest_idx <- which(res_year == max(res_year, na.rm = TRUE)) |> tail(1)

year <- res_year[latest_idx]
path <- urls$path[latest_idx]
file <- tempfile()
download.file(path, file)
new_file <- unzip(file, exdir = tempdir())

# Read in data
#
# The source periodically changes the CSV layout. In 2026 it merged the
# separate OPER_CODE + OPER_NAME columns into a single "<code> - <name>"
# column (header like "CD.OPER_CODE||'-'||CD.OPER_NAME") and dropped/renamed
# others (REG_ADDR_KOATUU, DEP_CODE, N_REG_NEW). cols_only() matches by exact
# name, so those columns silently vanished from the parquet — which then broke
# the OPER_CODE filter in data_summarise.R and dropped the whole year.
#
# Read every column as text (robust to renames/reorders) and normalise to the
# canonical schema below so each yearly file carries the same columns.
raw <- read_delim(new_file,
                  delim = ";",
                  col_types = cols(.default = col_character()),
                  locale = locale(encoding = "UTF-8", decimal_mark = ",")) |>
  select(-any_of("VIN"))

# Reconstruct OPER_CODE / OPER_NAME when the source ships them merged into one
# "<code> - <name>" column.
if (!all(c("OPER_CODE", "OPER_NAME") %in% names(raw))) {
  oper_col <- names(raw)[str_detect(names(raw), "OPER") & str_detect(names(raw), fixed("||"))]
  if (length(oper_col) >= 1) {
    raw <- raw |>
      mutate(
        OPER_CODE = str_extract(.data[[oper_col[1]]], "^\\s*\\d+"),
        OPER_NAME = str_trim(str_replace(.data[[oper_col[1]]], "^\\s*\\d+\\s*-\\s*", ""))
      )
  }
}

# Canonical schema (matches the historic parquet files). Any column the source
# omits is added as NA so open_dataset() sees a consistent schema across years.
canonical <- c("PERSON", "REG_ADDR_KOATUU", "OPER_CODE", "OPER_NAME", "D_REG",
               "DEP_CODE", "DEP", "BRAND", "MODEL", "MAKE_YEAR", "COLOR",
               "KIND", "BODY", "PURPOSE", "FUEL", "CAPACITY", "OWN_WEIGHT",
               "TOTAL_WEIGHT", "N_REG_NEW")
for (col in setdiff(canonical, names(raw))) raw[[col]] <- NA_character_

new_data <- raw |>
  mutate(
    OPER_CODE    = as.integer(OPER_CODE),
    MAKE_YEAR    = as.integer(MAKE_YEAR),
    CAPACITY     = parse_double(CAPACITY,     locale = locale(decimal_mark = ",")),
    OWN_WEIGHT   = parse_double(OWN_WEIGHT,   locale = locale(decimal_mark = ",")),
    TOTAL_WEIGHT = parse_double(TOTAL_WEIGHT, locale = locale(decimal_mark = ","))
  ) |>
  select(all_of(canonical))

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
  if(new_year == current_max_year) {
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




