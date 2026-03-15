library(httr)
library(readr)
library(dplyr)

share_url  <- "https://alternative-fuels-observatory.ec.europa.eu/sites/default/files/csv/european-union-eu27/market_share_of_new_af_passenger_car_and_van_registrations_m1_n1.csv"
fleet_url  <- "https://alternative-fuels-observatory.ec.europa.eu/sites/default/files/csv/european-union-eu27/fleet_overview_of_af_passenger_cars_and_vans_m1_n2.csv"
fshare_url <- "https://alternative-fuels-observatory.ec.europa.eu/sites/default/files/csv/european-union-eu27/fleet_percentage_of_passenger_car_and_van_total_fleet_m1_n1.csv"

# Detect data_as_of date, year and months covered from Last-Modified header.
#
# AFO release convention:
#   - Jan–Jun modification  →  full previous year data  (12 months)
#   - Jul–Dec modification  →  running current year data (modified_month - 1 months)
detect_period <- function(url) {
  response   <- HEAD(url)
  lm_header  <- headers(response)[["last-modified"]]

  if (!is.null(lm_header)) {
    data_as_of <- as.Date(lm_header, format = "%a, %d %b %Y")
  } else {
    data_as_of <- Sys.Date()
  }

  mod_year  <- as.integer(format(data_as_of, "%Y"))
  mod_month <- as.integer(format(data_as_of, "%m"))

  if (mod_month <= 6) {
    # Early-year release: full previous year
    year   <- mod_year - 1L
    months <- 12L
  } else {
    # Mid/late-year update: running current year
    year   <- mod_year
    months <- mod_month - 1L
  }

  list(data_as_of = data_as_of, year = year)
}

period <- detect_period(share_url)
message(sprintf(
  "data_as_of: %s  |  year: %d",
  period$data_as_of, period$year
))

# Check whether this snapshot is already recorded — skip if so
out_path <- "output-data/EU_EV_data.csv"

if (file.exists(out_path)) {
  existing_dates <- read_csv(out_path, show_col_types = FALSE,
                             col_select = any_of("data_as_of")) |>
    pull(data_as_of) |>
    as.Date()
  if (period$data_as_of %in% existing_dates) {
    message("No new data: snapshot ", period$data_as_of, " already recorded. Skipping.")
    quit(save = "no", status = 0)
  }
  message("New data for year ", period$year,
          " (", period$data_as_of, "). Updating.")
}

# Download and prefix columns
share <- read_csv(share_url, show_col_types = FALSE) |>
  rename(country = Country) |>
  rename_with(~ paste0("EU_share_", .), -country)

fleet <- read_csv(fleet_url, show_col_types = FALSE) |>
  rename(country = Country) |>
  rename_with(~ paste0("EU_fleet_", .), -country)

fshare <- read_csv(fshare_url, show_col_types = FALSE) |>
  rename(country = Country) |>
  rename_with(~ paste0("EU_fleet_share_", .), -country)

# Join all 3 and add period columns
new_data <- share |>
  full_join(fleet,  by = "country") |>
  full_join(fshare, by = "country") |>
  mutate(
    year       = period$year,
    data_as_of = period$data_as_of
  ) |>
  select(year, data_as_of, country, everything())

# Load existing data and append new snapshot
if (file.exists(out_path)) {
  existing <- read_csv(out_path, show_col_types = FALSE)
  # Handle files written before data_as_of column was added
  if (!"data_as_of" %in% names(existing)) {
    existing <- mutate(existing, data_as_of = as.Date(NA))
  } else {
    existing <- mutate(existing, data_as_of = as.Date(data_as_of))
  }
  # Drop months column if present from older file format
  existing <- select(existing, -any_of("months"))
  # Replace rows for current year; keep all other years intact
  combined <- existing |>
    filter(year != period$year) |>
    bind_rows(new_data) |>
    arrange(year, data_as_of, country)
} else {
  combined <- new_data |> arrange(year, data_as_of, country)
}

write_csv(combined, out_path)
message(sprintf("Wrote %d rows (%d years) to %s",
  nrow(combined),
  n_distinct(combined$year),
  out_path))
