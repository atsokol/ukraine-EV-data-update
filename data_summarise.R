library(arrow)
library(lubridate)
library(readr)
library(dplyr)
library(stringr)

data <- open_dataset("raw-data/parquet")

# filter relevant codes
all_codes <- data %>% 
  group_by(OPER_CODE, OPER_NAME) %>% 
  count() %>% 
  arrange(OPER_CODE) %>% 
  collect()

primary_codes_table <- all_codes %>% 
  filter(str_detect(OPER_NAME, "ПЕРВИННА")) 

primary_codes <- primary_codes_table$OPER_CODE %>% unique()
primary_codes <- c(primary_codes, 70, 71, 72)

# BEV and PHEV codes
bev_codes <- c("ЕЛЕКТРО")
phev_codes <- c("БЕНЗИН, ГАЗ АБО ЕЛЕКТРО", "ЕЛЕКТРО АБО ДИЗЕЛЬНЕ ПАЛИВО", 
                "ЕЛЕКТРО АБО БЕНЗИН")
ev_codes <- c(bev_codes, phev_codes)

# Calculate total car registrations
cars <- data |> 
  filter(OPER_CODE %in% primary_codes,
         KIND == "ЛЕГКОВИЙ") |> 
  mutate(DATE = floor_date(D_REG, "month"),
         YEAR = year(D_REG),
         FUEL = case_when(
           FUEL %in% bev_codes ~ "BEV",
           FUEL %in% phev_codes ~ "PHEV",
           TRUE ~ "ICE")
  ) |> 
  mutate(AGE = YEAR - MAKE_YEAR) |> 
  group_by(DATE, BRAND, FUEL) |> 
  summarise(count = n()) |> 
  collect()  |> 
  mutate(BRAND = str_extract(BRAND, "\\S+")) |> 
  group_by(DATE, BRAND, FUEL) |> 
  summarise(count = sum(count)) |> 
  rename(date = DATE, brand = BRAND, fuel = FUEL)

write.csv(cars, "output-data/Ukraine_cars.csv")

cars |> 
  mutate(year = year(date)) |> 
  group_by(year) |> 
  summarise(count = sum(count))


ev_cars <- data |> 
  filter(OPER_CODE %in% primary_codes,
         FUEL %in% ev_codes,
         KIND == "ЛЕГКОВИЙ") |> 
  mutate(DATE = floor_date(D_REG, "month"),
         YEAR = year(D_REG),
         FUEL = case_when(
           FUEL %in% bev_codes ~ "BEV",
           FUEL %in% phev_codes ~ "PHEV",
           TRUE ~ "ICE")
  ) |> 
  mutate(AGE = YEAR - MAKE_YEAR) |> 
  collect() |> 
  mutate(BRAND = str_extract(BRAND, "\\S+")) |> 
  rename(date = DATE, brand = BRAND, fuel = FUEL) |> 
  select(brand, fuel, year = YEAR, age = AGE)

write.csv(ev_cars, "output-data/Ukraine_EV_cars.csv")