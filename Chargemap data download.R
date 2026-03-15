library(httr)
library(jsonlite)
library(tidyverse)

# Function to create grid from bounding box
create_grid_from_bbox <- function(bbox_params, zoom_level) {
  # Calculate tile size at zoom level (Mercator projection)
  lng_per_tile <- 360 / (2^zoom_level)
  
  # Calculate latitude tile size accounting for Mercator projection
  center_lat <- (bbox_params$NELat + bbox_params$SWLat) / 2
  lat_per_tile <- 360 / (2^zoom_level) * cos(center_lat * pi / 180)
  
  # Calculate number of tiles needed
  lng_range <- bbox_params$NELng - bbox_params$SWLng
  lat_range <- bbox_params$NELat - bbox_params$SWLat
  
  n_tiles_lng <- ceiling(lng_range / lng_per_tile)
  n_tiles_lat <- ceiling(lat_range / lat_per_tile)
  
  # Generate grid of bounding boxes
  grid_centers <- expand_grid(
    tile_lng = 0:(n_tiles_lng - 1),
    tile_lat = 0:(n_tiles_lat - 1)
  ) |>
    mutate(
      SWLng = bbox_params$SWLng + tile_lng * lng_per_tile,
      SWLat = bbox_params$SWLat + tile_lat * lat_per_tile,
      NELng = SWLng + lng_per_tile,
      NELat = SWLat + lat_per_tile,
      center_lng = (SWLng + NELng) / 2,
      center_lat = (SWLat + NELat) / 2
    ) |>
    select(center_lat, center_lng, SWLat, SWLng, NELat, NELng)
  
  return(grid_centers)
}

# Function to create grid from center coordinates
create_grid_from_center <- function(center_lat, center_lng, zoom_level) {
  # Calculate tile size at zoom level
  lng_per_tile <- 360 / (2^zoom_level)
  lat_per_tile <- 360 / (2^zoom_level) * cos(center_lat * pi / 180)
  
  # Calculate half tile size
  half_lng <- lng_per_tile / 2
  half_lat <- lat_per_tile / 2
  
  # Create single grid cell centered on the coordinates
  grid_centers <- tibble(
    center_lat = center_lat,
    center_lng = center_lng,
    SWLat = center_lat - half_lat,
    SWLng = center_lng - half_lng,
    NELat = center_lat + half_lat,
    NELng = center_lng + half_lng
  )
  
  return(grid_centers)
}


# Recursive function to fetch all charging stations
fetch_all_stations <- function(bbox_params, 
                              zoom_level, 
                              max_zoom = 13, 
                              country_id = 228,
                              level = 1,
                              is_bbox = TRUE,
                              existing_stations = NULL) {
  
  # Check zoom limit
  if (zoom_level > max_zoom) {
    cat("\nMax zoom level reached\n")
    return(tibble())
  }
  
  cat("\n=== LEVEL", level, "- Zoom", zoom_level, "===\n")
  
  # Create grid based on input type
  if (is_bbox) {
    grid <- create_grid_from_bbox(bbox_params, zoom_level)
    cat("Processing", nrow(grid), "grid cells\n")
  } else {
    # bbox_params is actually a data frame with lat/lng centers
    all_pools <- list()
    all_clusters <- list()
    
    for (i in 1:nrow(bbox_params)) {
      cat("Processing cluster", i, "of", nrow(bbox_params), "\n")
      
      grid <- create_grid_from_center(
        bbox_params$lat[i],
        bbox_params$lng[i],
        zoom_level
      )
      
      stations <- fetch_charging_stations(grid, verbose = FALSE)
      
      if (nrow(stations) > 0) {
        pools <- stations |> filter(type == "pool")
        clusters <- stations |> filter(type == "cluster")
        
        if (nrow(pools) > 0) {
          # Remove full duplicates from existing stations
          if (!is.null(existing_stations) && nrow(existing_stations) > 0) {
            pools <- anti_join(pools, existing_stations, by = names(pools))
          }
          
          if (nrow(pools) > 0) {
            all_pools[[i]] <- pools
            cat("  Found", nrow(pools), "pools\n")
          }
        }
        
        if (nrow(clusters) > 0) {
          all_clusters[[i]] <- clusters
          cat("  Found", nrow(clusters), "clusters\n")
        }
      }
      
      if (i < nrow(bbox_params)) {
        Sys.sleep(0.5)
      }
    }
    
    current_pools <- bind_rows(all_pools)
    remaining_clusters <- bind_rows(all_clusters)
    
    # Update existing stations with current pools
    if (nrow(current_pools) > 0) {
      if (is.null(existing_stations)) {
        existing_stations <- current_pools
      } else {
        existing_stations <- bind_rows(existing_stations, current_pools) |>
          distinct()
      }
    }
    
    cat("\nLevel", level, "summary: Pools =", nrow(current_pools), 
        ", Clusters =", nrow(remaining_clusters), "\n")
    
    # Recursively process remaining clusters
    if (nrow(remaining_clusters) > 0) {
      next_pools <- fetch_all_stations(
        remaining_clusters,
        zoom_level + 1,
        max_zoom,
        country_id,
        level + 1,
        is_bbox = FALSE,
        existing_stations = existing_stations
      )
      
      # Remove duplicates from next_pools before combining
      if (nrow(next_pools) > 0 && nrow(current_pools) > 0) {
        next_pools <- anti_join(next_pools, current_pools, by = names(next_pools))
      }
      
      current_pools <- bind_rows(current_pools, next_pools)
    }
    
    # Filter for country
    if (nrow(current_pools) > 0) {
      current_pools <- current_pools |> filter(pool$i18n_country_id == country_id)
    }
    
    return(current_pools)
  }
  
  # First pass: fetch stations from grid
  all_stations <- fetch_charging_stations(grid, verbose = TRUE)
  
  if (nrow(all_stations) == 0) {
    return(tibble())
  }
  
  # Separate pools and clusters
  pools <- all_stations |> 
    filter(type == "pool", pool$i18n_country_id == country_id)
  
  clusters <- all_stations |> filter(type == "cluster")
  
  # Initialize existing_stations with first level pools
  if (is.null(existing_stations) && nrow(pools) > 0) {
    existing_stations <- pools
  }
  
  cat("\nLevel", level, "summary: Pools =", nrow(pools), 
      ", Clusters =", nrow(clusters), "\n")
  
  # Recursively process clusters
  if (nrow(clusters) > 0) {
    next_pools <- fetch_all_stations(
      clusters,
      zoom_level + 1,
      max_zoom,
      country_id,
      level + 1,
      is_bbox = FALSE,
      existing_stations = existing_stations
    )
        
    pools <- bind_rows(pools, next_pools) |> distinct()
  }
  
  return(pools)
}


# Main execution
ukraine_bbox <- list(
  NELat = 52.3794,
  NELng = 40.2278,
  SWLat = 44.3864,
  SWLng = 22.1369
)

# Fetch all stations recursively
all_stations_new <- fetch_all_stations(
  bbox_params = ukraine_bbox,
  zoom_level = 8,
  max_zoom = 13,
  country_id = 228
)

# Save results
write_json(all_stations_new, "ukraine_charging_stations_complete.json", pretty = TRUE)

cat("\n\n=== FINAL RESULTS ===")
cat("\nTotal unique stations collected:", nrow(all_stations_new), "\n")