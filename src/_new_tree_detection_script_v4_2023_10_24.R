###_______________________________________###
### New TLS Based Tree Detection Pipeline ###
###_______________________________________###

### Notes: Private Property of Neal Swayze
### Drafted 10/15/2023
### Intention: Business draft of lidar pipeline and tree detection pipeline

###_________________________###
### Load required libraries ###
###_________________________###

library(lidR)
library(TreeLS)
library(terra)
library(raster)
library(sf)
library(sfarrow)
library(ini)
library(mapview)
library(parallel)
library(foreach)
library(doParallel)
library(ForestTools)
library(rFIA)
library(randomForest)
library(Metrics)
library(randomcoloR)
library(stringr)
library(dbscan)
library(dplyr)
library(geometry)
library(deldir)
library(plot3D)
library(rgl)

###____________________###
### Set root directory ###
###____________________###

rootdir = "D:/conifer_cloud_llc/02_projects/iron_sandbox_master/04_big_plot_testing"

###_________________________###
### MASTER SCRIPT FUNCTIONS ###
###_________________________###

### Function to generate nested project directories
create_project_structure = function(rootdir){
  
  ###_________________________###
  ### Set input las directory ###
  ###_________________________###
  input_las_dir = file.path(rootdir, "01_input_las")
  
  ###______________________________________________###
  ### Set output las directory and sub directories ###
  ###______________________________________________###
  output_dir = file.path(rootdir, "02_processed_data")
  
  ### Set output directory for storing lasgrid object
  las_grid_dir = file.path(output_dir, "00_grid_dir")
  
  ### Set output directory for las tiles
  las_tile_dir = file.path(output_dir, "01_tiled_las")
  
  ### Set output directory for las ground tiles
  las_ground_tile_dir = file.path(output_dir, "02_tiled_ground_las")
  
  ### Set output directory for normalized tiles
  las_norm_tile_dir = file.path(output_dir, "03_tiled_normalized_las")
  
  ### Set output directory for the las stem files
  las_stem_dir = file.path(output_dir, "04_las_stem_dir")
  
  ### Set output directory for stem polygon tiles
  stem_poly_tile_dir = file.path(output_dir, "05_stem_polygon_dir")
  
  ### Set output directory for storing intermediate spatial files
  working_spatial_dir = file.path(output_dir, "06_working_spatial_dir")
  
  ### Set output directory for cropped tree files 
  las_tree_dir = file.path(output_dir, "07_las_tree_dir")
  
  ###___________________________________________________###
  ### Set output delivery directory amd sub directories ###
  ###___________________________________________________###
  delivery_dir = file.path(rootdir, "03_delivery")
  
  ### Set output delivery directory for stats files
  delivery_stats_dir = file.path(delivery_dir, "01_processing_stats")
  
  ### Set output delivery directory for spatial files
  delivery_spatial_dir = file.path(delivery_dir, "02_spatial_files")
  
  ### Set output delivery directory for point cloud files
  delivery_las_dir = file.path(delivery_dir, "03_las_files")
  
  ### Create the directories
  dir.create(las_grid_dir, showWarnings = FALSE)
  dir.create(input_las_dir, showWarnings = FALSE)
  dir.create(output_dir, showWarnings = FALSE)
  dir.create(las_tile_dir, showWarnings = FALSE)
  dir.create(las_ground_tile_dir, showWarnings = FALSE)
  dir.create(las_norm_tile_dir, showWarnings = FALSE)
  dir.create(las_stem_dir, showWarnings = FALSE)
  dir.create(stem_poly_tile_dir, showWarnings = FALSE)
  dir.create(working_spatial_dir, showWarnings = FALSE)
  dir.create(las_tree_dir, showWarnings = FALSE)
  dir.create(delivery_dir, showWarnings = FALSE)
  dir.create(delivery_stats_dir, showWarnings = FALSE)
  dir.create(delivery_spatial_dir, showWarnings = FALSE)
  dir.create(delivery_las_dir, showWarnings = FALSE)
  
  ###______________________________###
  ### Set names of the directories ###
  ###______________________________###
  
  names(rootdir) = "rootdir"
  names(input_las_dir) = "input_las_dir"
  names(output_dir) = "output_dir"
  names(las_grid_dir) = "las_grid_dir"
  names(las_tile_dir) = "las_tile_dir"
  names(las_ground_tile_dir) = "las_ground_tile_dir"
  names(las_norm_tile_dir) = "las_norm_tile_dir"
  names(las_stem_dir) = "las_stem_dir"
  names(stem_poly_tile_dir) = "stem_poly_tile_dir"
  names(working_spatial_dir) = "working_spatial_dir"
  names(las_tree_dir) = "las_tree_dir"
  names(delivery_dir) = "delivery_dir"
  names(delivery_stats_dir) = "delivery_stats_dir"
  names(delivery_spatial_dir) = "delivery_spatial_dir"
  names(delivery_las_dir) = "delivery_las_dir"
  
  ###______________________________###
  ### Append to output config list ###
  ###______________________________###
  
  config = cbind(rootdir, input_las_dir, output_dir, las_grid_dir, las_tile_dir, las_ground_tile_dir,
                 las_norm_tile_dir, las_stem_dir, stem_poly_tile_dir, working_spatial_dir,
                 las_tree_dir,delivery_dir, delivery_stats_dir, delivery_spatial_dir, delivery_las_dir)
  
  config = as.data.frame(config)
  config
  
  ### Return config
  return(config)
  
}

### Function to read in available las data within input directory
read_las_as_ctg = function(config){
  
  ### Get the input directory read in 
  ctg = lidR::readTLSLAScatalog(config$input_las_dir)
  print(ctg)
  print(st_crs(ctg))
  return(ctg)
  
}

### Function to check input las and extent of desired tile resolution
generate_grid_over_extent = function(las_ctg, desired_tile_res){
  
  ### Pull the las extent geometry
  las_ctg_geom = sf::st_as_sf(las_ctg@data$geometry)
  
  ### Make a grid with the desired tile size
  grid = sf::st_make_grid(las_ctg_geom, desired_tile_res)
  grid = st_as_sf(grid)
  
  mapview(grid) + las_ctg_geom
  
  ### Create grid ID 
  grid_id = rep(1:nrow(grid))
  grid = cbind(grid, grid_id)
  
  message("Output grid has ", nrow(grid), " tiles ...")
  
  ###____________________________###
  ### Write the grid to the disk ###
  ###____________________________###
  
  setwd(config$las_grid_dir)
  suppressWarnings(sfarrow::st_write_parquet(grid, "project_grid.parquet"))
  
  return(grid)
  
}

### Function to generate .lax index files for input directory path
create_lax_for_tiles = function(desired_las_dir){
  
  ###__________________________________________###
  ### Create a lax index file for the las file ###
  ###__________________________________________###
  message("Initializing .lax indexing for desired directory ... ")
  las_list = list.files(desired_las_dir, pattern = ".las")
  laz_list = list.files(desired_las_dir, pattern = ".laz")
  lidar_list = append(las_list, laz_list)
  lidar_list
  
  message("Indexing ", length(lidar_list), " las files ... ")
  
  start_time = Sys.time()
  cores = detectCores()
  cluster = makeCluster(cores)
  registerDoParallel(cluster)
  foreach(i = 1:length(lidar_list)) %dopar% {
    
    ### Get the desired file
    des_file = lidar_list[i]
    des_file
    
    ### Compile the .lax file name
    des_file_lax = tools::file_path_sans_ext(des_file)
    des_file_lax = paste0(des_file_lax, ".lax")
    des_file_lax_path = paste0(desired_las_dir, "/", des_file_lax)
    des_file_lax_path
    
    ### See if the .lax version exists in the input directory
    does_file_exsist = file.exists(des_file_lax_path)
    does_file_exsist
    
    ### If file exsists, do nothing
    if(does_file_exsist == TRUE){return(NULL)}
    
    ### If file doesnt exsist, create a .lax index
    if(does_file_exsist == FALSE){
      
      ### Append the directory path to the las file
      path = paste0(desired_las_dir, "/", des_file)
      
      ### Write index
      rlas::writelax(path)
      
    }
    
  }
  stopCluster(cluster)
  end_time = Sys.time()
  total_time = difftime(end_time, start_time, units = c("mins"))
  message("Total lax index time took ", total_time, " minutes ... ")
  
}

### New function to tile the raw point cloud to the desired grid - with buffers 
new_tile_las_to_grid_foreach = function(config, desired_buffer_size){
  
  ###_____________________________###
  ### Read in the las grid object ###
  ###_____________________________###
  
  message(" --- Starting grid tiling algorithim --- ")
  master_start = Sys.time()
  
  message("Reading in project grid ... ")
  setwd(config$las_grid_dir)
  las_grid = sfarrow::st_read_parquet("project_grid.parquet")
  
  message("Reading in the las as a lascatalog for indexing ... ")
  las_ctg = lidR::readTLSLAScatalog(config$input_las_dir)
  las_ctg
  
  ###____________________________________________###
  ### Loop through and tile the input las object ###
  ###____________________________________________###
  
  start_time = Sys.time()
  cores = detectCores()
  message("Registering parallel session on ",  cores-2, " cores ... ")
  cluster = makeCluster(cores-2)
  registerDoParallel(cluster)
  message("Starting parallel grid tiling at ", start_time)
  message("Creating tiles for ", nrow(las_grid), " grids ... ")
  foreach(i=1:nrow(las_grid), .packages = c("lidR", "sf"), .inorder = FALSE, .errorhandling = "remove") %dopar% {
    
    ### Get the desired grid
    des_grid_cell = las_grid[i,]
    des_grid_cell
    
    ### Does file exist
    file_to_generate = paste0(config$las_tile_dir, "/", i, ".laz")
    does_file_exist = file.exists(file_to_generate)
    does_file_exist
    
    ### If file has been generated already, skip
    if(does_file_exist == TRUE){return(NULL)}
    
    ### If file has not been generated already, try to generate
    if(does_file_exist == FALSE){
      
      ### Buffer the grid cell to the desired buffer size
      des_grid_cell_buff = sf::st_buffer(des_grid_cell, desired_buffer_size)
      
      ### Crop the las file from the catalog for the grid cell extent  
      las = suppressWarnings(lidR::clip_roi(las_ctg, des_grid_cell_buff))
      #st_crs(las) = st_crs(las_grid)
      
      ### Check if the point cloud is empty
      is_cloud_empty = suppressWarnings(lidR::is.empty(las))
      is_cloud_empty
      
      ### If is_cloud_empty == TRUE, return NULL
      if(is_cloud_empty == TRUE){return(NULL)}
      
      ### If is_cloud_empty = FALSE, write to disk
      if(is_cloud_empty == FALSE){
        
        ### Write the cropped las to the disk
        setwd(config$las_tile_dir)
        out_name = paste0(des_grid_cell$grid_id, ".laz")
        suppressWarnings(lidR::writeLAS(las, out_name))
        return(NULL)
        
      }
      
    }
    
  }
  stopCluster(cluster)
  end_time = Sys.time()
  total_time = difftime(end_time, start_time, units = c("mins"))
  message("Total grid tiling time took ", total_time, " minutes ... ")
  
  ### Get some stats for the tiling run
  tiled_ctg = lidR::readTLSLAScatalog(config$las_tile_dir)
  tiled_ctg_geom = tiled_ctg$geometry
  tiled_ctg_geom = sf::st_as_sf(tiled_ctg_geom)
  las_area_m2 = sum(sf::st_area(tiled_ctg_geom))
  las_area_acres = las_area_m2/4047
  processing_time_mins = total_time
  number_starting_grids = nrow(las_grid)
  number_ending_grids = nrow(tiled_ctg_geom)
  
  ### Combine the output stats dataframe and write to the disk
  stats_output = cbind(number_starting_grids, number_ending_grids, las_area_m2, las_area_acres, processing_time_mins)
  stats_output = as.data.frame(stats_output)
  setwd(config$delivery_stats_dir)
  write.csv(stats_output, file = "01_grid_tiling_stats.csv")
  
  ### Get a list of tiles that processed
  processed_tile_list = list.files(config$las_tile_dir)
  message(length(processed_tile_list), " files were generated ... ")
  message(length(processed_tile_list)/nrow(las_grid) *100, " percent of tile grid occupied with lidar ... ")
  
  return(tiled_ctg_geom)
  
}

### Function to classify ground and height normalize las tiles
classify_ground_normalize = function(config, las_grid, want_to_classify_ground){
  
  ###_______________________###
  ### Configure directories ###
  ###_______________________###
  
  message(" --- Initializing ground classification and normalization algorithm --- ")
  master_start = Sys.time()
  
  ###_____________________________###
  ### Read in the las grid object ###
  ###_____________________________###
  
  message("Reading in project grid ... ")
  setwd(config$las_grid_dir)
  las_grid = sfarrow::st_read_parquet("project_grid.parquet")
  
  message("Reading in the las as a lascatalog for indexing ... ")
  las_ctg = lidR::readALSLAScatalog(config$input_las_dir)
  las_ctg
  
  ###________________________________________###
  ### Next, classify ground across the tiles ###
  ###________________________________________###
  
  ### Get a list of files to ground classify
  las_list = list.files(config$las_tile_dir, pattern = ".las")
  laz_list = list.files(config$las_tile_dir, pattern = ".laz")
  lidar_list = append(las_list, laz_list)
  message("Classifying ", length(lidar_list), " ground and height normalizing tiles in parallel ... ")
  lidar_list
  
  ###______________________________________________________________________________________###
  ### In parallel, classify ground, and height normalize across the tiles and rewrite them ###
  ###______________________________________________________________________________________###
  
  start_time = Sys.time()
  cores = detectCores()
  message("Registering parallel session on ", cores-1, " cores ... ")
  cluster = makeCluster(cores-1)
  registerDoParallel(cluster)
  message("Starting parallel classification/normalization ... ")
  foreach(i = 1:length(lidar_list), .packages = c("lidR", "sf"), .inorder = FALSE, .errorhandling = "remove") %dopar% {
    
    ### Get the desired lidar tile
    des_tile_name = lidar_list[i]
    des_tile_name
    
    ### Has the file been generated already?
    des_tile_to_check = paste0(config$las_norm_tile_dir, "/", des_tile_name)
    does_file_exsist = file.exists(des_tile_to_check)
    does_file_exsist
    
    ### If file exists, skip
    if(does_file_exsist == TRUE){return(NULL)}
    
    ### If file does not exsist, classify and height normalize
    if(does_file_exsist == FALSE){
      
      ### Get the matching grid polygon 
      las_grid_id = tools::file_path_sans_ext(des_tile_name)
      matching_grid_cell = las_grid[las_grid$grid_id == las_grid_id,]
      matching_grid_cell
      
      ### Read in the lidar tile
      setwd(config$las_tile_dir)
      las = lidR::readALSLAS(des_tile_name)
      #st_crs(las) = st_crs(las_grid)
      
      ### Drop duplicated points
      las = lidR::filter_duplicates(las)
      
      ### Set threads to 2 for speed up in height normalization
      lidR::set_lidr_threads(1)
      
      ### Classify ground points
      if(want_to_classify_ground == TRUE){las = lidR::classify_ground(las, algorithm = csf(rigidness = 1, sloop_smooth = TRUE))}
      
      ### Pull out the ground points
      ground = lidR::filter_poi(las, Classification == 2)
      
      ### Check if ground is empty, if not, write to disk
      ground_is_empty = lidR::is.empty(ground)
      ground_is_empty
      
      if(ground_is_empty == FALSE){
        setwd(config$las_ground_tile_dir)
        lidR::writeLAS(ground, file = des_tile_name)}
      
      ### Height normalize the file
      las = lidR::normalize_height(las, algorithm = knnidw())
      
      ### Remove points below 0.05
      las = lidR::filter_poi(las, Z > -0.05)
      
      ### Remove high outlier points
      height_filter = quantile(las@data$Z, 0.99999)
      las = lidR::filter_poi(las, Z < height_filter)
      
      ### Set crs
      #desired_crs = sf::st_crs(las_grid)
      #lidR::st_crs(las) = desired_crs
      #las
      
      ### Crop the las to the original grid polygon
      las = lidR::clip_roi(las, matching_grid_cell)
      
      ### Is las null?
      check = is.null(las)
      
      ### If las is null, return NULL
      if(check == TRUE){return(NULL)}
      
      ### Is the las file empty
      is_las_empty = lidR::is.empty(las)
      is_las_empty
      
      ### If las is empty, return Null
      if(is_las_empty == TRUE){return(NULL)}
      
      ### If las isnt empty, write the las to the disk
      if(is_las_empty == FALSE){
        
        ### Overwrite the existing file
        setwd(config$las_norm_tile_dir)
        lidR::writeLAS(las, file = des_tile_name)
        return(NULL)
        
      }
      
    }
    
  }
  stopCluster(cluster)
  end_time = Sys.time()
  total_time = difftime(end_time, start_time, units = c("mins"))
  message("Total ground classification and height normalization complete in ", total_time,  " minutes ... ")
  
  ###________________________________________###
  ### Get a catalog of the normalized points ###
  ###________________________________________###
  
  message("Reading in the normalized las file as a catalog ... ")
  ctg = lidR::readALSLAScatalog(config$las_norm_tile_dir)
  ctg_geom = ctg$geometry
  num_points_list = ctg$Number.of.point.records
  num_points_list = unlist(num_points_list)
  num_points_list = as.data.frame(num_points_list)
  number_points = sum(num_points_list$num_points_list)
  number_points
  
  ### Get the area of the dataset ina acres
  catalog_area = st_union(ctg@data$geometry)
  catalog_area = st_area(catalog_area)
  catalog_area = as.numeric(catalog_area)
  las_area_m2 = catalog_area
  las_area_acres = catalog_area/4047
  las_area_acres
  
  ### Get a list of processed files
  normalized_file_list = list.files(config$las_norm_tile_dir, pattern = ".laz")
  message(length(normalized_file_list), " lidar files height normalized ... ")
  difference = length(lidar_list) - length(normalized_file_list)
  message(difference, " files failed height normalization ...")
  
  ###_______________________________###
  ### Get the total processing time ###
  ###_______________________________###
  
  master_end = Sys.time()
  total_master_time = difftime(master_end, master_start, units = c("mins"))
  
  ###____________________________________________###
  ### Write some stats to the delivery directory ###
  ###____________________________________________###
  processing_time_mins = total_master_time
  stats_output = cbind(number_points, las_area_m2, las_area_acres, processing_time_mins)
  stats_output = as.data.frame(stats_output)
  setwd(config$delivery_stats_dir)
  write.csv(stats_output, file = "02_point_cloud_classification_normalization_stats.csv")
  message(" --- Total ground classification and height normalization took ", total_master_time, " minutes --- ")
  
  return(ctg_geom)
  
}

### Function to get DEM from normalized tiles
rasterize_tiles_to_dem = function(config, des_dem_res, calculate_extent){
  
  ###_______________________###
  ### Configure directories ###
  ###_______________________###
  
  message(" --- Initializing digitial elevation model rasterization --- ")
  master_start = Sys.time()
  
  ###_____________________________###
  ### Read in the las grid object ###
  ###_____________________________###
  
  message("Reading in project grid ... ")
  setwd(config$las_grid_dir)
  las_grid = sfarrow::st_read_parquet("project_grid.parquet")
  
  message("Reading in the las as a lascatalog for indexing ... ")
  las_ctg = lidR::readTLSLAScatalog(config$input_las_dir)
  las_ctg
  
  ###_______________________________________________________________________________###
  ### Get a list of classified ground normalized tiles to generate CHM raster files ###
  ###_______________________________________________________________________________###
  
  message("Getting a list of tiles to rasterize ... ")
  tile_list = list.files(config$las_ground_tile_dir, pattern = ".laz")
  tile_list
  
  ### Loop through and generate CHM files for each tile
  message("Processing ", length(tile_list), " las files in parallel ... ")
  start_time = Sys.time()
  cores = detectCores()-1
  cluster = makeCluster(cores)
  registerDoParallel(cluster)
  dem_point_list = foreach(i = 1:length(tile_list), .packages = c("lidR", "terra", "data.table", "sf")) %dopar% {
    
    ### Get the desired tile
    des_tile_name = tile_list[i]
    des_tile_name
    
    ### Read in the file
    setwd(config$las_ground_tile_dir)
    las = lidR::readTLSLAS(des_tile_name, select = "xyzc")
    
    ### Create a DEM from the file
    dem = lidR::rasterize_canopy(las, res = des_dem_res, algorithm = p2r())
    
    ### Get the DEM points
    pts = terra::crds(dem)
    pts = data.table::as.data.table(pts)
    
    ### Get the values
    z = terra::as.points(dem)
    z = sf::st_as_sf(z)
    z = sf::st_drop_geometry(z)
    z
    
    ### Combine the point values
    dem_points = cbind(pts, z)
    dem_points = data.table::as.data.table(dem_points)
    dem_points
    
    ### Return the points to the function 
    return(dem_points)
    
  }
  stopCluster(cluster)
  message("DEM las merge complete ... ")
  
  ### Merge the CHM points
  merged_dem_points = data.table::rbindlist(dem_point_list) 
  colnames(merged_dem_points) = c("X", "Y", "Z")
  merged_dem_points
  
  ###_____________________________________###
  ### Make a las file from the CHM points ###
  ###_____________________________________###
  
  message("Generating the master LiDAR file ... ")
  las = lidR::LAS(merged_dem_points)
  st_crs(las) = st_crs(las_grid)
  las
  
  ### Write cleaned las to the disk
  message("Writing the master lidar file to the disk ... ")
  setwd(config$delivery_las_dir)
  lidR::writeLAS(las, file = "master_dem_point_cloud.las")
  
  las_area_m2 = lidR::area(las)
  las_area_acres = las_area_m2/4047
  message("Total merged DEM area of ", las_area_acres, " acres .... ")
  
  ###____________________________________###
  ### Generate a CHM from the CHM points ###
  ###____________________________________###
  
  message("Generating the master DEM file ... ")
  master_dem = lidR::rasterize_canopy(las, res = des_dem_res, algorithm = p2r())
  master_dem
  
  ###_____________________________________###
  ### Write the master raster to the disk ###
  ###_____________________________________###
  
  message("Writing the master DEM file to the disk ... ")
  setwd(config$delivery_spatial_dir)
  outname = paste0("master_dem_raster_", des_dem_res, "m.tif")
  terra::writeRaster(master_dem, outname, overwrite = TRUE)
  
  ###_______________________________________###
  ### Get a polygon of the full file extent ###
  ###_______________________________________###
  
  if(calculate_extent == TRUE){
    
    message("Getting the extent of the DEM file ... ")
    extent_raster = terra::clamp(master_dem, 1, 1, 1)
    extent_poly = terra::as.polygons(extent_raster)
    extent_poly = terra::simplifyGeom(extent_poly)
    extent_poly = terra::fillHoles(extent_poly)
    extent_poly = sf::st_as_sf(extent_poly)
    extent_poly = st_union(extent_poly)
    extent_poly = st_as_sf(extent_poly)
    extent_poly
    
    ### Write the poly to the disk
    setwd(config$working_spatial_dir)
    #sf::st_write(extent_poly, dsn = "chm_extent_kml.kml", delete_dsn = TRUE, quiet = TRUE)
    sf::st_write(extent_poly, dsn = "dem_extent_geopackage.gpkg", delete_dsn = TRUE, quiet = TRUE)
    
  }
  
  ###____________________###
  ### Get the total time ###
  ###____________________###
  
  end_time = Sys.time()
  total_time = difftime(end_time, start_time, units = c("mins"))
  message(" --- Total lidar DEM rasterization took ", total_time, " minutes --- ")
  
}

### Function to generate a quick master canopy height model from normalized tiles
rasterize_tiles_to_chm = function(config, des_chm_res, max_height_threshold, calculate_extent){
  
  ###_______________________###
  ### Configure directories ###
  ###_______________________###
  
  message("Initializing canopy height model rasterization ... ")
  master_start = Sys.time()
  
  ###_____________________________###
  ### Read in the las grid object ###
  ###_____________________________###
  
  message("Reading in project grid ... ")
  setwd(config$las_grid_dir)
  las_grid = sfarrow::st_read_parquet("project_grid.parquet")
  
  message("Reading in the las as a lascatalog for indexing ... ")
  las_ctg = lidR::readTLSLAScatalog(config$input_las_dir)
  las_ctg
  
  ###_______________________________________________________________________________###
  ### Get a list of classified ground normalized tiles to generate CHM raster files ###
  ###_______________________________________________________________________________###
  
  message("Getting a list of tiles to rasterize ... ")
  tile_list = list.files(config$las_norm_tile_dir, pattern = ".laz")
  tile_list
  
  ### Loop through and generate CHM files for each tile
  start_time = Sys.time()
  cores = detectCores()-1
  message("Registering parallel session on ", cores, " cores ...")
  cluster = makeCluster(cores)
  registerDoParallel(cluster)
  message("Processing ", length(tile_list), " las files in parallel ... ")
  chm_point_list = foreach(i = 1:length(tile_list), .packages = c("lidR", "terra", "data.table", "sf")) %dopar% {
    
    ### Get the desired tile
    des_tile_name = tile_list[i]
    
    ### Read in the file
    setwd(config$las_norm_tile_dir)
    las = lidR::readTLSLAS(des_tile_name, select = "xyzc")
    
    ### Create a CHM from the file
    chm = lidR::rasterize_canopy(las, res = des_chm_res, algorithm = p2r())
    
    ### Get the CHM points
    pts = terra::crds(chm)
    pts = data.table::as.data.table(pts)
    
    ### Get the values
    z = terra::as.points(chm)
    z = sf::st_as_sf(z)
    z = sf::st_drop_geometry(z)
    z
    
    ### Combine the point values
    chm_points = cbind(pts, z)
    chm_points = data.table::as.data.table(chm_points)
    chm_points
    
    ### Return the points to the function 
    return(chm_points)
    
  }
  stopCluster(cluster)
  message("CHM lidar merge complete ... ")
  
  ### Merge the CHM points
  merged_chm_points = data.table::rbindlist(chm_point_list) 
  colnames(merged_chm_points) = c("X", "Y", "Z")
  
  ###_____________________________________###
  ### Make a las file from the CHM points ###
  ###_____________________________________###
  
  message("Generating the master LiDAR file ... ")
  des_crs = sf::st_crs(las_grid)
  las = lidR::LAS(merged_chm_points)
  st_crs(las) = des_crs
  las
  
  ### Drop noise points
  message("Dropping noise points above set threshold ... ")
  las = lidR::filter_poi(las, Z < max_height_threshold)
  
  ### Write cleaned las to the disk
  message("Writing the master lidar file to the disk ... ")
  setwd(config$delivery_las_dir)
  lidR::writeLAS(las, file = "master_chm_point_cloud.las")
  
  las_area_m2 = lidR::area(las)
  las_area_acres = las_area_m2/4047
  message("Total merged CHM area of ", las_area_acres, " acres .... ")
  
  ###____________________________________###
  ### Generate a CHM from the CHM points ###
  ###____________________________________###
  
  message("Generating the master CHM file ... ")
  master_chm = lidR::rasterize_canopy(las, res = des_chm_res, algorithm = p2r())
  master_chm
  
  ###_____________________________________###
  ### Write the master raster to the disk ###
  ###_____________________________________###
  
  message("Writing the master CHM file to the disk ... ")
  setwd(config$delivery_spatial_dir)
  outname = paste0("master_chm_raster_", des_chm_res, "m.tif")
  terra::writeRaster(master_chm, outname, overwrite = TRUE)
  
  ###_______________________________________###
  ### Get a polygon of the full file extent ###
  ###_______________________________________###
  
  if(calculate_extent == TRUE){
    
    message("Getting the extent of the CHM file ... ")
    extent_raster = terra::clamp(master_chm, 1, 1, 1)
    extent_poly = terra::as.polygons(extent_raster)
    extent_poly = terra::simplifyGeom(extent_poly)
    extent_poly = terra::fillHoles(extent_poly)
    extent_poly = sf::st_as_sf(extent_poly)
    extent_poly = st_union(extent_poly)
    extent_poly = st_as_sf(extent_poly)
    extent_poly
    
    ### Write the poly to the disk
    setwd(config$working_spatial_dir)
    #sf::st_write(extent_poly, dsn = "chm_extent_kml.kml", delete_dsn = TRUE, quiet = TRUE)
    sf::st_write(extent_poly, dsn = "chm_extent_geopackage.gpkg", delete_dsn = TRUE, quiet = TRUE)
    
  }
  
  ###____________________###
  ### Get the total time ###
  ###____________________###
  
  end_time = Sys.time()
  total_time = difftime(end_time, start_time, units = c("mins"))
  message(" --- Total lidar rasterization took ", total_time, " minutes --- ")
  
}

### Function to generate a quick master stem canopy height model from normalized tiles
rasterize_tiles_to_stem_chm = function(config, des_stem_chm_res, min_z, max_z, calculate_extent){
  
  ###_______________________###
  ### Configure directories ###
  ###_______________________###
  
  message("Initializing stem canopy height model rasterization ... ")
  master_start = Sys.time()
  
  ###_____________________________###
  ### Read in the las grid object ###
  ###_____________________________###
  
  message("Reading in project grid ... ")
  setwd(config$las_grid_dir)
  las_grid = sfarrow::st_read_parquet("project_grid.parquet")
  
  message("Reading in the las as a lascatalog for indexing ... ")
  las_ctg = lidR::readTLSLAScatalog(config$input_las_dir)
  las_ctg
  
  ###_______________________________________________________________________________###
  ### Get a list of classified ground normalized tiles to generate CHM raster files ###
  ###_______________________________________________________________________________###
  
  message("Getting a list of tiles to rasterize ... ")
  tile_list = list.files(config$las_norm_tile_dir, pattern = ".laz")
  
  ### Loop through and generate CHM files for each tile
  stem_start = Sys.time()
  message("Processing ", length(tile_list), " las files in parallel ... ")
  start_time = Sys.time()
  cores = detectCores()-1
  cluster = makeCluster(cores)
  registerDoParallel(cluster)
  chm_point_list = foreach(i = 1:length(tile_list), .packages = c("lidR", "terra", "data.table", "sf")) %dopar% {
    
    ### Get the desired tile
    des_tile_name = tile_list[i]
    
    ### Read in the file
    setwd(config$las_norm_tile_dir)
    las = lidR::readTLSLAS(des_tile_name, select = "xyzc")
    
    ### Filter the points to within the min and max Z range 
    las = lidR::filter_poi(las, Z > min_z)
    las = lidR::filter_poi(las, Z < max_z)
    
    ### Get a logic check 
    is_las_empty = lidR::is.empty(las)
    is_las_empty
    
    ### If the las file is empty, return NULL
    if(is_las_empty == TRUE){return(NULL)}
    
    ### If the las file isnt empty, make a stem CHM
    if(is_las_empty == FALSE){
      
      ### Create a CHM from the file
      chm = lidR::rasterize_canopy(las, res = des_stem_chm_res, algorithm = p2r())
      
      ### Pull the X, Y, and Z columns
      df = cbind(las@data$X, las@data$Y, las@data$Z)
      df = data.table::as.data.table(df)
      
      ### Return the points to the function 
      return(df)
      
    }
    
  }
  stopCluster(cluster)
  stem_end = Sys.time()
  stem_total = difftime(stem_end, stem_start, units = c("mins"))
  message("Total stem CHM extraction took ", stem_total, " minutes ... ")
  
  ### Merge the CHM points
  message("Merging the stem points into a master las file ... ")
  las = data.table::rbindlist(chm_point_list) 
  colnames(las) = c("X", "Y", "Z")
  las = lidR::LAS(las)
  st_crs(las) = st_crs(las_grid)
  
  ### Write cleaned las to the disk
  message("Writing the master lidar file to the disk ... ")
  setwd(config$delivery_las_dir)
  lidR::writeLAS(las, file = "master_stem_chm_point_cloud.las")
  
  las_area_m2 = lidR::area(las)
  las_area_acres = las_area_m2/4047
  message("Total merged stem CHM area of ", las_area_acres, " acres .... ")
  
  ###____________________________________###
  ### Generate a CHM from the CHM points ###
  ###____________________________________###
  
  message("Generating the master stem CHM file ... ")
  master_chm = lidR::rasterize_canopy(las, res = des_stem_chm_res, algorithm = p2r())
  
  ###_____________________________________###
  ### Write the master raster to the disk ###
  ###_____________________________________###
  
  message("Writing the master stem CHM file to the disk ... ")
  setwd(config$delivery_spatial_dir)
  outname = paste0("master_stem_chm_raster_", des_stem_chm_res, "m.tif")
  terra::writeRaster(master_chm, outname, overwrite = TRUE)
  
  ###_______________________________________###
  ### Get a polygon of the full file extent ###
  ###_______________________________________###
  
  if(calculate_extent == TRUE){
    
    message("Getting the extent of the stem CHM file ... ")
    extent_raster = terra::clamp(master_chm, 1, 1, 1)
    extent_poly = terra::as.polygons(extent_raster)
    #extent_poly = terra::simplifyGeom(extent_poly)
    #extent_poly = terra::fillHoles(extent_poly)
    extent_poly = sf::st_as_sf(extent_poly)
    extent_poly = st_union(extent_poly)
    extent_poly = st_as_sf(extent_poly)
    extent_poly
    
    ### Write the poly to the disk
    setwd(config$working_spatial_dir)
    #sf::st_write(extent_poly, dsn = "chm_extent_kml.kml", delete_dsn = TRUE, quiet = TRUE)
    sf::st_write(extent_poly, dsn = "stem_chm_extent_geopackage.gpkg", delete_dsn = TRUE, quiet = TRUE)
    
  }
  
  
  ###____________________###
  ### Get the total time ###
  ###____________________###
  
  end_time = Sys.time()
  total_time = difftime(end_time, start_time, units = c("mins"))
  print(paste("Total lidar rasterization took ", total_time, " minutes ... "))
  
}

### Function to detect stems within normalized tiles
detect_stems_within_tiles = function(config, dbh_max_size_m){
  
  ###_______________________###
  ### Configure directories ###
  ###_______________________###
  
  message("Initializing tree detection algorithm ... ")
  master_start = Sys.time()
  
  ###_____________________________###
  ### Read in the las grid object ###
  ###_____________________________###
  
  message("Reading in project grid ... ")
  setwd(config$las_grid_dir)
  las_grid = sfarrow::st_read_parquet("project_grid.parquet")
  
  message("Reading in the las as a lascatalog for indexing ... ")
  las_ctg = lidR::readALSLAScatalog(config$input_las_dir)
  las_ctg
  
  ####____________________________________###
  ### Get a list of .las files to process ###
  ###_____________________________________###
  
  laz_list = list.files(config$las_norm_tile_dir, pattern = ".laz")
  las_list = list.files(config$las_norm_tile_dir, pattern = ".las")
  lidar_list = append(laz_list, las_list)
  message("Processing ", length(lidar_list), " las files in parallel ... ")
  head(lidar_list)
  
  ###____________________________________________________________###
  ### Get the extent of the las directory as a lascatalog object ###
  ###____________________________________________________________###
  
  las_extent = lidR::readALSLAScatalog(config$las_norm_tile_dir)
  las_extent = las_extent@data
  las_extent
  
  ### Pull some important stats for the las extent
  las_extent = subset(las_extent, select = c(filename, Number.of.point.records))
  las_name = basename(las_extent$filename)
  extent_area = sf::st_area(las_extent)
  extent_area = as.numeric(extent_area)
  number_points = las_extent$Number.of.point.records
  las_extent = cbind(las_extent, las_name, extent_area, number_points)
  mapview(las_extent)
  
  ###____________________________________________________###
  ### Loop through the las files and try to detect stems ###
  ###____________________________________________________###
  
  stem_start = Sys.time()
  cores = detectCores()
  message("Registering parallel session on ", cores-1, " cores ... ")
  cluster = makeCluster(cores-1)
  registerDoParallel(cluster)
  stem_detection_status_grid_list = foreach(i = 1:length(lidar_list), 
                                            .packages = c("TreeLS", "sf", "lidR", "ForestTools", "terra", "dplyr", "raster"),
                                            .errorhandling = "remove",
                                            .inorder = FALSE) %dopar% {
                                              
                                              ### Get the desired las file
                                              las_name = lidar_list[i]
                                              las_name
                                              
                                              ### See if the file has been generated
                                              path_to_check = paste0(config$las_stem_dir, "/", las_name)
                                              does_file_exist = file.exists(path_to_check)
                                              does_file_exist
                                              
                                              if(does_file_exist == TRUE){return(NULL)}
                                              
                                              ### Read in the desired las file
                                              las_path = paste0(config$las_norm_tile_dir, "/", las_name)
                                              las = lidR::readTLSLAS(las_path)
                                              las = lidR::filter_duplicates(las)
                                              st_crs(las) = st_crs(las_grid)
                                              
                                              ### Get the matching polygon extent from the las extent
                                              extent = las_extent[las_extent$filename %in% las_path,]
                                              extent
                                              
                                              ### Get the area of the las file
                                              las_area_m2 = lidR::area(las)
                                              las_area_m2
                                              
                                              ### Check the maximum point height
                                              max_point_height = max(las@data$Z)
                                              max_point_height
                                              
                                              ### Get the extent of the las file
                                              extent = cbind(extent, las_area_m2, max_point_height)
                                              extent
                                              
                                              ###____________________________________________________________###
                                              ### If the max point height is below X feet, return empty tile ###
                                              ###____________________________________________________________###
                                              
                                              if(max_point_height < 2){
                                                
                                                ### Compile extent polygon statistics for failed file
                                                stem_detection_status = "no points below 2m"
                                                number_stems_detected = "0"
                                                max_stem_dbh = "0"
                                                mean_stem_dbh = "0"
                                                
                                                ### Append the extent polygon stats to the polygon file
                                                extent = cbind(extent, stem_detection_status, number_stems_detected, max_stem_dbh, mean_stem_dbh)
                                                return(extent)
                                                
                                              }
                                              
                                              ###______________________________________________________________###
                                              ### If the max point height is above X feet, try to detect stems ###
                                              ###______________________________________________________________###
                                              
                                              if(max_point_height > 2){
                                                
                                                ###_____________________________________________________###
                                                ### Define function to map for potential tree locations ###
                                                ###_____________________________________________________###
                                                
                                                ### Function to map for potential tree locations
                                                tree_map_function <- function(las){
                                                  result <- tryCatch(
                                                    expr = {
                                                      
                                                      map = TreeLS::treeMap(las, map.hough(min_h = 1,
                                                                                           max_h = 5,
                                                                                           h_step = 0.5,
                                                                                           pixel_size = 0.025,
                                                                                           max_d = 0.6,
                                                                                           min_density = 0.0001,
                                                                                           min_votes = 3), 0)
                                                    },
                                                    error = function(e) {
                                                      message <- paste("Error:", e$message)
                                                      return(message)
                                                    }
                                                  )
                                                  
                                                  if (inherits(result, "error")) {
                                                    return(result)
                                                  } else {
                                                    return(result)
                                                  }
                                                }
                                                
                                                ### Run the function to search for candidate locations
                                                result = tree_map_function(las)
                                                
                                                ### Get a logic check
                                                check = class(result)
                                                check
                                                
                                                ###_______________________________________________________________###
                                                ### If the class of the result === Character, then no stems found ###
                                                ###_______________________________________________________________###
                                                
                                                if(check == "character"){
                                                  
                                                  ### Compile extent polygon statistics for failed file
                                                  stem_detection_status = "no stems found"
                                                  number_stems_detected = 0
                                                  max_stem_dbh = 0
                                                  mean_stem_dbh = 0
                                                  
                                                  ### Append the extent polygon stats to the polygon file
                                                  extent = cbind(extent, stem_detection_status, number_stems_detected, max_stem_dbh, mean_stem_dbh)
                                                  return(extent)
                                                  
                                                }
                                                
                                                ### If the class of the result == "LAS"
                                                if(check == "LAS"){
                                                  
                                                  ###___________________________________###
                                                  ### Classify the tree and stem points ###
                                                  ###___________________________________###
                                                  
                                                  ### Merge stems
                                                  result = TreeLS::treeMap.merge(result)
                                                  
                                                  ### Classify tree regions
                                                  las = TreeLS::treePoints(las, result, trp.crop(l = 3))
                                                  
                                                  ### Classify stem points
                                                  las = TreeLS::stemPoints(las, stm.hough(h_step = 0.5,
                                                                                          max_d = 0.6,
                                                                                          h_base = c(1, 2.5),
                                                                                          pixel_size = 0.025,
                                                                                          min_density = 0.1,
                                                                                          min_votes = 3))
                                                  
                                                  ###_______________________________________________________###
                                                  ### Set the classification codes of different point types ###
                                                  ###_______________________________________________________###
                                                  
                                                  ### Pull out the stem files
                                                  stem_points = lidR::filter_poi(las, Stem == TRUE)
                                                  stem_points@data$Classification = 4
                                                  
                                                  ### Pull out the ground points
                                                  ground = filter_poi(las, Classification == 2)
                                                  
                                                  ### Pull out the remaining points that arent ground
                                                  remaining_points = filter_poi(las, Stem == FALSE)
                                                  remaining_points@data$Classification = 5
                                                  
                                                  ### Combine the newly classified data
                                                  las_reclassified = rbind(stem_points, ground, remaining_points)
                                                  
                                                  ### Write the stem points to the disk
                                                  setwd(config$las_stem_dir)
                                                  lidR::writeLAS(las_reclassified, las_name)
                                                  
                                                  ###_______________________________________________________###
                                                  ### GET THE STEM CYLINDER MEASUREMENTS AND STEM INVENTORY ###
                                                  ###_______________________________________________________###
                                                  
                                                  ### Search through tree points and estimate DBH
                                                  tree_inv = TreeLS::tlsInventory(las, dh = 1.37, dw = 0.2, d_method = shapeFit(shape = "circle", algorithm = "ransac", n = 20)) 
                                                  tree_inv$Radius = as.numeric(tree_inv$Radius)
                                                  tree_inv$dbh_m = tree_inv$Radius*2
                                                  tree_inv$dbh_cm = tree_inv$dbh_m*100
                                                  tree_inv$basal_area_m2 = pi * (tree_inv$Radius)^2
                                                  tree_inv$basal_area_ft2 = tree_inv$basal_area_m2 * 10.764
                                                  tree_inv
                                                  
                                                  ###_____________________________###
                                                  ### Create spatial file outputs ###
                                                  ###_____________________________###
                                                  
                                                  ### Create Spatial file of stem inventory
                                                  des_crs = sf::st_crs(las_grid)
                                                  tree_inv_sf = sf::st_as_sf(tree_inv, coords = c("X", "Y"), crs = des_crs)
                                                  inv_coords = sf::st_coordinates(tree_inv_sf)
                                                  tree_inv_sf = cbind(tree_inv_sf, inv_coords)
                                                  
                                                  ### Make TreeIDs unique
                                                  tree_ID = paste0(tree_inv_sf$X, "_", tree_inv_sf$Y)
                                                  tree_inv_sf = cbind(tree_inv_sf, tree_ID)
                                                  
                                                  ### Drop unwanted columns
                                                  tree_inv_sf = subset(tree_inv_sf, select = -c(TreeID))
                                                  tree_inv_sf = subset(tree_inv_sf, select = c(tree_ID, H, X, Y, Radius, Error, dbh_m, dbh_cm, basal_area_m2, basal_area_ft2))
                                                  
                                                  ### Rename the columns to be a bit more helpful
                                                  names(tree_inv_sf) = c("treeID", "tree_height_m", "stem_x", "stem_y", 
                                                                         "radius_m", "radius_error_m", "dbh_m", "dbh_cm", 
                                                                         "basal_area_m2", "basal_area_ft2", "geometry")
                                                  
                                                  ### Remove points outside the extent polygon
                                                  extent_buffer = sf::st_buffer(extent, 1)
                                                  tree_inv_sf = sf::st_crop(tree_inv_sf, extent_buffer)
                                                  
                                                  ### Write stem polygons to the disk
                                                  setwd(config$stem_poly_tile_dir)
                                                  out_name = tools::file_path_sans_ext(las_name)
                                                  out_name = paste0(out_name, ".parquet")
                                                  sfarrow::st_write_parquet(tree_inv_sf, out_name)
                                                  
                                                  ###__________________________________________________###
                                                  ### Return the extent with values for tree detection ###
                                                  ###__________________________________________________###
                                                  
                                                  ### Compile extent polygon statistics for failed file
                                                  stem_detection_status = "stems detected"
                                                  number_stems_detected = nrow(tree_inv_sf)
                                                  max_stem_dbh = max(tree_inv_sf$dbh_cm)
                                                  mean_stem_dbh = mean(tree_inv_sf$dbh_cm)
                                                  
                                                  ### Append the extent polygon stats to the polygon file
                                                  extent = cbind(extent, stem_detection_status, number_stems_detected, max_stem_dbh, mean_stem_dbh)
                                                  extent
                                                  return(extent)}
                                                
                                              }
                                              
                                            }
  stopCluster(cluster)
  stem_end = Sys.time()
  stem_total = difftime(stem_end, stem_start, units = c("mins"))
  message("Stem detection complete, took ", stem_total, " minutes ... ")
  
  ###______________________________________________________###
  ### Compose a status grid for the stem detection attempt ###
  ###______________________________________________________###
  
  ### Merge the list into an SF object
  stem_detection_status_grid = sf::st_as_sf(data.table::rbindlist(stem_detection_status_grid_list))
  grid_with_status = subset(stem_detection_status_grid, select = c(stem_detection_status))
  grid_with_count = subset(stem_detection_status_grid, select = c(number_stems_detected))
  mapview(grid_with_count)
  
  ###________________________________###
  ### Get the grid tiles that failed ###
  ###________________________________###
  
  ### Pull in the full grid using the las extent, then grab any missing files from the stem detection
  grid_cells_that_failed = las_extent[!(las_extent$filename %in% stem_detection_status_grid$filename),]
  grid_cells_that_failed
  
  ### Append the missing information to the grid cells that failed
  las_area_m2 = rep("NA", nrow(grid_cells_that_failed))
  max_point_height = rep("NA", nrow(grid_cells_that_failed))
  stem_detection_status = rep("failed to process", nrow(grid_cells_that_failed))
  number_stems_detected = rep("NA", nrow(grid_cells_that_failed))
  max_stem_dbh = rep("NA", nrow(grid_cells_that_failed))
  mean_stem_dbh = rep("NA", nrow(grid_cells_that_failed))
  
  ### Append the empty fill data to the remaining grid
  grid_cells_that_failed = cbind(grid_cells_that_failed, las_area_m2, max_point_height, stem_detection_status, 
                                 number_stems_detected, max_stem_dbh, mean_stem_dbh)
  
  ###________________________________________________###
  ### Combine the status grid and the remaining grid ###
  ###________________________________________________###
  
  final_status_grid = rbind(stem_detection_status_grid, grid_cells_that_failed)
  test = subset(final_status_grid, select = c(stem_detection_status))
  mapview(test)
  
  num_files_failed = nrow(grid_cells_that_failed)
  message(num_files_failed, " files failed to process ...")
  
  ### Write the status grid to the delivery folder
  setwd(config$working_spatial_dir)
  sf::st_write(final_status_grid, dsn = "bottom_up_stem_detection_status_grid.gpkg", append = FALSE, quiet = TRUE)
  
  ###___________________________________________________###
  ### Merge the stem las point files into a single file ###
  ###___________________________________________________###
  
  ### Get a list of stem point files to process
  message("Merging the stem points into a single file ... ")
  stem_point_list = list.files(config$las_stem_dir, pattern = ".laz$")
  stem_point_list
  
  ### Loop through the las files and pull the points in df format
  start_time = Sys.time()
  cores = detectCores()
  cluster = makeCluster(cores)
  registerDoParallel(cluster)
  stem_points_list = foreach(i=1:length(stem_point_list),
                             .errorhandling = "remove",
                             .inorder = FALSE, 
                             .packages = c("data.table", "lidR")) %dopar% {
                               
                               ### Get the desired las name
                               des_las_name = stem_point_list[i]
                               
                               ### Read in the stem points
                               setwd(config$las_stem_dir)
                               des_las = lidR::readTLSLAS(des_las_name)
                               des_las
                               
                               ### Grab the dataframe
                               df = des_las@data
                               
                               ### Pull the stem only points
                               stem_point_df = df[df$Classification == 4,]
                               stem_point_df
                               
                               ### Return the dataframe
                               return(stem_point_df)
                               
                             }
  stopCluster(cluster)
  
  ### Bind the list into a single object
  merged_stem_points = rbindlist(stem_points_list)
  merged_stem_points
  
  ### Points to LAS
  stem_points_las = lidR::LAS(merged_stem_points)
  st_crs(stem_points_las) = st_crs(las_grid)
  
  ### Write the las file to the disk
  setwd(config$delivery_las_dir)
  lidR::writeLAS(stem_points_las, "detected_stem_points.las")
  end_time = Sys.time()
  total_time = difftime(end_time, start_time, units = c("mins"))
  message("Total stem merging took ", total_time, " minutes ... ")
  
  ###__________________________________________________________###
  ### Merge the stem point location tiles into a single object ###
  ###__________________________________________________________###
  
  ### Get a list of stem point locations to merge
  message("Merging the stem point locations in parallel ... ")
  setwd(config$stem_poly_tile_dir)
  poly_list = list.files(config$stem_poly_tile_dir, pattern = ".parquet")
  poly_list
  
  ### Loop through in parallel and read in the stem point location files
  merge_start = Sys.time()
  cores = detectCores()
  cluster = makeCluster(cores)
  registerDoParallel(cluster)
  merged_poly_list = foreach(i = 1:length(poly_list)) %dopar% {
    
    ### Get the desired polygon
    des_poly_name = poly_list[i]
    
    ### Read it in
    setwd(config$stem_poly_tile_dir)
    des_poly = sfarrow::st_read_parquet(des_poly_name)
    des_poly = sf::st_make_valid(des_poly)
    ### Return to foreach
    return(des_poly)
    
  }
  stopCluster(cluster)
  
  ### Merge the stem points to a single file
  dbh_locations_sf = st_as_sf(rbindlist(merged_poly_list))
  st_crs(dbh_locations_sf) = st_crs(las_grid)
  merge_end = Sys.time()
  merge_total = difftime(merge_end, merge_start, units = c("mins"))
  message("Tree stem point location merge complete, took ", merge_total, " minutes ...")
  
  ###________________________________________________###
  ### CLEAN UP THE STEM POLYGONS WITH SOME FILTERING ###
  ###________________________________________________###
  
  ### Get the NA condition
  is_na = is.na(dbh_locations_sf$radius_m)
  dbh_locations_sf = cbind(dbh_locations_sf, is_na)
  dbh_locations_sf
  
  ### Drop stems with NA values
  dbh_locations_sf = dbh_locations_sf[dbh_locations_sf$is_na == "FALSE",]
  dbh_locations_sf = subset(dbh_locations_sf, select = -c(is_na))
  dbh_locations_sf
  
  ### Drop DBHs above set threshold 
  dbh_locations_sf = dbh_locations_sf[dbh_locations_sf$dbh_m < dbh_max_size_m,]
  dbh_locations_sf
  
  ### Remove empty geometry stem locations
  condition = sf::st_is_empty(dbh_locations_sf)
  dbh_locations_sf = cbind(dbh_locations_sf, condition)
  dbh_locations_sf = dbh_locations_sf[dbh_locations_sf$condition == FALSE,]
  dbh_locations_sf
  
  ### Reset the condition
  dbh_locations_sf$condition = rep("detected_stem", nrow(dbh_locations_sf))
  dbh_locations_sf
  
  ###K Buffer the points by the radius of the DBH
  dbh_locations_sf_buffer = sf::st_buffer(dbh_locations_sf, dist = dbh_locations_sf$radius_m)
  
  ###___________________________________________________________###
  ### Write the DBHs and the merged canopy polygons to the disk ###
  ###___________________________________________________________###
  print(paste0("Writing the merged canopies and the detected stems to the disk ... "))
  setwd(config$working_spatial_dir)
  sf:::st_write(dbh_locations_sf, dsn = "bottom_up_detected_stem_locations.gpkg", append = FALSE, delete_dsn = TRUE, quiet = TRUE)
  sf:::st_write(dbh_locations_sf_buffer, dsn = "bottom_up_detected_stem_locations_buffered.gpkg", append = FALSE, delete_dsn = TRUE, quiet = TRUE)
  
  ### Get the total processing time
  master_end = Sys.time()
  master_total = difftime(master_end, master_start, units = c("mins"))
  print(paste0("Total DBH detection took ", master_total, " minutes ... "))
  
  ### Compile output statistics 
  number_trees_found = nrow(dbh_locations_sf)
  number_tiles_processed = nrow(stem_detection_status_grid)
  number_tiles_failed = num_files_failed
  master_dbh_detection_mins = master_total
  output_stats = cbind(number_tiles_processed, number_tiles_failed, number_trees_found, master_dbh_detection_mins)
  
  ### Write output statistics to the disk
  setwd(config$delivery_stats_dir)
  write.csv(output_stats, file = "03_dbh_detection_stats.csv")
  print(paste0("Total stem detection complete ... "))
  
}

###__________________________###
### FUNCTIONS IN DEVELOPMENT ###
###__________________________###

### New function to detect crowns top down, independent from main pipeline
detect_crowns_top_down = function(config, desired_chm_res, window_size, minimum_height, min_crown_size_m2){
  
  ### Read in the chm
  setwd(config$delivery_spatial_dir)
  desired_chm_name = paste0("master_chm_raster_", desired_chm_res, "m.tif")
  message("Reading in ", desired_chm_name, " ... ")
  input_file_path = paste0(config$delivery_spatial_dir, "/", desired_chm_name)
  input_chm = terra::rast(input_file_path)
  input_chm
  
  ### Get tree tops using liDR tools
  start_time = Sys.time()
  message("Detecting tree tops through local maxima detection ... ")
  input_seeds = lidR::locate_trees(input_chm, algorithm = lmf(ws = window_size, hmin = minimum_height))
  input_seeds = sf::st_zm(input_seeds)
  input_seeds
  
  ### Use the tree tops to delineate crowns using MCWS from ForestTools
  message("Delineating crowns from detected local maxima ... ")
  input_chm_raster = raster::raster(input_chm)
  crowns = ForestTools::mcws(input_seeds, input_chm_raster, minHeight = minimum_height, format = "raster", verbose = FALSE)
  crowns
  
  ### Get the treeID fixed for the input seeds
  message("Cleaning up delineated crowns ... ")
  input_seeds_coords = as.data.frame(sf::st_coordinates(input_seeds))
  treeID = paste0(round(input_seeds_coords$X, 1), "_", round(input_seeds_coords$Y, 1))
  input_seeds = subset(input_seeds, select = -c(treeID))
  input_seeds = cbind(input_seeds, treeID)
  input_seeds
  
  ### Convert crown raster to terra rast, then polygons, then to Sf
  crowns = terra::rast(crowns)
  crowns = terra::as.polygons(crowns)
  crowns = terra::makeValid(crowns)
  crowns = terra::simplifyGeom(crowns)
  crowns = terra::fillHoles(crowns)
  crowns = sf::st_as_sf(crowns)
  crowns = sf::st_buffer(crowns, 0)
  
  ### Get the crown area, then remove super small crowns
  message("Getting crown area of polygons ... ")
  crown_area_m2 = as.numeric(sf::st_area(crowns))
  crowns = cbind(crowns, crown_area_m2)
  crowns = crowns[crowns$crown_area_m2 > min_crown_size_m2,]
  crowns
  
  ### Join the crowns with the seeds to append data, remove Nulls
  message("Joining crowns with input tree top seeds ... ")
  crowns = sf::st_join(crowns, input_seeds)
  crowns = subset(crowns, select = -c(layer))
  crowns = sf::st_difference(crowns)
  crowns = subset(crowns, select = c(treeID, Z, crown_area_m2))
  colnames(crowns) = c("treeID", "tree_height_m", "crown_area_m2", "geometry") 
  crowns
  
  ### Add height summaries
  message("Extracting height summaries for each crown ... ")
  mean_crown_ht_m = exactextractr::exact_extract(input_chm, crowns, fun = "mean")
  median_crown_ht_m = exactextractr::exact_extract(input_chm, crowns, fun = "median")
  min_crown_ht_m = exactextractr::exact_extract(input_chm, crowns, fun = "min")
  crowns = cbind(crowns, mean_crown_ht_m, median_crown_ht_m, min_crown_ht_m)
  
  ### Write the crowns to the disk
  message("Writing the crowns to the disk ... ")
  setwd(config$working_spatial_dir)
  sf::st_write(crowns, "top_down_detected_crowns.gpkg", quiet = TRUE, append = FALSE)
  
  ### Get the end time
  end_time = Sys.time()
  total_time = difftime(end_time, start_time, units = c("mins"))
  message("Total top-down crown delineation took ", total_time, " minutes ... ")
  
  
}

### New function to detect crowns bottom up, independent from main pipeline
detect_crowns_bottom_up = function(config, desired_chm_res, window_size, minimum_height, min_crown_size_m2){
  
  ### Read in the detected stem locations
  start_time = Sys.time()
  
  ### Read in the chm
  setwd(config$delivery_spatial_dir)
  desired_chm_name = paste0("master_chm_raster_", desired_chm_res, "m.tif")
  message("Reading in ", desired_chm_name, " ... ")
  input_file_path = paste0(config$delivery_spatial_dir, "/", desired_chm_name)
  input_chm = terra::rast(input_file_path)
  input_chm
  
  ## Read in the stems
  message("Reading in the detected stem location points ... ")
  path_to_stem_locations = paste0(config$working_spatial_dir, "/bottom_up_detected_stem_locations.gpkg")
  stem_locations = sf::st_read(path_to_stem_locations, quiet = TRUE)
  stem_locations
  
  ### Create Seeds object for delineation
  message("Preparing stem locations for crown delineation ... ")
  input_seeds = subset(stem_locations, select = c(tree_height_m))
  colnames(input_seeds) = c("Z", "geometry")
  treeID = rep(1:nrow(input_seeds))
  input_seeds = cbind(treeID, input_seeds)
  input_seeds
  
  ### Use the tree tops to delineate crowns using MCWS from ForestTools
  message("Detecting crowns from stem location seeds ... ")
  input_chm_raster = raster::raster(input_chm)
  crowns = ForestTools::mcws(input_seeds, input_chm_raster, minHeight = minimum_height, format = "raster")
  crowns
  
  ### Get the treeID fixed for the input seeds
  message("Cleaning up the delineated crowns ... ")
  input_seeds_coords = as.data.frame(sf::st_coordinates(input_seeds))
  treeID = paste0(round(input_seeds_coords$X, 1), "_", round(input_seeds_coords$Y, 1))
  input_seeds = subset(input_seeds, select = -c(treeID))
  input_seeds = cbind(input_seeds, treeID)
  input_seeds
  
  ### Convert crown raster to terra rast, then polygons, then to Sf
  crowns = terra::rast(crowns)
  crowns = terra::as.polygons(crowns)
  crowns = terra::simplifyGeom(crowns)
  crowns = terra::fillHoles(crowns)
  crowns = sf::st_as_sf(crowns)
  crowns = sf::st_buffer(crowns, 0)
  #crowns = sf::st_difference(crowns)
  
  ### Get the crown area, then remove super small crowns
  message("Getting crown area of polygons ... ")
  crown_area_m2 = as.numeric(sf::st_area(crowns))
  crowns = cbind(crowns, crown_area_m2)
  crowns = crowns[crowns$crown_area_m2 > min_crown_size_m2,]
  
  ### Join the crowns with the seeds to append data, remove Nulls
  message("Joining crowns with input tree top seeds ... ")
  stem_locations = subset(stem_locations, select = -c(treeID))
  crowns = sf::st_join(crowns, stem_locations)
  crowns = sf::st_difference(crowns)
  crowns = subset(crowns, select = -c(layer))
  
  ### Add height summaries
  message("Extracting height summaries for each crown ... ")
  mean_crown_ht_m = exactextractr::exact_extract(input_chm, crowns, fun = "mean")
  median_crown_ht_m = exactextractr::exact_extract(input_chm, crowns, fun = "median")
  min_crown_ht_m = exactextractr::exact_extract(input_chm, crowns, fun = "min")
  crowns = cbind(crowns, mean_crown_ht_m, median_crown_ht_m, min_crown_ht_m)
  
  ### Write the stem crowns to the disk
  message("Writing the crowns to the disk ... ")
  setwd(config$working_spatial_dir)
  sf::st_write(crowns, "bottom_up_detected_crowns.gpkg", quiet = TRUE, append = FALSE)
  
  ### Get the total time
  end_time = Sys.time()
  total_time = difftime(end_time, start_time, units = c("mins"))
  message("Total bottom-up crown delineation took ", total_time, " minutes ... ")
  
  
}

### Detect trees using bottom up seeds and top down seeds 
wrapper_hybrid_crown_delineation_missing_dbh_prediction = function(config){
  
  ###_________________________________________###
  ### Detect tree tops using top down methods ###
  ###_________________________________________###
  
  master_start_time = Sys.time()
  setwd(config$working_spatial_dir)
  message("Reading in the top down detected crowns ... ")
  top_down_crowns = sf::st_read("top_down_detected_crowns.gpkg", quiet = TRUE)
  
  ###__________________________________________###
  ### Detect tree tops using bottom up methods ###
  ###__________________________________________###
  
  setwd(config$working_spatial_dir)
  message("Reading in the bottom up detected crowns ... ")
  bottom_up_crowns = sf::st_read("bottom_up_detected_crowns.gpkg", quiet = TRUE)
  
  ### Read in the stem locations 
  setwd(config$working_spatial_dir)
  message("Reading in the stem locations ... ")
  stem_locations = sf::st_read("bottom_up_detected_stem_locations.gpkg", quiet = TRUE)
  
  ###________________________________________________________###
  ### Join the Top down crowns with the stem location points ###
  ###________________________________________________________###
  
  ### Join the top down crowns with the stem location points
  print(paste0("Combining top down crowns with stem locations ... "))
  top_down_crowns_joined_stem_locations = sf::st_join(top_down_crowns, stem_locations)
  
  ### Get the top down crowns without stems
  top_down_crowns_no_stems = top_down_crowns_joined_stem_locations[is.na(top_down_crowns_joined_stem_locations$radius_m),]
  top_down_crowns_no_stems
  
  ###_________________________________________________###
  ### Generate Local Model of DBH - crown area/height ###
  ###_________________________________________________###
  
  ### Prepare training dataset for RF modeling
  print(paste0("Preparing to model missing stem locations ... "))
  bottom_up_crowns_no_geom = sf::st_drop_geometry(bottom_up_crowns)
  training_dataset = subset(bottom_up_crowns_no_geom, select = c(radius_m, crown_area_m2, tree_height_m, 
                                                                 mean_crown_ht_m, median_crown_ht_m, min_crown_ht_m))
  
  ### Run a randomForest model to predict DBH using various crown predictors
  print(paste0("Running random forest model to predict radius_m for crowns ... "))
  stem_prediction_model = randomForest::randomForest(radius_m ~ ., data = training_dataset)
  stem_prediction_model
  plot(stem_prediction_model$predicted ~ training_dataset$radius_m)
  rmse = Metrics::rmse(training_dataset$radius_m, stem_prediction_model$predicted)
  mae = Metrics::mae(training_dataset$radius_m, stem_prediction_model$predicted)
  print(paste0("Radius Model has a root mean square error of ", rmse*100, " cm ... "))
  print(paste0("Radius Model has a mean absoulte error of ", mae*100, " cm ... "))
  
  ###___________________________________________________________________###
  ### Predict missing radius values for the top down crowns with no DBH ###
  ###___________________________________________________________________###
  
  ### Predict the missing radius values 
  print(paste0("Predicting missing radius values for top down crowns with no stems ... "))
  top_down_crowns_no_stems_no_geom = sf::st_drop_geometry(top_down_crowns_no_stems)
  prediction_dataset = subset(top_down_crowns_no_stems_no_geom, select = c(crown_area_m2, tree_height_m.x, mean_crown_ht_m,median_crown_ht_m, min_crown_ht_m))
  colnames(prediction_dataset) = c("crown_area_m2", "tree_height_m", "mean_crown_ht_m", "median_crown_ht_m", "min_crown_ht_m")
  radius_m = predict(stem_prediction_model, prediction_dataset)
  radius_m
  
  ###_____________________________________________________________###
  ### Engineer missing features for top down crowns with no stems ###
  ###_____________________________________________________________###
  
  ### Replace the new radius values and derivatives in place of NULL data
  print(paste0("Generating additional features for top down crowns with no stems ... "))
  radius_error_m = "NA"
  dbh_m = radius_m*2
  dbh_cm = dbh_m*100
  basal_area_m2 = pi * (radius_m)^2
  basal_area_ft2 = basal_area_m2 * 10.764
  
  ### Replace the missing data for 
  top_down_crowns_no_stems$radius_m = radius_m
  top_down_crowns_no_stems$radius_error_m = radius_error_m
  top_down_crowns_no_stems$dbh_m = dbh_m
  top_down_crowns_no_stems$dbh_cm = dbh_cm
  top_down_crowns_no_stems$basal_area_m2 = basal_area_m2
  top_down_crowns_no_stems$basal_area_ft2 = basal_area_ft2
  top_down_crowns_no_stems_centroids = sf::st_centroid(top_down_crowns_no_stems)
  top_down_crowns_no_stems$condition = rep("predicted_stem", nrow(top_down_crowns_no_stems_centroids))
  
  ###__________________________________________________________________###
  ### Create fake stem locations for the top down crowns with no stems ###
  ###__________________________________________________________________###
  
  print(paste0("Generating predicted stem locations ... "))
  top_down_crowns_no_stems_predicted_stem_coords = data.frame(sf::st_coordinates(top_down_crowns_no_stems_centroids))
  treeID = paste0(round(top_down_crowns_no_stems_predicted_stem_coords$X,1), "_", round(top_down_crowns_no_stems_predicted_stem_coords$Y,1))
  condition = rep("predicted_stem", nrow(top_down_crowns_no_stems_centroids))
  predicted_stem_locations = subset(top_down_crowns_no_stems_centroids, select = c(tree_height_m.x, radius_m,radius_error_m, dbh_m, dbh_cm, basal_area_m2, basal_area_ft2))
  predicted_stem_locations = cbind(treeID, predicted_stem_locations, condition)
  predicted_stem_locations = sf::st_as_sf(predicted_stem_locations)
  predicted_stem_locations
  
  ### Write the predicted stem locations to the disk
  setwd(config$working_spatial_dir)
  sf::st_write(predicted_stem_locations, dsn = "predicted_stem_locations.gpkg", append = FALSE, quiet = TRUE)
  predicted_stem_locations = sf::st_read("predicted_stem_locations.gpkg", quiet = TRUE)
  predicted_stem_locations
  
  ###___________________________________________________________________________________________________________###
  ### Re-run the bottom up crown delineation using the predicted stem locations and the detected stem locations ###
  ###___________________________________________________________________________________________________________###
  
  ### Combine the predicted and detected stem locations
  stem_locations = subset(stem_locations, select = -c(stem_x, stem_y))
  colnames(predicted_stem_locations) = c("treeID", "tree_height_m", "radius_m", "radius_error_m", "dbh_m", "dbh_cm", "basal_area_m2", "basal_area_ft2", "condition", "geom")
  combined_stem_locations = rbind(stem_locations, predicted_stem_locations)
  
  ### Buffer the combined stem locations by Radius_m
  combined_stem_locations_buff = sf::st_buffer(combined_stem_locations, dist = combined_stem_locations$radius_m)
  
  ###_________________________###
  ### Write files to the disk ###
  ###_________________________###
  
  print(paste0("Writing spatial files to the disk ... "))
  setwd(config$delivery_spatial_dir)
  sf::st_write(combined_stem_locations_buff, "final_stem_locations_dbh.gpkg", append = FALSE, quiet = TRUE)
  sf::st_write(combined_stem_locations, "final_stem_locations.gpkg", append = FALSE, quiet = TRUE)
  
  ### Get the total function time
  master_end_time = Sys.time()
  master_total_time = difftime(master_end_time, master_start_time, units = c("mins"))
  print(paste0("Total stem seed hybridization complete in ", master_total_time, " minutes ..."))
  
}

### New function to detect crowns bottom up, independent from main pipeline
detect_final_crowns = function(config, desired_chm_res, window_size, minimum_height, min_crown_size_m2){
  
  ### Read in the detected stem locations
  start_time = Sys.time()
  
  ### Read in the chm
  setwd(config$delivery_spatial_dir)
  desired_chm_name = paste0("master_chm_raster_", desired_chm_res, "m.tif")
  message("Reading in ", desired_chm_name, " ... ")
  input_file_path = paste0(config$delivery_spatial_dir, "/", desired_chm_name)
  input_chm = terra::rast(input_file_path)
  input_chm
  
  ## Read in the stems
  message("Reading in the final stem location points ... ")
  setwd(config$delivery_spatial_dir)
  stem_locations = sf::st_read("final_stem_locations.gpkg", quiet = TRUE)
  combined_stem_locations = sf::st_read("final_stem_locations.gpkg", quiet = TRUE)
  
  ### Create Seeds object for delineation
  message("Preparing stem locations for crown delineation ... ")
  input_seeds = subset(stem_locations, select = c(tree_height_m))
  colnames(input_seeds) = c("Z", "geometry")
  treeID = rep(1:nrow(input_seeds))
  input_seeds = cbind(treeID, input_seeds)
  input_seeds
  
  ### Use the tree tops to delineate crowns using MCWS from ForestTools
  message("Detecting crowns from stem location seeds ... ")
  input_chm_raster = raster::raster(input_chm)
  crowns = ForestTools::mcws(input_seeds, input_chm_raster, minHeight = minimum_height, format = "raster")
  crowns
  
  ### Get the treeID fixed for the input seeds
  message("Cleaning up the delineated crowns ... ")
  input_seeds_coords = as.data.frame(sf::st_coordinates(input_seeds))
  treeID = paste0(round(input_seeds_coords$X, 1), "_", round(input_seeds_coords$Y, 1))
  input_seeds = subset(input_seeds, select = -c(treeID))
  input_seeds = cbind(input_seeds, treeID)
  input_seeds
  
  ### Convert crown raster to terra rast, then polygons, then to Sf
  crowns = terra::rast(crowns)
  crowns = terra::as.polygons(crowns)
  crowns = terra::simplifyGeom(crowns)
  crowns = terra::fillHoles(crowns)
  crowns = sf::st_as_sf(crowns)
  crowns = sf::st_buffer(crowns, 0)
  #crowns = sf::st_difference(crowns)
  
  ### Get the crown area, then remove super small crowns
  message("Getting crown area of polygons ... ")
  crown_area_m2 = as.numeric(sf::st_area(crowns))
  crowns = cbind(crowns, crown_area_m2)
  crowns = crowns[crowns$crown_area_m2 > min_crown_size_m2,]
  
  ### Join the crowns with the seeds to append data, remove Nulls
  message("Joining crowns with input tree top seeds ... ")
  stem_locations = subset(stem_locations, select = -c(treeID))
  crowns = sf::st_join(crowns, stem_locations)
  crowns = sf::st_difference(crowns)
  crowns = subset(crowns, select = -c(layer))
  
  ### Add height summaries
  message("Extracting height summaries for each crown ... ")
  mean_crown_ht_m = exactextractr::exact_extract(input_chm, crowns, fun = "mean")
  median_crown_ht_m = exactextractr::exact_extract(input_chm, crowns, fun = "median")
  min_crown_ht_m = exactextractr::exact_extract(input_chm, crowns, fun = "min")
  crowns = cbind(crowns, mean_crown_ht_m, median_crown_ht_m, min_crown_ht_m)
  
  ### Append the tree ID from the stem locations to the crowns
  message("Cleaning up final crowns ... ")
  treeID = subset(combined_stem_locations, select = c(treeID))
  crowns = sf::st_join(crowns, treeID)
  crowns = sf::st_difference(crowns)
  
  ### Write the stem crowns to the disk
  message("Writing the final crowns to the disk ... ")
  setwd(config$delivery_spatial_dir)
  sf::st_write(crowns, "final_detected_crowns.gpkg", quiet = TRUE, append = FALSE)
  
  ### Get the total time
  end_time = Sys.time()
  total_time = difftime(end_time, start_time, units = c("mins"))
  message("Total final crown delineation took ", total_time, " minutes ... ")
  
  
}

### New function to colorize las files based on input type
colorize_input_las = function(input_las, desired_color){
  
  color_ramp_rgb = paste(as.vector(col2rgb(desired_color)), collapse = " ")
  color_ramp_rgb = stringr::str_split(color_ramp_rgb, pattern = " ")  
  color_ramp_rgb = as.data.frame(color_ramp_rgb)  
  color_ramp_rgb = t(color_ramp_rgb)
  rownames(color_ramp_rgb) = NULL
  color_ramp_rgb = as.data.frame(color_ramp_rgb)
  colnames(color_ramp_rgb) = c("R", "G", "B")
  R = rep(as.integer(color_ramp_rgb$R), nrow(input_las@data))    
  G = rep(as.integer(color_ramp_rgb$G), nrow(input_las@data))  
  B = rep(as.integer(color_ramp_rgb$B), nrow(input_las@data)) 
  
  ### Use the lidR functions to colorize the point cloud
  input_las = lidR::add_lasrgb(input_las, R = R, G = G, B = B)   
  return(input_las)
  
}

### New function to crop each crown to each canopy polygon, colorize, write to disk
crop_crowns_by_id_and_colorize_stems_and_veg = function(config, las_grid, colorize_input_las, stem_color_hex, veg_color_hex, ground_color_hex, use_bottom_up_crowns){
  
  stem_color_hex = "#8B4513"
  veg_color_hex = "#228B22"
  ground_color_hex = "#EEE8AA"
  
  ###___________________________###
  ### Read in the desired files ###
  ###___________________________###
  
  ### Read in the master crown file
  print(paste0("Initializing tree crown crop and colorization algorithm ... "))
  
  ### If use bottom up crowns == FALSE, use combined crowns
  if(use_bottom_up_crowns == FALSE){  
    
    setwd(config$delivery_spatial_dir)
    master_crowns = sf::st_read("final_detected_crowns.gpkg", quiet = TRUE)
    master_crowns
    
  }
  
  ### If use bottom up crowns == TRUE, use bottom up crowns
  if(use_bottom_up_crowns == TRUE){  
    
    setwd(config$working_spatial_dir)
    master_crowns = sf::st_read("bottom_up_crowns.gpkg", quiet = TRUE)
    master_crowns
    
  }
  
  
  
  ### Read in the classified las files as a catalog
  las_stem_ctg = lidR::readTLSLAScatalog(config$las_stem_dir)
  las_stem_ctg
  
  ###_____________________________###
  ### Initialize parallel session ###
  ###_____________________________###
  
  print(paste0("Processing ", nrow(master_crowns), " crown files ... "))
  start_time = Sys.time()
  cores = detectCores()
  print(paste0("Starting parallel cluster on ", cores-1, " cores ... "))
  cluster = makeCluster(cores-1)
  registerDoParallel(cluster)
  foreach(i=1:nrow(master_crowns), .packages = c("lidR"), .errorhandling = "remove", .inorder = FALSE) %dopar% {
    
    ### Get the desired crown file
    des_crown = master_crowns[i,]
    des_crown
    
    ### Has the crown already been cropped/colored?
    file_to_check = paste0(config$las_tree_dir, "/", i, ".laz")
    does_file_exsist = file.exists(file_to_check)
    does_file_exsist
    
    ### If file exsists, skip
    if(does_file_exsist == TRUE){return(NULL)}
    
    ### If file doesnt exsist, crop the file
    if(does_file_exsist == FALSE){
      
      ### Crop the crown file from the las catalog
      cropped_crown = lidR::clip_roi(las_stem_ctg, des_crown)
      
      ### Drop points low to the ground
      cropped_crown = lidR::filter_poi(cropped_crown, Z > 0.1)
      
      ### Pull the veg and stems from the cropped crown
      veg = lidR::filter_poi(cropped_crown, Classification == 5)
      stem = lidR::filter_poi(cropped_crown, Classification == 4)
      
      ### Get some checks
      veg_empty = lidR::is.empty(veg)
      stem_empty = lidR::is.empty(stem)
      
      ###______________________________________________________________________###
      ### If stems and crowns are not empty, color them then write to the disk ###
      ###______________________________________________________________________###
      
      if(veg_empty == FALSE && stem_empty == FALSE){
        
        ### Get random color for the veg
        des_veg_color = randomcoloR::randomColor(count = 1, hue = c("green"), luminosity = c("dark"))
        
        ### Colorize the veg points
        veg = colorize_input_las(veg, desired_color = des_veg_color)
        
        ### Get random colors for the stems
        des_stem_color = randomcoloR::randomColor(count = 1, hue = c("orange"), luminosity = c("dark"))
        
        ### Colorize the stem points
        stem = colorize_input_las(stem, desired_color = des_stem_color)
        
        ### Combine the colored stems and veg
        combined = rbind(veg, stem)
        
        ### Write the colored crown to the disk
        setwd(config$las_tree_dir)
        out_name = paste0(des_crown$treeID,".laz")
        lidR::writeLAS(combined, out_name)
        return(NULL)
        
      }
      
      ###___________________________________________________________________________________###
      ### If the veg isnt empty, and the stems are empty, color the veg, then write to disk ###
      ###___________________________________________________________________________________###
      
      if(veg_empty == FALSE && stem_empty == TRUE){
        
        ### Get random color for the veg
        des_veg_color = randomcoloR::randomColor(count = 1, hue = c("green"), luminosity = c("dark"))
        
        ### Colorize the veg points
        veg = colorize_input_las(veg, desired_color = des_veg_color)
        
        ### Write the colored crown to the disk
        setwd(config$las_tree_dir)
        out_name = paste0(des_crown$treeID,".laz")
        lidR::writeLAS(veg, out_name)
        return(NULL)
        
      }
      
    }
    
  }
  stopCluster(cluster)
  end_time = Sys.time()
  total_crop_time = difftime(end_time, start_time, units = c("mins"))
  print(paste0("Total crop/colorization took ", total_crop_time, " minutes ... "))
  
  ###___________________________________________###
  ### Loop through the cropped crowns and merge ###
  ###___________________________________________###
  
  las_list = list.files(config$las_tree_dir, pattern = ".las")
  laz_list = list.files(config$las_tree_dir, pattern = ".laz")
  lidar_list = append(las_list, laz_list)
  lidar_list
  
  print(paste0("Merging ", length(lidar_list), " tree las files ... "))
  start_time = Sys.time()
  cores = detectCores()
  print(paste0("Starting parallel cluster on ", cores-1, " cores ... "))
  cluster = makeCluster(cores-1)
  registerDoParallel(cluster)
  merged_data_las = foreach(i=1:length(lidar_list)) %dopar% {
    
    setwd(config$las_tree_dir)
    des_file = lidar_list[i]
    las = lidR::readTLSLAS(des_file)
    data = las@data
    return(data)
    
  }
  stopCluster(cluster)
  
  merged_data = data.table::rbindlist(merged_data_las)
  las = lidR::LAS(merged_data)
  st_crs(las) = st_crs(las_stem_ctg)
  las
  
  ### Write the las file to the disk
  setwd(config$delivery_las_dir)
  lidR::writeLAS(las, "master_classified_trees.las")
  
}

### Function to calculate basal area from final stem dataset
get_final_tree_stats = function(config){
  
  message(" --- Starting final tree stats pipeline --- ")
  master_start = Sys.time()
  setwd(config$delivery_spatial_dir)
  
  ### Read in the final stem dataset
  message("Reading in the final stem locations ... ")
  final_stems = sf::st_read("final_stem_locations.gpkg", quiet = TRUE)
  final_stems
  
  ### Read in the final canopy dataset
  message("Reading in the final crown dataset ... ")
  final_crowns = sf::st_read("final_detected_crowns.gpkg", quiet = TRUE)
  final_crowns
  
  ### Read in the summary stats from height normalization
  setwd(config$delivery_stats_dir)
  normalization_stats = read.csv("02_point_cloud_classification_normalization_stats.csv")
  head(normalization_stats)
  
  ###______________________________________________###
  ### Get the basal area for the processed dataset ###
  ###______________________________________________###
  
  ### Convert the stem measurement diameter to inches
  message("Getting basal area statistics ... ")
  dbh_inches = final_stems$dbh_cm/2.54
  
  ### Get basal area for each tree
  basal_area_square_ft = (dbh_inches^2) * 0.005454
  
  ### Get the sum of basal area
  sum_basal_area_square_ft = sum(basal_area_square_ft)
  sum_basal_area_square_ft
  
  ### Get the sum of basal area in acres
  sum_basal_area_acres = sum_basal_area_square_ft/43560
  sum_basal_area_acres
  
  ###___________________________________###
  ### Get the total tree count and area ###
  ###___________________________________###
  
  message("Getting tree statistics ... ")
  total_tree_count = nrow(final_crowns)
  total_tree_count
  
  ### Get the sum crown area
  sum_crown_area_m2 = sum(final_crowns$crown_area_m2)
  sum_crown_area_m2  
  
  ### Convert to square feet
  sum_crown_area_ft2 = sum_crown_area_m2*10.764
  sum_crown_area_ft2
  
  ### Convert to acres
  sum_crown_area_acres = sum_crown_area_m2/4047
  sum_crown_area_acres
  
  ### Get the max tree height
  max_tree_height = max(final_crowns$tree_height_m, na.rm = TRUE)
  max_tree_height
  
  ### Get the median tree height
  median_tree_height = median(final_crowns$tree_height_m, na.rm = TRUE)
  median_tree_height
  
  ### Get the mean tree height
  mean_tree_height = mean(final_crowns$tree_height_m, na.rm = TRUE)
  mean_tree_height
  
  ###________________________###
  ### Calculate tree density ###
  ###________________________###
  
  message("Calculating tree density per acre ... ")
  las_area_acres = normalization_stats$las_area_acres
  trees_per_acre = total_tree_count/las_area_acres
  trees_per_acre
  
  ###_______________________________###
  ### Compile final stats dataframe ###
  ###_______________________________###
  
  message("Compiling master stats dataframe ... ")
  final_stats_dataframe = cbind(total_tree_count, trees_per_acre, max_tree_height, median_tree_height, mean_tree_height,
                                sum_crown_area_ft2, sum_crown_area_acres, sum_basal_area_square_ft, sum_basal_area_acres)
  
  final_stats_dataframe = data.frame(final_stats_dataframe)
  final_stats_dataframe
  
  ###___________________________________###
  ### Write the final stats to the disk ###
  ###___________________________________###
  
  message("Writing the final stats dataframe to the disk ... ")
  setwd(config$delivery_spatial_dir)
  write.csv(final_stats_dataframe, "final_tree_detection_stats.csv")
  
  ###________________###
  ### Wrap things up ###
  ###________________###
  
  master_end = Sys.time()
  master_total = difftime(master_end, master_start, units = c("mins"))
  message(" --- Final tree stats pipeline complete in ", master_total, " minutes ---")
  return(final_stats_dataframe)
  
}

###_______________________________###
### Configure project directories ###
###_______________________________###

### Set up the project directory structure
total_start = Sys.time()
config = create_project_structure(rootdir)

### Read in the las files, check the number of points and CRS
las_ctg = read_las_as_ctg(config)
las_ctg
mapview(las_ctg@data$geometry)

###__________________________________________________________###
### Set parameters for tiling, buffering, and tree detection ###
###__________________________________________________________###

### Set the extent of the tile grid
desired_tile_res = 50

### Set the desired buffer size
desired_buffer_size = 10

### Set the window size for top down tree detection
window_size = 3.5

### Set the minimum height for crown delineation
minimum_height = 1

### Set the minimum crown size (m^2) to be retained after delineation
min_crown_size_m2 = 3

### Set the maximum dbh size (meters)
dbh_max_size_m = 1.5

###_______________________________________________________###
### Run the function to generate a tile grid for cropping ###
###_______________________________________________________###

### Generate the grid overlay on top of the las catalog geometry
las_grid = generate_grid_over_extent(las_ctg, desired_tile_res)
las_grid_buff = sf::st_buffer(las_grid, desired_buffer_size)
mapview(las_grid_buff) + las_ctg@data$geometry

###_______________________________###
### Tile the original point cloud ###
###_______________________________###

create_lax_for_tiles(config$input_las_dir)
tiled_grid = new_tile_las_to_grid_foreach(config, desired_buffer_size)
mapview(tiled_grid)
create_lax_for_tiles(config$las_tile_dir)

###______________________________________________________###
### Next, classify ground and height normalize each tile ###
###______________________________________________________###

height_norm_grid = classify_ground_normalize(config, las_grid, want_to_classify_ground = TRUE)
mapview(height_norm_grid)
create_lax_for_tiles(config$las_ground_tile_dir)
create_lax_for_tiles(config$las_norm_tile_dir)

###___________________________________________###
### Rasterize the tiled las to get a DEM file ###
###___________________________________________###

rasterize_tiles_to_dem(config, des_dem_res = 3, calculate_extent = FALSE)

###_____________________________________________###
### Rasterize the normalized tiles to CHM tiles ###
###_____________________________________________###

rasterize_tiles_to_chm(config, des_chm_res = 0.25, max_height_threshold = 60, calculate_extent = FALSE)

###_____________________________________________________###
### Rasterize the stem section at DBH to stem CHM tiles ###
###_____________________________________________________###

rasterize_tiles_to_stem_chm(config, des_stem_chm_res = 0.07, min_z = 1.2, max_z = 1.4, calculate_extent = FALSE)

###_______________________________________________________________###
### Detect trees via stem search algorithm, then delineate crowns ###
###_______________________________________________________________###

detect_stems_within_tiles(config, dbh_max_size_m)
create_lax_for_tiles(config$las_stem_dir)

###_______________________________________###
### Detect crowns using top down approach ###
###_______________________________________###

detect_crowns_top_down(config, desired_chm_res = 0.25, window_size, minimum_height, min_crown_size_m2)

###________________________________________###
### Detect crowns using bottom up approach ###
###________________________________________###

detect_crowns_bottom_up(config, desired_chm_res = 0.25, window_size, minimum_height, min_crown_size_m2)

###_____________________________________________________________________###
### Run the hybrid crown delineation method with missing dbh prediction ###
###_____________________________________________________________________###

wrapper_hybrid_crown_delineation_missing_dbh_prediction(config)

###______________________________________###
### Run the final tree detection methods ###
###______________________________________###

detect_final_crowns(config, desired_chm_res = 0.25, window_size, minimum_height, min_crown_size_m2)

###___________________________________________###
### Run the crop and colorize by tree ID step ###
###___________________________________________###

crop_crowns_by_id_and_colorize_stems_and_veg(config, las_grid, colorize_input_las, stem_color_hex, veg_color_hex, ground_color_hex,
                                             use_bottom_up_crowns = FALSE)

###_____________________________________###
### Get final tree detection statistics ###
###_____________________________________###

final_stats = get_final_tree_stats(config)
final_stats

###_______________________________###
### Get the total processing time ###
###_______________________________###

total_end = Sys.time()
total_master = difftime(total_end, total_start, units = c("mins"))
print(paste0("Total tree pipeline took ", total_master, " minutes ... "))
