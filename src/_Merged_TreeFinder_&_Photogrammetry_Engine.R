###############################################################################
###############################################################################
###                                                                         ###
### Turnkey Photogrammetry Point Cloud Processing Script                    ###
### Photogrammetry ENGINE V8                                                ###
### Written by Neal Swayze 3/26/2021                                        ###
### POC: nealswayze1@gmail.com                                              ###
###                                                                         ###
### Updated 6/14/2023 by Wade Tinkham                                       ###
###                                                                         ###
###############################################################################
###############################################################################

# Creator Notes: 
# This script has been updated to include the most recent tree detection and 
# DBH detection capability, which tiles out large point clouds into smaller 
# regions, then loops through these tiles to detect stems.

###############################################################################
#####################      INSTALL / LOAD PACKAGES      #######################
###############################################################################

###############################################################################
### When installing TreeLS it may be necessary to install Rtools, for new   ###
### installations of R the system will prompt you to install this but in    ###
### older version you may need to do it yourself. For some installations    ###
### it has been necessary to run the TreeLS command twice.                  ###
###############################################################################

#library(devtools)
#writeLines('PATH="${RTOOLS40_HOME}\\usr\\bin;${PATH}"', con = "~/.Renviron")
Sys.which("make")

#install.packages("pacman")
library(pacman)
pacman::p_load(lidR, ForestTools, rlas, gstat, VoxR, raster, rgdal, sp, 
               rgeos, terra, sf, mapedit, ggplot2, tidyverse, RCSF, mapview,
               dplyr, data.table, foreach, doParallel)

#devtools::install_github("tiagodc/TreeLS")
library(TreeLS)

#devtools::install_github("andrew-plowright/ForestTools")                      
#library(ForestTools)

###############################################################################
####################        PREPERATION STEPS         #########################
###############################################################################

# 1. Create a folder on your desktop with the name "engine"

# 2. Put your Structure From Motion point cloud in the folder

# 3. Point your root directory at the engine folder

# 4. Change the name of your raw .las or .laz file on line 84

###############################################################################
#####################    CHANGE THE SETTINGS BELOW     ########################
###############################################################################

### Establish Root Directory ##################################################
rootDir <- ("/Users/Wade/Desktop/engine2")

### Set Max height for retaining points in the point cloud (m)
max_height = 35

### Set the Minimum height for tree detection 
min_height <- 1.37

### Set the Minimum height for DBH detection
min_height_dbh <- 1.37

### Set the desired resolution for the canopy height model
desired_chm_resolution <- 0.10

################################################################################
###   The following step will tile the single .las file into smaller tiles   ###
###        You will need to adjust the buffer size below on line 149         ###
###         You will need to adjust the chunk size below on line 153         ###
###           Check the visualization of the parameters on line 157          ###
################################################################################

### Read in the Las File
setwd(rootDir)
desired_las = "knf1_112m.laz"
ctg = readLAScatalog(desired_las)
ctg
las_check(ctg)
plot(ctg, mapview= TRUE, map.type = "Esri.WorldImagery")

### Setting tile buffer size (m)
opt_chunk_buffer(ctg) <- 0.5
chunk_buff <- opt_chunk_buffer(ctg)

### Setting tile size (m)
opt_chunk_size(ctg) <- 75
chunk_tile <- opt_chunk_size(ctg) 

### Checking your tile/buffer dimensions
plot(ctg, chunk = TRUE)

################################################################################
#####################  CONFIGURE PARALELL PROCESSING  ##########################
################################################################################

### Enable multiple cores for parallel processing (SOMEWHAT TEMPERMENTAL AND ###
### THROWS ERRORS, BUT DOES WORK)                                            ###
library(future)

### Detect the number of cores on your machine #################################
availableCores() 
plan(multisession, workers = 4L)
set_lidr_threads(4L)

################################################################################
################  AUTOMATICALLY CONFIGURE DIRECTORY STRUCTURE   ################
################################################################################

### Starting Time ##############################################################
start_time <- Sys.time()

### Creating a set of processing folders for outputs ###########################
dir.create(file.path(rootDir, "/outputs"))
dir.create(file.path(rootDir, "/outputs/01_las"))
dir.create(file.path(rootDir, "/outputs/02_CHM"))
dir.create(file.path(rootDir, "/outputs/03_single_tree_csv"))
dir.create(file.path(rootDir, "/outputs/03_single_tree_shp"))
dir.create(file.path(rootDir, "/outputs/03_single_tree_clipped"))
dir.create(file.path(rootDir, "/outputs/03_single_tree_metrics"))

### Setting your output directories for processing steps #######################
output <- file.path(rootDir, "/outputs")
las_output <- file.path(rootDir, "/outputs/01_las")
chm_output <- file.path(rootDir, "/outputs/02_CHM")
csv_output <- file.path(rootDir, "/outputs/03_single_tree_csv")
shp_output <- file.path(rootDir, "/outputs/03_single_tree_shp")
clip_output <- file.path(rootDir, "/outputs/03_single_tree_clipped")
tree_output <- file.path(rootDir, "/outputs/03_single_tree_metrics")

################################################################################
#################        DEFINE PROCESSING FUNCTIONS         ###################
################################################################################

### Ground Classification Function #############################################

ground_classify <- function(las) {
  ground <- classify_ground(las, csf(sloop_smooth = FALSE, 
                                     class_threshold = 0.2, 
                                     cloth_resolution = 0.2, 
                                     rigidness = 3L, 
                                     iterations = 500L, 
                                     time_step = 0.65))
  
  ground <- filter_poi(ground, Classification == 2)
  ground <- filter_poi(ground, Z < quantile(Z, .99))
  return(ground) # output
}

engine_ground <- function(chunk) {
  las <- readLAS(chunk)
  if (is.empty(las)) return(NULL)
  las <- ground_classify(las)
  las <- filter_poi(las, buffer == 0)
  #las <- clip_roi(las, study_area)
  return(las)
}

### Ground Classification and Height Normalization Function #####################

classify_normalize <- function(las) { # Classify grounds points using cloth simulation filter
  ground <- classify_ground(las, csf(sloop_smooth = FALSE, 
                                     class_threshold = 0.2, 
                                     cloth_resolution = 0.2, 
                                     rigidness = 3L, 
                                     iterations = 500L, 
                                     time_step = 0.65)) 
  
  normalized <- normalize_height(ground, knnidw()) # normalize height
  normalized <- filter_poi(normalized, Z > -0.01) # remove points below 0
  normalized <- filter_poi(normalized, Z < max_height) # remove points above maximum defined height
  return(normalized) # output
}

engine_height <- function(chunk) {
  las <- readLAS(chunk)
  if (is.empty(las)) return(NULL)
  las <- classify_normalize(las)
  las <- filter_poi(las, buffer == 0)
  #las <- clip_roi(las, study_area)
  return(las)
}

### Defining the stem map function to extract DBH values #######################
stem_cleaner <- function(las) {
  las <- filter_poi(las, Z < 4)
  las <- filter_poi(las, Z > 0.1)
  map <- treeMap(las, map.hough(min_h = 1.32, max_h = 1.42, min_density = 0.001, max_d = 1)) # Extract tree map from a thinned point cloud
  las <- treePoints(las, map, trp.crop()) # Classify tree regions
  las <- stemPoints(las, stm.hough(max_d = 1.5, min_density = 0.1)) # Classify stem points
  return(las) # output las file with stem map data
}

engine_stem <- function(chunk){
  las <- readLAS(chunk)
  if (is.empty(las)) return(NULL)
  las <- stem_cleaner(las)
  return(las)
}

### Creating an processing statistics dataframe ################################
names <- c("las_file", "raw_points", "ground_points", "normalized_points", "prop. ground retained", 
           "prop. normalized retained", "num detected trees", "num detected stems", "processing time (min)")
stats_dataframe <- data.frame()
for (k in names) stats_dataframe[[k]] <- as.numeric()
stats_dataframe$las_file <- as.character(stats_dataframe$las_file)
setwd(output)
write.csv(stats_dataframe, "processing_statistics.csv", row.names=FALSE)

################################################################################
#####################      PROCESSING STARTS BELOW      ########################
################################################################################

### Calculate full number of returns ###########################################
ctg_num <- ctg$Number.of.1st.return
ctg_num

################################################################################
##########################    PROCESSING STEP 1     ############################
################# Classifying Ground, Rasterizing to DEM  ######################
################################################################################

### Run the Ground Classification Step #########################################
options <- list(automerge = TRUE)
opt_chunk_buffer(ctg) <- chunk_buff
opt_chunk_size(ctg) <- chunk_tile
ground <- catalog_apply(ctg, engine_ground, .options = options)
dem <- grid_terrain(ground, res = desired_chm_resolution, algorithm = knnidw(k = 6L, p = 2))
plot(dem)

### Writing out the Las file to the desired directory ##########################
setwd(las_output)
desired_las <- tools::file_path_sans_ext(desired_las)
out_name <- paste0("ground_only_points__", desired_las,".laz")
writeLAS(ground, file = out_name)

### Writing out the DEM file to the desired directory ##########################
out_name<-paste0("dem_", desired_chm_resolution, "m__", desired_las, ".tif")
setwd(chm_output)
writeRaster(dem, file = out_name, overwrite = TRUE)

### Calculating the number of ground points remaining ##########################
ground_num <- ground$Z
ground_num <- as.data.frame(ground_num)
ground_num <- nrow(ground_num)
rm(dem, ground)
gc()

################################################################################
############################   PROCESSING STEP 2   #############################    
###################### height normalization & create CHM ####################### 
################################################################################

options <- list(automerge = TRUE)
opt_chunk_buffer(ctg) <- chunk_buff
opt_chunk_size(ctg) <- chunk_tile
normalized <- catalog_apply(ctg, engine_height, .options = options)
chm <- grid_canopy(normalized, res = desired_chm_resolution, algorithm = p2r())
plot(chm)

### Writing out the Las file to the desired directory ##########################
setwd(las_output)
desired_las <- tools::file_path_sans_ext(desired_las)
out_name_las <- paste0("normalized_points__", desired_las,".laz")
writeLAS(normalized, file = out_name_las)

### Writing out the DEM file to the desired directory ##########################
out_name<-paste0("chm_", desired_chm_resolution, "m__", desired_las, ".tif")
setwd(chm_output)
writeRaster(chm, file = out_name, overwrite = TRUE)

### Calculating the number of normalized points remaining ######################
norm_num <- normalized$Z
norm_num <- as.data.frame(norm_num)
norm_num <- nrow(norm_num)
gc()

################################################################################
###########################    PROCESSING STEP 3    ############################    
##########################   Single Tree Detection   ########################### 
################################################################################

################################################################################
### Set linear function for single tree detection and minimum tree      ######## 
### height for detection.The below single_tree_detection function is    ########
### based on work by Creasy et al. 2021.                                ########     
###                                                                     ########
### Maybe change the 0.2 on line 300 to improve tree extraction         ########
################################################################################
### Define single tree extraction function parameters
single_tree_detection <- function(x) { 1 + x * 0.2 }

### Detect single tree locations, calculate the number of detected trees, 
tree_tops <- find_trees(chm, lmf(single_tree_detection, hmin = min_height, shape = c("circular")))
plot(chm)
plot(tree_tops, add = TRUE) 

### Get single tree detected statistics for the input canopy height model
number_trees_detected <- nrow(tree_tops)
min_height_detected <- min(tree_tops$Z)
mean_height_detected <- mean(tree_tops$Z)
median_height_detected <- median(tree_tops$Z)
max_height_detected <- max(tree_tops$Z)
number_trees_detected
max_height_detected

### Delineate crowns, convert crown raster to spatvector polygon, then to spatial polygons dataframe
tree_crowns <- mcws(treetops = tree_tops, CHM = chm, format = "raster", minHeight = min_height, verbose = TRUE)
tree_crowns <- terra::rast(tree_crowns)
tree_crowns <- terra::as.polygons(tree_crowns)
tree_crowns <- as(tree_crowns, "Spatial")
tree_crowns
plot(chm)
plot(tree_crowns, add = TRUE)

### Save tree locations and heights to csv and shapefile #######################
setwd(csv_output)
write.csv(tree_tops, "tree_locations.csv")
write.csv(tree_crowns, "tree_crowns.csv")

setwd(shp_output)
writeOGR(tree_tops, dsn = '.', layer = "tree_heights", 
         driver = "ESRI Shapefile", overwrite_layer = TRUE)

writeOGR(tree_crowns, dsn = '.', layer = "tree_crowns", 
         driver = "ESRI Shapefile", overwrite_layer = TRUE)

################################################################################
##########################    PROCESSING STEP 4     ############################
#####################    DBH Detection and Extraction    #######################
################################################################################

### Read in the tree tops and tree crowns as an intermediate step in case of script failure
setwd(shp_output)
tree_tops <- readOGR('.', "tree_heights")
tree_crowns <- readOGR('.', "tree_crowns")

### Read in the tiles as a las catalog
setwd(las_output)
tile_ctg <- readLAScatalog(normalized_output)
tile_ctg
plot(tile_ctg)

### Create a list of tree top points
tree_list <- setNames(split(tree_tops, seq(nrow(tree_tops))), rownames(tree_tops))
length(tree_list)
class(tree_list)

### Create a list of crown polygons
crown_list <- setNames(split(tree_crowns, seq(nrow(tree_crowns))), rownames(tree_crowns))
length(crown_list)
class(crown_list)

length(tree_list)
length(crown_list)

###############################################
####### RUN DBH DETECTION FOREACH LOOP ########
###############################################

### Get number of cores, and activate processing cluster
num_cores <- 20
cl <- makeCluster(num_cores)
registerDoParallel(cl)

### For each loop to crop tile catalog to individual tree point clouds, detect DBH values, and match DBH Values
foreach(i = 1:length(tree_list), .errorhandling = "remove") %dopar% {
  
  ### Load nessesary packages
  library(lidR)
  library(rlas)
  library(raster)
  library(rgeos)
  library(TreeLS)
  library(dplyr)
  
  ##################################################
  ### SUMMARIZE THE DETECTED TREE AND CROWN DATA ###
  ##################################################
  
  ### Get the tree height and location for the tree ID
  tree_data <- tree_list[[i]]
  tree_data <- as.data.frame(tree_data)
  tree_data 
  
  ### Get the crown polygon for the tree ID
  crown_polygon <- crown_list[[i]]
  crown_area <- area(crown_polygon)
  crown_area
  
  ### Get detected tree id, coords, and crown area into a single dataframe
  all_tree_data <- cbind(tree_data, crown_area)
  all_tree_data
  
  ### Predict DBH based on height using FIA equation
  all_tree_data$predicted_dbh <- (1.36627 * (all_tree_data$tree_height^1.08107))
  
  #####################################################################
  ### CLIP THE POINT CLOUD FROM THE TILE CATALOG, CHECK STEM POINTS ###
  #####################################################################
  
  ### Clip the las file to the buffered polygon
  las <- clip_roi(tile_ctg, crown_polygon)
  #plot(las, color = "RGB")
  
  ### Get the number of stem points
  stem_points <- filter_poi(las, Z > 1.3)
  stem_points <- filter_poi(stem_points, Z < 1.44)
  stem_point_count <- as.data.frame(stem_points$Z)
  stem_point_count <- nrow(stem_point_count)
  stem_point_count
  
  #######################################################
  ### WRITE OUT TREE DATA IF THERE ARE NO STEM POINTS ###
  #######################################################
  
  if(stem_point_count < 20 ){
    
    ### Create dummy DBH detection objects
    dbh_x <- NA
    dbh_y <- NA
    radius <- NA
    dbh <- NA
    radius_error <- NA
    tree_height_treels <- NA
    dbh_height_match_error <- NA
    estimate_stem_volume_m3 <- NA
    selected_dbh <- data.frame(dbh_x, dbh_y, radius, dbh, radius_error, tree_height_treels, dbh_height_match_error, estimate_stem_volume_m3)
    selected_dbh
    
    ### Merge the candidate dbh data with the detected tree data
    final_tree_data <- cbind(all_tree_data, selected_dbh)
    final_tree_data
    
    out_name <- paste0("treeID_", final_tree_data$treeID, ".csv")
    setwd(csv_output)
    write.csv(final_tree_data, file = out_name)
    
    ### Color the canopy points
    points <- las
    point_data <- points@data
    point_data$Z <- final_tree_data$tree_height
    point_data$R <- 65025
    point_data$G <- 1
    point_data$B <- 1
    points@data <- point_data
    points
    
    ### Write out unmodified las file
    las <- rbind(las, points)
    out_las_name <- paste0("treeID_", final_tree_data$treeID, ".las")
    setwd(clip_output)
    writeLAS(las, file = out_las_name)
    
    rm(canopy_points, canopy_p ,median ,sd,lowest_live_crown_ht ,green_canopy_points,
       voxels,num_voxels,single_voxel_volume,green_canopy_volume_meters3,brown_canopy_points,
       brown_canopy_volume_meters3,dbh_x,dbh_y ,radius,dbh,radius_error,tree_height_treels,
       dbh_height_match_error, estimate_stem_volume_m3, final_tree_data)
    gc()
  }
  
  #########################################
  ### DETECT DBHs, SELECT TOP DBH VALUE ###
  #########################################
  
  if(stem_point_count > 20){
    
    ### Extract tree map from a thinned point cloud
    map <- treeMap(las, map.hough(min_h = 1.2, 
                                  max_h = 1.5, 
                                  min_density = 0.05, 
                                  max_d = 0.5))
    
    ### Classify tree regions
    las <- treePoints(las, map, trp.crop())
    
    ### Classify stem points
    las <- stemPoints(las, stm.hough(max_d = 1, min_density = 0.05))
    
    ### Get a dataframe of possible detected DBH values
    dbh_locations <- tlsInventory(las, dh = 1.37, dw = 0.2, d_method = shapeFit(shape = "circle", algorithm = "ransac", n = 30)) 
    dbh_locations <- subset(dbh_locations, Radius < 1.2)
    dbh_locations <- as.data.frame(dbh_locations)
    dbh_locations
    
    ### Calculate best DBH match based on DBH detected max height
    options("scipen" = 100, "digits" = 4)
    dbh_height_errors <-  abs(dbh_locations$H - all_tree_data$tree_height)
    dbh_height_errors <- as.data.frame(dbh_height_errors)
    best_dbh_match <- min(dbh_height_errors)
    dbh_locations <- cbind(dbh_locations, dbh_height_errors)
    dbh_locations 
    
    ### Filter the candidate DBH detections to get the best match, do some data reorganization
    selected_dbh_1 <- filter(dbh_locations, dbh_height_errors == best_dbh_match)
    selected_dbh <- subset(selected_dbh_1, select = c(X, Y, Radius, Error, H, dbh_height_errors))
    dbh <- selected_dbh$Radius * 2
    selected_dbh <- cbind(selected_dbh, dbh)
    selected_dbh <- subset(selected_dbh, select = c(X, Y, Radius, dbh, Error, H, dbh_height_errors))
    colnames(selected_dbh) <- c("dbh_x", "dbh_y", "radius", "dbh", "radius_error", "tree_height_treels", "dbh_height_match_error")
    selected_dbh$dbh * 100
    selected_dbh
    
    ### Get the stem only points for the selected tree
    stem_points_las <- filter_poi(las, Stem == TRUE)
    selected_stem_points <- filter_poi(stem_points_las, TreeID == selected_dbh_1$TreeID)
    #plot(selected_stem_points)
    
    ##############################################
    ### COMPARE PREDICTED DBH TO EXTRACTED DBH ###
    ##############################################
    
    dbh_comparison_error <- abs(selected_dbh$dbh - all_tree_data$predicted_dbh)
    dbh_comparison_error
    
    ### If comparison error is larger than 11 cm, write dummy dbh to disk
    if (dbh_comparison_error > 0.1025) {
      
      ### Create dummy DBH detection objects
      dbh_x <- NA
      dbh_y <- NA
      radius <- NA
      dbh <- NA
      radius_error <- NA
      tree_height_treels <- NA
      dbh_height_match_error <- NA
      estimate_stem_volume_m3 <- NA
      selected_dbh <- data.frame(dbh_x, dbh_y, radius, dbh, radius_error, tree_height_treels, dbh_height_match_error, estimate_stem_volume_m3)
      selected_dbh
      
      ### Merge the candidate dbh data with the detected tree data
      final_tree_data <- cbind(all_tree_data, selected_dbh)
      final_tree_data
      
      out_name <- paste0("treeID_", final_tree_data$treeID, ".csv")
      setwd(csv_output)
      write.csv(final_tree_data, file = out_name)
      
      ### Color the canopy points
      points <- las
      point_data <- points@data
      point_data$Z <- final_tree_data$tree_height
      point_data$R <- 65025
      point_data$G <- 1
      point_data$B <- 1
      points@data <- point_data
      
      points
      
      ### Write out unmodified las file
      las <- rbind(las, points)
      out_las_name <- paste0("treeID_", final_tree_data$treeID, ".las")
      setwd(clip_output)
      writeLAS(las, file = out_las_name)
      
      rm(canopy_points, canopy_p ,median ,sd,lowest_live_crown_ht ,green_canopy_points,
         voxels,num_voxels,single_voxel_volume,green_canopy_volume_meters3,brown_canopy_points,
         brown_canopy_volume_meters3,dbh_x,dbh_y ,radius,dbh,radius_error,tree_height_treels,
         dbh_height_match_error, estimate_stem_volume_m3, final_tree_data)
      
      gc()
    } 
    
    if (dbh_comparison_error < 0.1025) {
      
      ### Merge the candidate dbh data with the detected tree data
      final_tree_data <- cbind(all_tree_data, selected_dbh)
      final_tree_data
      
      ### Write the tree to the disk
      out_name <- paste0("treeID_", final_tree_data$treeID, ".csv")
      setwd(csv_output)
      write.csv(final_tree_data, file = out_name)
      
      ### Color the las files with attributes and write to disk - with DBH disk
      #########################################################################
      canopy_points <- las
      data <- canopy_points@data
      data$Z <- final_tree_data$tree_height
      data$R <- 65025
      data$G <- 1
      data$B <- 1
      canopy_points@data <- data
      
      ### Color the DBH disk
      dbh_points <- clip_circle(las, xcenter = final_tree_data$dbh_x, ycenter = final_tree_data$dbh_y, radius = final_tree_data$radius)
      data <- dbh_points@data
      data$Z <- 1.37
      data$R <- 65025
      data$G <- 55000
      data$B <- 1
      dbh_points@data <- data
      
      ### Bind the new data to the old las file, write out
      las <- rbind(las, canopy_points, dbh_points)
      setwd(clip_output)
      out_las_name <- paste0("treeID_", final_tree_data$treeID, ".las")
      out_las_name
      writeLAS(las, file = out_las_name)
    }
  }
  
  ### Clean up after yourself
  rm(tree_data, crown_data, all_tree_data, las, stem_point_count, dbh_x, dbh_y, radius, dbh, 
     radius_error, tree_height_treels, dbh_height_match_error, estimate_stem_volume_m3, selected_dbh,
     map, dbh_locations, selected_dbh, stem_points_las, selected_stem_points, is_radius_na,
     canopy_points, canopy_p, median, sd, lowest_live_crown_ht, green_canopy_points, voxels, num_voxels, 
     single_voxel_volume, brown_canopy_volume_meters3, data, brown_canopy_points, dbh_height_errors, dbh_locations, 
     final_tree_data, points, selected_dbh, stem_points_las, stem_point_count, selected_dbh_1, stem_points, stem_voxel, 
     dbh_points)
  
  gc()
}

### Stop the processing cluster
stopCluster(cl)

########################################################################
####### MERGE STEM MAP DATA AND RETURN FULL DATASET TO THE DISK ########
########################################################################

### Make list of  .csv files
setwd(csv_output)
list <- list.files(csv_output, pattern = "csv$", full.names = F)
list

### Get number of cores, and activate processing cluster
num_cores <- detectCores() - 2
cl <- makeCluster(num_cores)
registerDoParallel(cl)

### Foreach loop to merge the tree data
final_merged_data <- foreach(i = 1:length(list), .combine = rbind) %dopar% {
  
  ### Load Libraries
  library(data.table)
  
  ### Get first csv file
  first_file <- list[[i]]
  
  ### Read in desired file
  setwd(csv_output)
  first_file <- fread(first_file)
  
  ### Return to foreachloop
  return(first_file)
}

### Stop the processing cluster
stopCluster(cl)

### Return the result of the foreach loop
final_detected_tree_data <- as.data.frame(final_merged_data)
final_detected_tree_data
nrow(final_detected_tree_data)
length(crown_list)

### Check the final detected tree data
final_detected_tree_data
nrow(final_detected_tree_data)

temp_model_data <- data.frame(dbh = final_detected_tree_data$dbh,
                              ht = final_detected_tree_data$tree_height)
final_detected_tree_data$dbh = final_detected_tree_data$dbh
temp_model_data[temp_model_data == 0] <- NA
temp_model_data <- na.omit(temp_model_data)

height_to_dbh_model <- nls(dbh~b*ht^z,start = list(b = 2.2, z = 1), data=temp_model_data)
summary(height_to_dbh_model)

final_detected_tree_data$final_dbh <- environment(height_to_dbh_model[["m"]][["fitted"]])[["internalPars"]][["b"]] * final_detected_tree_data$tree_height^environment(height_to_dbh_model[["m"]][["fitted"]])[["internalPars"]][["z"]]

for(i in 1:nrow(final_detected_tree_data)){
  if(is.na(final_detected_tree_data$dbh[i]) == TRUE) {
    final_detected_tree_data$dbh[i] = final_detected_tree_data$final_dbh[i]
  } else {
    final_detected_tree_data$dbh[i] = final_detected_tree_data$dbh[i]
  }
}

### Write out the detected, merged tree data
setwd(root_dir)
fwrite(final_detected_tree_data, "raw_merged_tree_and_dbh_data.csv")
head(final_detected_tree_data)

#######################################
### SUMMARIZE PROCESSING STATISTICS ###
#######################################

detected_dbh_values <- paste0(nrow(subset_data), " detected DBH's")
detected_dbh_values
setwd(root_dir)
write.table(detected_dbh_values, "num_detected_dbh.txt", row.names = FALSE, col.names = FALSE, quote = FALSE, sep= '\n')

proportion_detected_dbh <- (nrow(subset_data) / nrow(final_detected_tree_data)) * 100
proportion_detected_dbh <- paste0(proportion_detected_dbh, " % of trees with detected DBHs")
proportion_detected_dbh
setwd(root_dir)
write.table(proportion_detected_dbh, "proportion_detected_dbh.txt", row.names = FALSE, col.names = FALSE, quote = FALSE, sep= '\n')

### Get total processing time
end_time <- Sys.time()
time_taken <- paste0(as.numeric(difftime(time1 = end_time, time2 = start_time, units = "hours")))
time_taken <- paste0(time_taken,"  hours of processing time")
setwd(root_dir)
write.table(time_taken, "total_processing_time.txt", row.names = FALSE, col.names = FALSE, quote = FALSE, sep= '\n')
