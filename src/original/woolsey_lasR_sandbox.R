################################################################################
################################################################################
################################################################################
### 2024-03-18 lasR 0.3.2 updates
################################################################################
################################################################################
################################################################################
######################################
# DEFINE ALL lasR pipeline steps
######################################

########## !!!!!!!!!!!!!!!!! run point_cloud_processing.r to line 328...thru: 
 ## las_ctg = lidR::readLAScatalog(config$input_las_dir)

# remove all files in delivery and temp
list.files(config$temp_dir, recursive = T, full.names = T) %>%
    purrr::map(file.remove)
list.files(config$delivery_dir, recursive = T, full.names = T) %>%
    purrr::map(file.remove)

xxst_time = Sys.time()

######################################
# chunk for high density pt clouds
######################################
# define function to get chunk size based on sample data testing
  get_chunk_size_fn = function(ctg, max_pts_m2 = 1000){
    dta_sum = ctg@data %>%
      dplyr::mutate(
        area_m2 = sf::st_area(geometry) %>% as.numeric()
        , pts = Number.of.point.records
      ) %>% 
      sf::st_drop_geometry() %>% 
      dplyr::select(area_m2, pts) %>% 
      dplyr::summarise(
        tot_area_m2 = sum(area_m2)
        , dplyr::across(.cols = tidyselect::everything(), .fns = mean)
      ) %>% 
      dplyr::mutate(pts_m2 = pts/area_m2) %>% 
      dplyr::filter(pts_m2==max(pts_m2)) %>% 
      dplyr::mutate(
        factor = pts_m2/max_pts_m2
        , chunk = dplyr::case_when(
            factor <= 1 ~ 0
            , TRUE ~ round(sqrt(tot_area_m2/factor), digits = -1) # round to nearest 10
          )
      )
    return(dta_sum$chunk[1])
  }

  # set options for retile
  lidR::opt_chunk_size(las_ctg) = get_chunk_size_fn(ctg = las_ctg, max_pts_m2 = 1000)
  lidR::opt_chunk_buffer(las_ctg) = 10
  # plot(las_ctg, chunk = T)
  # apply the retile
  lidR::opt_progress(las_ctg) = F
  lidR::opt_output_files(las_ctg) = paste0(config$las_grid_dir,"/", "_{XLEFT}_{YBOTTOM}") # label outputs based on coordinates
  lidR::catalog_retile(las_ctg) # apply retile
  # create spatial index
  create_lax_for_tiles(
    las_file_list = list.files(config$las_grid_dir, pattern = ".*\\.(laz|las)$", full.names = T)
  )
######################################
# lasR steps
######################################
  ###################
  # read with filter
  ###################
    lasr_read = lasR::reader_las(filter = "-drop_duplicates")
    # lasr_read = lasR::reader_las()
  ###################
  # classify
  ###################
  ## !!!!!!!!!!!!!!!! using lasR, but NOT using lasR...
    # Classify pipelines use lasR::callback() that exposes the point cloud as a data.frame. 
    # One of the reasons why lasR is more memory-efficient and faster than lidR is that it 
    # does not expose the point cloud as a data.frame. Thus, these pipelines are not very different 
    # from the lidR::classify_ground() function in lidR. 
    # The advantage of using lasR here is the ability to pipe different stages.
    # There is no function in lasR to classify the points...create one
    lasr_classify = function(
      # csf options
      smooth = FALSE, threshold = 0.5
      , resolution = 0.5, rigidness = 1L
      , iterations = 500L, step = 0.65
    ){
      csf = function(data, smooth, threshold, resolution, rigidness, iterations, step)
      {
        id = RCSF::CSF(data, smooth, threshold, resolution, rigidness, iterations, step)
        class = integer(nrow(data))
        class[id] = 2L
        data$Classification <- class
        return(data)
      }
      
      classify = lasR::callback(
        csf
        , expose = "xyz"
        # csf options
        , smooth = smooth, threshold = threshold
        , resolution = resolution, rigidness = rigidness
        , iterations = iterations, step = step
      )
      return(classify)
    }
  
  ###################
  # denoise
  ###################
    # classify isolated points
    lasr_denoise = lasR::classify_isolated_points(res =  5, n = 6)
    # lasr_denoise_filter = lasR::delete_points(filter = lasR::drop_noise())
    
  ###################
  # dtm
  ###################
    # file name for write
      dtm_file_name = paste0(config$delivery_dir, "/dtm_", desired_dtm_res, "m.tif")
    # perform Delaunay triangulation
      lasr_triangulate = lasR::triangulate(
        # class 2 = ground; class 9 = water
        filter = lasR::keep_ground() + lasR::keep_class(c(9)) + lasR::drop_noise()
        , max_edge = c(0,1)
        # # write to disk to preserve memory
        # , ofile = paste0(config$las_denoise_dir, "/", "*_tri.gpkg")
      )
    # rasterize the result of the Delaunay triangulation
      lasr_dtm = lasR::rasterize(
        res = desired_dtm_res
        , lasr_triangulate
        , filter = lasR::drop_noise()
        # write to disk to preserve memory
        , ofile = paste0(config$dtm_dir, "/", "*_dtm.tif")
      )
    # # Pits and spikes filling for raster with algorithm from St-Onge 2008 (see reference).
      lasr_dtm_pitfill = lasR::pit_fill(raster = lasr_dtm, ofile = dtm_file_name)
  
  ###################
  # normalize
  ###################
    # normalize
      lasr_normalize = lasR::transform_with(lasr_triangulate, operator = "-")
      ##### ^^^^^ this was taking forever with high density point clouds
      ##### .... trying with callback instead
    # define normalize function using callback
      lasr_normalize_expose = function(use_tin = F){
        normalize_ht = function(data, use_tin)
        {
          # read with lidR
          las = lidR::LAS(data)
          # height normalize
          if(use_tin == T){
            # height normalize using tin
            las_norm = lidR::normalize_height(las, algorithm = tin())
          }else{
            # height normalize using knnidw
            las_norm = lidR::normalize_height(las, algorithm = knnidw())
          }
          # update Z
          data$Z = as.numeric(las_norm@data$Z)
          return(data)
        }
        # pass to callback
        normalize = lasR::callback(
          normalize_ht
          , expose = "xyzc"
          # normalize options
          , use_tin = use_tin
        )
        return(normalize)
      }
    # write
      lasr_write_normalize = lasR::write_las(
        filter = "-drop_z_below 0 -drop_class 2 9 18"
        , ofile = paste0(config$las_normalize_dir, "/*_normalize.las")
        , keep_buffer = T
      )
  
  ###################
  # chm
  ###################
    # file name for write
      chm_file_name = paste0(config$delivery_dir, "/chm_", desired_chm_res, "m.tif")
  
    # chm
      #set up chm pipeline step
      # operators = "max" is analogous to `lidR::rasterize_canopy(algorithm = p2r())`
      # for each pixel of the output raster the function attributes the height of the highest point found
      lasr_chm = lasR::rasterize(
        res = desired_chm_res
        , operators = "max"
        , filter = paste0(
          "-drop_class 2 9 18 -drop_z_below "
          , minimum_tree_height_m
          , " -drop_z_above "
          , max_height_threshold_m
        )
        # write to disk to preserve memory
        # , ofile = paste0(config$chm_dir, "/", "*_chm.tif")
      )
    # Pits and spikes filling for raster with algorithm from St-Onge 2008 (see reference).
      lasr_chm_pitfill = lasR::pit_fill(raster = lasr_chm, ofile = chm_file_name)

######################################
# lasR full pipeline
######################################
  if(keep_intermediate_files == T){
    # set up intermediate file write steps
      # classify write step
      lasr_write_classify = lasR::write_las(
          ofile = paste0(config$las_classify_dir, "/*_classify.las")
          , filter = "-drop_noise -drop_duplicates"
          , keep_buffer = F
        )
    # pipeline
      lasr_pipeline_temp = lasr_read +
        lasr_classify() + lasr_denoise + lasr_denoise_filter +
        lasr_write_classify + 
        lasr_triangulate +
        lasr_dtm + lasr_dtm_pitfill +
        # lasr_normalize + lasr_write_normalize +
        lasr_normalize_expose() + lasr_write_normalize +
        lasr_chm + lasr_chm_pitfill
  }else{
    # pipeline
      lasr_pipeline_temp = lasr_read +
        lasr_classify() + lasr_denoise + 
        lasr_triangulate +
        lasr_dtm + lasr_dtm_pitfill +
        # lasr_normalize + lasr_write_normalize +
        lasr_normalize_expose() + lasr_write_normalize +
        lasr_chm + lasr_chm_pitfill
  }
######################################
# execute pipeline
######################################
  message(
    "starting lasR processing with a chunk size of "
    , lidR::opt_chunk_size(las_ctg)
    , " (0 = no chunking) and a buffer of "
    , lidR::opt_chunk_buffer(las_ctg)
    , "..."
    , Sys.time()
  )
  # execute
    lasr_ans = lasR::exec(
      lasr_pipeline_temp
      , ncores = (lasR::ncores()-1)
      , on = list.files(config$las_grid_dir, pattern = ".*\\.(laz|las)$", full.names = T)
      , progress = T
      # , on = las_ctg
      # , on = las_grid
      # , buffer = 10 #las_grid_buff_m # 10
      # , chunk = 1500 #las_grid_res_m # 100
    )
  
  # clean up
    remove(list = ls()[grep("_temp",ls())])
    gc()

###################################
# lasR cleanup and polish
###################################
  # lasr_ans %>% str()
  # lasr_ans$pit_fill %>% terra::plot()
  # lasr_ans$pit_fill %>% terra::crs()
  # lasr_ans[[length(lasr_ans)]] %>% terra::plot()
  
  # fill dtm cells that are missing still with the mean of a window
    dtm_rast = terra::rast(dtm_file_name)
    dtm_rast = dtm_rast %>%
      terra::crop(
        las_ctg@data$geometry %>% 
          sf::st_union() %>% 
          terra::vect() %>% 
          terra::project(terra::crs(dtm_rast))
      ) %>% 
      terra::mask(
        las_ctg@data$geometry %>% 
          sf::st_union() %>% 
          terra::vect() %>% 
          terra::project(terra::crs(dtm_rast))
      ) %>% 
      terra::focal(
        w = 3
        , fun = "mean"
        , na.rm = T
        # na.policy Must be one of:
          # "all" (compute for all cells)
          # , "only" (only for cells that are NA)
          # , or "omit" (skip cells that are NA).
        , na.policy = "only"
      )
    # dtm_rast %>% terra::crs()
    # dtm_rast %>% terra::plot()
    
  # write to delivery directory
    terra::writeRaster(
      dtm_rast
      , filename = dtm_file_name
      , overwrite = T
    )
  
  # fill chm cells that are missing still with the mean of a window
    chm_rast = terra::rast(chm_file_name)
    chm_rast = chm_rast %>%
      terra::crop(
        las_ctg@data$geometry %>% 
          sf::st_union() %>% 
          terra::vect() %>% 
          terra::project(terra::crs(chm_rast))
      ) %>% 
      terra::mask(
        las_ctg@data$geometry %>% 
          sf::st_union() %>% 
          terra::vect() %>% 
          terra::project(terra::crs(chm_rast))
      ) %>% 
      terra::focal(
        w = 3
        , fun = "mean"
        , na.rm = T
        # na.policy Must be one of:
          # "all" (compute for all cells)
          # , "only" (only for cells that are NA)
          # , or "omit" (skip cells that are NA).
        , na.policy = "only"
      )
    # chm_rast %>% terra::crs()
    # chm_rast %>% terra::plot()
    
  # write to delivery directory
    terra::writeRaster(
      chm_rast
      , filename = chm_file_name
      , overwrite = T
    )
  
  # create spatial index files (.lax)
    # normalize
    create_lax_for_tiles(
      las_file_list = list.files(config$las_normalize_dir, pattern = ".*\\.(laz|las)$", full.names = T)
    )
    # classify
    create_lax_for_tiles(
      las_file_list = list.files(config$las_classify_dir, pattern = ".*\\.(laz|las)$", full.names = T)
    )
  
  # clean up
    remove(list = ls()[grep("_temp",ls())])
    # remove(lasr_ans)
    gc()

# ###################################
# # tile the normalized files to process with TreeLS
# ###################################
#   norm_ctg_temp = lidR::readLAScatalog(config$las_normalize_dir)
#   # retile catalog
#   lidR::opt_progress(norm_ctg_temp) = F
#   lidR::opt_output_files(norm_ctg_temp) = paste0(config$las_grid_dir,"/", "_{XLEFT}_{YBOTTOM}") # label outputs based on coordinates
#   lidR::opt_chunk_buffer(norm_ctg_temp) = las_grid_buff_m
#   lidR::opt_chunk_size(norm_ctg_temp) = las_grid_res_m # retile
#   lidR::catalog_retile(norm_ctg_temp) # apply retile
#   # create spatial index
#   create_lax_for_tiles(
#     las_file_list = list.files(config$las_grid_dir, pattern = ".*\\.(laz|las)$", full.names = T)
#   )
#   # clean up
#   remove(list = ls()[grep("_temp",ls())])
#   gc()

message("total time to process was "
          , difftime(Sys.time(), xxst_time, units = c("mins")) %>% as.numeric()
          , "mins"
        )
  
dtm_rast %>% 
  as.data.frame(xy = T) %>% 
  dplyr::rename(f=3) %>% 
  ggplot() +
    geom_tile(aes(x=x,y=y,fill=f)) +
    geom_sf(data = las_ctg@data$geometry, fill = NA) +
    scale_fill_viridis_c(option = "viridis") +
    theme_void()
chm_rast %>% 
  as.data.frame(xy = T) %>% 
  dplyr::rename(f=3) %>% 
  ggplot() +
    geom_tile(aes(x=x,y=y,fill=f)) +
    geom_sf(data = las_ctg@data$geometry, fill = NA) +
    scale_fill_viridis_c(option = "plasma") +
    theme_void()

#################################################################################
#################################################################################
# Detect tree stems 
#################################################################################
#################################################################################

  ###_____________________________________________________###
  ### Define function to map for potential tree locations ###
  ### Using TreeLS::treeMap                               ###
  ###_____________________________________________________###
  ### Function to map for potential tree locations with error handling
    tree_map_function <- function(las){
      result <- tryCatch(
        expr = {
          map = TreeLS::treeMap(
            las = las
            , method = map.hough(
              # height thresholds applied to filter a point cloud before processing
              min_h = 1
              , max_h = 5
              # height interval to perform point filtering/assignment/classification
              , h_step = 0.5
              # pixel side length to discretize the point cloud layers 
                # while performing the Hough Transform circle search
              , pixel_size = 0.025
              # largest tree diameter expected in the point cloud
              , max_d = 0.75 # 0.75m = 30in
              # minimum point density (0 to 1) within a pixel evaluated 
                # on the Hough Transform - i.e. only dense point clousters will undergo circle search
                # hey google, define "clouster" ?
              , min_density = 0.0001
              # minimum number of circle intersections over a pixel 
                # to assign it as a circle center candidate.
              , min_votes = 3
            )
            # parameter passed down to treeMap.merge (if merge > 0)
            , merge = 0
          )
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
  
  ## Define the stem processing function
    # This function combines `TreeLS` processing to the normalized point cloud to: 
    # 
    # 1) Apply the `TreeLS::treeMap` [stem detection function](#detect_stem_fn)
    # 2) Merge overlapping tree coordinates using `TreeLS::treeMap.merge`
    # 3) Assign tree IDs to the original points using `TreeLS::treePoints`
    # 4) Flag only the stem points using `TreeLS::stemPoints`
    # 5) DBH estimation is done using `TreeLS::tlsInventory`
    # 
    # The result writes: 
      # i) a `laz` to the `r config$las_stem_dir` directory with the 
      #   `Classification` data updated to: 
      #     ground points (class 2); 
      #     water points (class 9); 
      #     stem points (class 4); non-stem (class 5). 
      # ii) Also written is a `parquet` file with the tree identification stem locations, heights, and DBH estimates.
  
  # pass this function a file path of the normalized las you wish to detect stems and classify
    write_stem_las_fn <- function(las_path_name, min_tree_height = 2) {
        ### Get the desired las file
        las_name = basename(las_path_name)
        
        ### See if the las file has been generated
        path_to_check = paste0(config$las_stem_dir, "/", las_name)
        does_file_exist = file.exists(path_to_check)
        ### See if the vector file has been generated
        path_to_check = paste0(config$stem_poly_tile_dir, "/", tools::file_path_sans_ext(las_name), ".parquet")
        does_file_exist2 = file.exists(path_to_check)
        # does_file_exist2
        
        # IF FILES DO NOT EXIST...DO IT
        if(does_file_exist == F | does_file_exist2 == F){
          ### Read in the desired las file
          las_norm_tile = lidR::readLAS(las_path_name)
          las_norm_tile = lidR::filter_poi(las_norm_tile, Z >= 0)
          
          # get the maximum point height
          max_point_height = max(las_norm_tile@data$Z)
          
          # IF MAX HEIGHT GOOD...KEEP DOING IT
          if(max_point_height >= min_tree_height){
            ###______________________________________________________________###
            ### 1) Apply the `TreeLS::treeMap` [stem detection function](#detect_stem_fn)
            ###______________________________________________________________###
            ### Run the function to search for candidate locations
            treemap_temp = tree_map_function(las_norm_tile)
            
            ### If the class of the result == "LAS"...REALLY KEEP DOING IT
            if(class(treemap_temp) == "LAS"){
              ###______________________________________________________________###
              ### 2) Merge overlapping tree coordinates using `TreeLS::treeMap.merge`
              ###______________________________________________________________###
              treemap_temp = TreeLS::treeMap.merge(treemap_temp)
              ###______________________________________________________________###
              ### 3) Assign tree IDs to the original points using `TreeLS::treePoints`
              ###______________________________________________________________###
              ### Classify tree regions
              ## Assigns TreeIDs to a LAS object based on coordinates extracted from a treeMap object.
              las_norm_tile = TreeLS::treePoints(
                las = las_norm_tile
                , map = treemap_temp
                , method = trp.crop(l = 3)
              )
              # plot(las_norm_tile, color = "TreeID")
              
              ###______________________________________________________________###
              ### 4) Flag only the stem points using `TreeLS::stemPoints`
              ###______________________________________________________________###
              ### Classify stem points
              las_norm_tile = TreeLS::stemPoints(
                las = las_norm_tile
                , method = stm.hough(
                  # height interval to perform point filtering/assignment/classification.
                  h_step = 0.5
                  # largest tree diameter expected in the point cloud
                  , max_d = 0.75 # 0.75m = 30in
                  # tree base height interval to initiate circle search
                  , h_base = c(1, 2.5)
                  #  pixel side length to discretize the point cloud layers 
                    # while performing the Hough Transform circle search.
                  , pixel_size = 0.025
                  # minimum point density (0 to 1) within a pixel evaluated 
                    # on the Hough Transform - i.e. only dense point clousters will undergo circle search
                    # hey google, define "clouster" ?
                  , min_density = 0.1
                  # minimum number of circle intersections over a pixel 
                    # to assign it as a circle center candidate.
                  , min_votes = 3
                )
              )
              
              ###______________________________________________________________###
              ### 5) DBH estimation is done using `TreeLS::tlsInventory`
              ###______________________________________________________________###
              ### Search through tree points and estimate DBH to return a data frame of results
                tree_inv_df = TreeLS::tlsInventory(
                  las = las_norm_tile
                  # height layer (above ground) to estimate stem diameters, in point cloud units
                  , dh = 1.37
                  # height layer width, in point cloud units
                  , dw = 0.2
                  # parameterized shapeFit function, i.e. method to use for diameter estimation.
                  , d_method = shapeFit(
                    # either "circle" or "cylinder".
                    shape = "circle"
                    # optimization method for estimating the shape's parameters
                    , algorithm = "ransac"
                    # number of points selected on every RANSAC iteration.
                    , n = 20
                  )
                )
                # class(tree_inv_df)
                # tree_inv_df %>% dplyr::glimpse()
            if(nrow(tree_inv_df)>0){
              ###_______________________________________________________###
              ### 93) clean up the DBH stem data frame ###
              ###_______________________________________________________###
                # add details to table and convert to sf data
                tree_inv_df = tree_inv_df %>% 
                  dplyr::mutate(
                    Radius = as.numeric(Radius)
                    , dbh_m = Radius*2
                    , dbh_cm = dbh_m*100
                    , basal_area_m2 = pi * (Radius)^2
                    , basal_area_ft2 = basal_area_m2 * 10.764
                    , treeID = paste0(X, "_", Y)
                    , stem_x = X
                    , stem_y = Y
                  ) %>% 
                  sf::st_as_sf(coords = c("X", "Y"), crs = sf::st_crs(las_norm_tile)) %>% 
                  dplyr::select(
                    treeID, H, stem_x, stem_y, Radius, Error
                    , dbh_m, dbh_cm, basal_area_m2, basal_area_ft2
                  ) %>% 
                  dplyr::rename(
                    tree_height_m = H
                    , radius_m = Radius
                    , radius_error_m = Error
                  )
                # tree_inv_df %>% dplyr::glimpse()
                
                ### Remove points outside the bounding box of the laz tile + 1m buffer
                tree_inv_df = tree_inv_df %>% 
                  sf::st_crop(
                    sf::st_bbox(las_norm_tile) %>% 
                      sf::st_as_sfc() %>% 
                      sf::st_buffer(1)
                  )
              
              ###_______________________________________________________###
              ### Set the classification codes of different point types ###
              ###_______________________________________________________###
              
              ### Pull out the stem files
              stem_points = lidR::filter_poi(las_norm_tile, Stem == TRUE)
              stem_points@data$Classification = 4
              
              ### Pull out the ground points
              ground = filter_poi(las_norm_tile, Classification %in% c(2,9))
              
              ### Pull out the remaining points that arent ground
              remaining_points = filter_poi(las_norm_tile, Stem == FALSE & !(Classification %in% c(2,9)))
              remaining_points@data$Classification = 5
              
              ### Combine the newly classified data
              las_reclassified = rbind(stem_points, ground, remaining_points)
              # str(las_reclassified)
              # class(las_reclassified)
              # plot(las_reclassified, color = "Classification")
              
              ###_______________________________________________________###
              ### Write output to disk ###
              ###_______________________________________________________###
              ### Write the stem points to the disk
              if(class(las_reclassified)=="LAS"){
                lidR::writeLAS(las_reclassified, paste0(config$las_stem_dir, "/", las_name))
              }
              
              ### Write stem polygons to the disk
              out_name = tools::file_path_sans_ext(las_name)
              out_name = paste0(config$stem_poly_tile_dir, "/", out_name, ".parquet")
              if(max(class(tree_inv_df)=="sf")==1){
                sfarrow::st_write_parquet(tree_inv_df, out_name)
              }
              return(T)
            }else{return(F)} # nrow(tree_inv_df)>0
          }else{return(F)} # tree_map_function() return is LAS
        }else{return(F)} # max_point_height >= min_tree_height
      }else{return(F)} # DOES FILE EXIST == F
    } # write_stem_las_fn
  
  #######################################
  ### call write_stem_las_fn
  #######################################
    
    if(use_parallel_processing == T){
      # Error in unserialize(socklist[[n]]) : error reading from connection
      # means that one of the workers died when trying to process...try restarting
      #######################################
      ### a parallel version is:
      #######################################
        flist_temp = list.files(config$las_normalize_dir, pattern = ".*\\.(laz|las)$", full.names = T)
        # configure parallel
        cores = parallel::detectCores()
        cluster = parallel::makeCluster(cores-1)
        # register the parallel backend with the `foreach` package
        doParallel::registerDoParallel(cluster)
        # pass to foreach to process each lidar file in parallel
          write_stem_las_ans = 
            foreach::foreach(
              i = 1:length(flist_temp)
              , .packages = c("tools","lidR","tidyverse","doParallel","TreeLS")
              , .inorder = F
            ) %dopar% {
              write_stem_las_fn(las_path_name = flist_temp[i], min_tree_height = minimum_tree_height_m)
            } # end foreach
          # write_stem_las_ans
        # stop parallel
        parallel::stopCluster(cluster)
      #######################################
      ### a parallel version is ^
      #######################################
    }else{
      # map over the normalized point cloud tiles
      write_stem_las_ans = 
        list.files(config$las_normalize_dir, pattern = ".*\\.(laz|las)$", full.names = T) %>%
          purrr::map(write_stem_las_fn, min_tree_height = minimum_tree_height_m)
    }
  
  # get file list
  las_stem_flist = list.files(config$las_stem_dir, pattern = ".*\\.(laz|las)$", full.names = T)
  
  # create spatial index files (.lax)
  create_lax_for_tiles(las_stem_flist)
  
  # clean up
    remove(list = ls()[grep("_temp",ls())])
    gc()
  
#################################################################################
#################################################################################
# Combine Stem Vector Data to get points with sfm DBH estimates
#################################################################################
#################################################################################
  # Combine the vector data written to the config$stem_poly_tile_dir directory as `parquet` tile files
  ###__________________________________________________________###
  ### Merge the stem vector location tiles into a single object ###
  ###__________________________________________________________###
  if(
    length(list.files(config$stem_poly_tile_dir, pattern = ".*\\.parquet$", full.names = T)) > 0
  ){
    dbh_locations_sf = list.files(config$stem_poly_tile_dir, pattern = ".*\\.parquet$", full.names = T) %>% 
        purrr::map(sfarrow::st_read_parquet) %>% 
        dplyr::bind_rows() %>% 
        sf::st_as_sf() %>% 
        sf::st_make_valid() %>% 
        sf::st_set_crs(proj_crs)
  
    ## Clean the Stem Vector Data
    # The cleaning process uses the following steps:
    # * remove stems with empty radius estimates from the `TreeLS::tlsInventory` DBH estimation step
    # * remove stems >= DBH threshold set by the user in the parameter `dbh_max_size_m` (`r dbh_max_size_m`m in this example)
    # * remove stems with empty or invalid xy coordinates
    
    dbh_locations_sf = dbh_locations_sf %>% 
      dplyr::filter(
        !is.na(radius_m)
        & dbh_m <= dbh_max_size_m
        & sf::st_is_valid(.)
        & !sf::st_is_empty(.)
      ) %>% 
      dplyr::mutate(
        condition = "detected_stem"
      )
  
    ###___________________________________________________________###
    ### Write the detected DBHs
    ###___________________________________________________________###
      sf:::st_write(
        dbh_locations_sf
        , dsn = paste0(config$delivery_dir, "/bottom_up_detected_stem_locations.gpkg")
        , append = FALSE
        , delete_dsn = TRUE
        , quiet = TRUE
      )
  }else{dbh_locations_sf = NA}
  # clean up
  remove(list = ls()[grep("_temp",ls())])
  gc()
# start time
  xx8_itd_start_time = Sys.time()
#################################################################################
#################################################################################
# CHM Individual Tree Detection
#################################################################################
#################################################################################
  ###__________________________________________________________###
  ### Individual tree detection (ITD) 
  ###__________________________________________________________###
  # This section utilizes the Canopy Height Model (CHM) raster

  ###_________________________________###
  ### define the variable window function
  ###_________________________________###
  # (with a minimum window size of 2m and a maximum of 5m)
  # define the variable window function
    ws_fn = function(x) {
      y = dplyr::case_when(
        is.na(x) ~ 1e-3 # requires non-null
        , x < 0 ~ 1e-3 # requires positive
        , x < 2 ~ 1 # set lower bound
        , x > 30 ~ 5  # set upper bound
        , TRUE ~ 0.75 + (x * 0.14)
      )
      return(y)
    }

  ###___________________________________________________###
  ### Individual tree detection using CHM (top down)
  ###___________________________________________________###
    ### ITD on CHM
    # call the locate_trees function and pass the variable window
    tree_tops = lidR::locate_trees(
        chm_rast
        , algorithm = lmf(
          ws = ws_fn
          , hmin = minimum_tree_height_m
        )
      )
  
    # # what it is?
    #   ggplot() + 
    #     geom_tile(
    #       data = chm_rast %>% terra::aggregate(3) %>% as.data.frame(xy=T) %>% dplyr::rename(f=3)
    #       , mapping = aes(x=x,y=y,fill=f)
    #     ) +
    #     geom_sf(
    #       data = tree_tops
    #       , shape = 20
    #       , color = "black"
    #       , size = 0.5
    #     ) +
    #     coord_sf(
    #       expand = FALSE
    #     ) +
    #     scale_fill_viridis_c(option = "plasma") +
    #     labs(fill = "Hgt. (m)", x = "", y = "") +
    #     theme_light()
      
  # clean up
    remove(list = ls()[grep("_temp",ls())])
    gc()

#################################################################################
#################################################################################
# Delineate Tree Crowns 
#################################################################################
#################################################################################
  # delineate tree crowns raster and vector (i.e. polygon) shapes
    # this takes way too looonnggggg ..........
      # crowns = lidR::watershed(
      #     # input raster layer !! works with stars !! ? not with terra ;[
      #     chm = chm_rast %>% stars::st_as_stars()
      #       # terra::subst(from = as.numeric(NA), to = 0)
      #     # threshold below which a pixel cannot be a tree. Default is 2
      #     , th_tree = minimum_tree_height_m
      #   )() # keep this additional parentheses's so it will work ?lidR::watershed
  
    # using ForestTools instead ..........
      crowns = ForestTools::mcws(
        treetops = sf::st_zm(tree_tops, drop = T) # drops z values
        , CHM = chm_rast
        , minHeight = minimum_tree_height_m
      )
    
    # str(crowns)
    # plot(crowns, col = (viridis::turbo(2000) %>% sample()))
    # crowns %>% terra::freq() %>% nrow()
    
  ### Write the crown raster to the disk
    terra::writeRaster(
      crowns
      , paste0(config$delivery_dir, "/top_down_detected_tree_crowns.tif")
      , overwrite = TRUE
    )
  
  # clean up
    remove(list = ls()[grep("_temp",ls())])
    gc()
# start time
  xx9_competition_start_time = Sys.time()
#################################################################################
#################################################################################
# Calculate local tree competition metrics for use in modelling
#################################################################################
#################################################################################
  # From [Tinkham et al. (2022)]
  # Local competition metrics, including: 
    # the distance to the nearest neighbor
    # , trees ha^−1^ within a 5 m radius
    # , and the relative tree height within a 5 m radius
    
  ### add tree data to tree_tops
  tree_tops = tree_tops %>% 
    # pull out the coordinates and update treeID
    dplyr::mutate(
      tree_x = sf::st_coordinates(.)[,1]
      , tree_y = sf::st_coordinates(.)[,2]
      , tree_height_m = sf::st_coordinates(.)[,3]
      , treeID = paste(treeID,round(tree_x, 1),round(tree_y, 1), sep = "_")
    )
  # str(tree_tops)
    
  ### set buffer around tree to calculate competition metrics
  competition_buffer_m = 5
  ### how much of the buffered tree area is within the study boundary?
    # use this to scale the TPA estimates below
  tree_tops_pct_buffer_temp = tree_tops %>% 
    # buffer point
    sf::st_buffer(competition_buffer_m) %>% 
    dplyr::mutate(
      point_buffer_area_m2 = as.numeric(sf::st_area(.))
    ) %>% 
    # intersect with study bounds
    sf::st_intersection(
      las_ctg@data$geometry %>% 
        sf::st_union() %>% 
        sf::st_as_sf()
    ) %>% 
    # calculate area of buffer within study
    dplyr::mutate(
      buffer_area_in_study_m2 = as.numeric(sf::st_area(.))
    ) %>% 
    sf::st_drop_geometry() %>% 
    dplyr::select(treeID, buffer_area_in_study_m2)
  
  ### use the tree top location points to get competition metrics
  comp_tree_tops_temp = tree_tops %>% 
    # buffer point
    sf::st_buffer(competition_buffer_m) %>% 
    dplyr::select(treeID, tree_height_m) %>% 
    # spatial join with all tree points
    sf::st_join(
      tree_tops %>% 
        dplyr::select(treeID, tree_height_m) %>% 
        dplyr::rename_with(
          .fn = ~ paste0("comp_",.x)
          , .cols = tidyselect::everything()[
              -dplyr::any_of(c("geometry"))
            ]
        )
    ) %>%
    sf::st_drop_geometry() %>% 
    # calculate metrics by treeID
    dplyr::group_by(treeID,tree_height_m) %>% 
    dplyr::summarise(
      n_trees = dplyr::n()
      , max_tree_height_m = max(comp_tree_height_m)
    ) %>% 
    dplyr::ungroup() %>% 
    dplyr::inner_join(
      tree_tops_pct_buffer_temp
      , by = dplyr::join_by("treeID")
    ) %>% 
    dplyr::mutate(
      comp_trees_per_ha = (n_trees/buffer_area_in_study_m2)*10000
      , comp_relative_tree_height = tree_height_m/max_tree_height_m*100
    ) %>% 
    dplyr::select(
      treeID, comp_trees_per_ha, comp_relative_tree_height
    )
  
  ### calculate distance to nearest neighbor
    ### cap distance to nearest tree within xxm buffer
    dist_buffer_temp = 35
    # get trees within radius
    dist_tree_tops_temp = tree_tops %>% 
      dplyr::select(treeID) %>% 
      # buffer point
      sf::st_buffer(dist_buffer_temp) %>% 
      # spatial join with all tree points
      sf::st_join(
        tree_tops %>% 
          dplyr::select(treeID, tree_x, tree_y) %>% 
          dplyr::rename(treeID2=treeID)
      ) %>% 
      dplyr::filter(treeID != treeID2)
    
    # calculate row by row distances 
    dist_tree_tops_temp = dist_tree_tops_temp %>% 
      sf::st_centroid() %>% 
      st_set_geometry("geom1") %>% 
      dplyr::bind_cols(
        dist_tree_tops_temp %>% 
          sf::st_drop_geometry() %>% 
          dplyr::select("tree_x", "tree_y") %>% 
          sf::st_as_sf(coords = c("tree_x", "tree_y"), crs = sf::st_crs(tree_tops)) %>% 
          st_set_geometry("geom2")
      ) %>% 
      dplyr::mutate(
        comp_dist_to_nearest_m = sf::st_distance(geom1, geom2, by_element = T) %>% as.numeric()
      ) %>% 
      sf::st_drop_geometry() %>% 
      dplyr::select(treeID,comp_dist_to_nearest_m) %>% 
      dplyr::group_by(treeID) %>% 
      dplyr::summarise(comp_dist_to_nearest_m = min(comp_dist_to_nearest_m, na.rm = T)) %>% 
      dplyr::ungroup()
  
  # dist_tree_tops_temp = dplyr::tibble(
  #   comp_dist_to_nearest_m = tree_tops %>% 
  #     sf::st_distance(
  #       tree_tops
  #       , by_element = F
  #     ) %>%
  #     # get minimum by row (margin=1) and remove 0 dist for dist to self
  #     apply(MARGIN=1,FUN=function(x){min(ifelse(x==0,NA,x),na.rm = T)})
  # )
  gc()

  ### join with original tree tops data
  tree_tops = tree_tops %>% 
    dplyr::left_join(
      comp_tree_tops_temp
      , by = dplyr::join_by("treeID")
    ) %>% 
    # add distance
    dplyr::left_join(
      dist_tree_tops_temp
      , by = dplyr::join_by("treeID")
    ) %>% 
    dplyr::mutate(comp_dist_to_nearest_m = dplyr::coalesce(comp_dist_to_nearest_m,dist_buffer_temp))
  
  ### Write the trees to the disk
    sf::st_write(
      tree_tops
      , paste0(config$delivery_dir, "/top_down_detected_tree_tops.gpkg")
      , quiet = TRUE, append = FALSE
    )

  # clean up
  remove(list = ls()[grep("_temp",ls())])
  gc()
# start time
  xx10_estdbh_start_time = Sys.time()
#################################################################################
#################################################################################
# Spatial join the tree tops with the tree crowns
#################################################################################
#################################################################################
  ### Convert crown raster to polygons, then to Sf
    crowns_sf = crowns %>% 
      # convert raster to polygons for each individual crown
      terra::as.polygons() %>% 
      # fix polygon validity
      terra::makeValid() %>% 
      # reduce the number of nodes in geometries
      terra::simplifyGeom() %>% 
      # remove holes in polygons
      terra::fillHoles() %>% 
      # convert to sf
      sf::st_as_sf() %>% 
      dplyr::rename(layer = 1) %>% 
      # get the crown area
      dplyr::mutate(
        crown_area_m2 = as.numeric(sf::st_area(.))
      ) %>% 
      #remove super small crowns
      dplyr::filter(
        crown_area_m2 > 0.1
      )
    
  # str(crowns_sf)
  # ggplot(crowns_sf) + geom_sf(aes(fill=as.factor(layer)),color=NA) +
  #   scale_fill_manual(values = viridis::turbo(nrow(crowns_sf)) %>% sample()) +
  #   theme_void() + theme(legend.position = "none")
  # 
  ### Join the crowns with the tree tops to append data, remove Nulls
  crowns_sf = crowns_sf %>%
    sf::st_join(tree_tops) %>% 
    dplyr::group_by(layer) %>% 
    dplyr::mutate(n_trees = dplyr::n()) %>% 
    dplyr::group_by(treeID) %>% 
    dplyr::mutate(n_crowns = dplyr::n()) %>% 
    dplyr::ungroup() %>% 
    dplyr::filter(
      n_trees==1 
      | (n_trees>1 & n_crowns==1) # keeps the treeID that only has one crown if multiple trees to one crown
    ) %>% 
    dplyr::select(c(
      "treeID", "tree_height_m"
      , "tree_x", "tree_y"
      , "crown_area_m2"
      , tidyselect::starts_with("comp_")
    ))
  # str(crowns_sf)
  
  ### Add crown data summaries
  crown_sum_temp = data.frame(
      mean_crown_ht_m = terra::extract(x = chm_rast, y = terra::vect(crowns_sf), fun = "mean", na.rm = T)
      , median_crown_ht_m = terra::extract(x = chm_rast, y = terra::vect(crowns_sf), fun = "median", na.rm = T)
      , min_crown_ht_m = terra::extract(x = chm_rast, y = terra::vect(crowns_sf), fun = "min", na.rm = T)
    ) %>% 
    dplyr::select(-c(tidyselect::ends_with(".ID"))) %>% 
    dplyr::rename_with(~ stringr::str_remove_all(.x,".Z")) %>% 
    dplyr::rename_with(~ stringr::str_remove_all(.x,".focal_mean"))
  
  ### join crown data summary
  crowns_sf = crowns_sf %>% 
    dplyr::bind_cols(crown_sum_temp)
  # str(crowns_sf)
  
  ### Write the crowns to the disk
    sf::st_write(
      crowns_sf
      , paste0(config$delivery_dir, "/top_down_detected_crowns.gpkg")
      , quiet = TRUE, append = FALSE
    )
  
remove(list = ls()[grep("_temp",ls())])
gc()

#################################################################################
#################################################################################
# Model Missing DBH's
#################################################################################
#################################################################################
  # This section uses the sample of DBH values extracted from the point cloud 
   # in the Detect Stems section to create a model using the SfM extracted height, 
    # crown area, and competition metrics as independent variables, 
    # and the SfM-derived DBH as the dependent variable.

  ## Join CHM derived Crowns with DBH stems

  ###________________________________________________________###
  ### Join the Top down crowns with the stem location points ###
  ###________________________________________________________###
    ### Join the top down crowns with the stem location points
    ## !! Note that one crown can have multiple stems within its bounds
    if(dplyr::coalesce(nrow(dbh_locations_sf),0)>0){
      crowns_sf_joined_stems_temp = crowns_sf %>%
        sf::st_join(
          dbh_locations_sf %>% 
            # rename all columns to have "stem" prefix
            dplyr::rename_with(
              .fn = ~ paste0("stem_",.x,recycle0 = T)
              , .cols = tidyselect::everything()[
                -dplyr::any_of(
                  c(tidyselect::starts_with("stem_"),"stem_x", "stem_y","geom","geometry")
                )
              ]
            )
        )
    }else{
      crowns_sf_joined_stems_temp = crowns_sf %>% dplyr::mutate(stem_dbh_cm = as.numeric(NA))
    }
    # str(crowns_sf_joined_stems_temp)
  ###________________________________________________________###
  ## Filter the SfM DBHs
  ###________________________________________________________###

    # 1) Predict an expected DBH value for each [CHM derived tree height](#chm_tree_detect) based on the regional model
    # 2) Remove stem DBH estimates that are outside the 90% prediction bounds
    # 3) Select the stem DBH estimate that is closest to the predicted DBH value (from 1) if multiple stems are within the bounds of one crown
    # 4) Use the SfM-detected stems remaining after this filtering workflow as the training data in the local DBH to height allometric relationship model

    ### Representative FIA Plots and Data

    ###__________________________________________________###
    ### read in FIA data ###
    ###__________________________________________________###
    # read in treemap data
    # downloaded from: https://www.fs.usda.gov/rds/archive/Catalog/RDS-2021-0074
    # read in treemap (no memory is taken)
    treemap_rast = terra::rast(paste0(input_treemap_dir, "/TreeMap2016.tif"))
    
    ### filter treemap based on las...rast now in memory
    treemap_rast = treemap_rast %>% 
      terra::crop(
        las_ctg@data$geometry %>% 
          sf::st_union() %>% 
          terra::vect() %>% 
          terra::project(terra::crs(treemap_rast))
      ) %>% 
      terra::mask(
        las_ctg@data$geometry %>% 
          sf::st_union() %>% 
          terra::vect() %>% 
          terra::project(terra::crs(treemap_rast))
      )
    
    # ggplot(treemap_rast %>% as.data.frame(xy=T) %>% rename(f=3)) +
    #   geom_tile(aes(x=x,y=y,fill=as.factor(f))) +
    #   scale_fill_viridis_d(option = "turbo") +
    #   theme_light() + theme(legend.position = "none")
    
    ### get weights for weighting each tree in the population models
    # treemap id = tm_id for linking to tabular data 
    tm_id_weight_temp = terra::freq(treemap_rast) %>%
      dplyr::select(-layer) %>% 
      dplyr::rename(tm_id = value, tree_weight = count) %>% 
      dplyr::mutate(tm_id = as.character(tm_id))
    # str(tm_id_weight_temp)

    ### get the TreeMap FIA tree list for only the plots included
    treemap_trees_df = readr::read_csv(
        paste0(input_treemap_dir, "/TreeMap2016_tree_table.csv")
        , col_select = c(
          tm_id
          , CN
          , TREE
          , STATUSCD
          , DIA
          , HT
        )
      ) %>% 
      dplyr::rename_with(tolower) %>% 
      dplyr::mutate(
        cn = as.character(cn)
        , tm_id = as.character(tm_id)
      ) %>% 
      dplyr::left_join(
        tm_id_weight_temp
        , by = dplyr::join_by("tm_id")
      ) %>% 
      dplyr::left_join(
        tm_id_weight_temp %>% dplyr::rename(cn = tm_id)
        , by = dplyr::join_by("cn")
      ) %>% 
      dplyr::mutate(tree_weight = dplyr::coalesce(tree_weight.x, tree_weight.y)) %>% 
      dplyr::select(-c(tree_weight.x, tree_weight.y)) %>% 
      dplyr::filter(
        # keep live trees only: 1=live;2=dead
        statuscd == 1
        & !is.na(dia) 
        & !is.na(ht) 
        & !is.na(tree_weight)
      ) %>%
      dplyr::mutate(
        dbh_cm = dia*2.54
        , tree_height_m = ht/3.28084
      ) %>% 
      dplyr::select(-c(statuscd,dia,ht)) %>% 
      dplyr::rename(tree_id=tree)  

    ###__________________________________________________________###
    ### Regional model of DBH as predicted by height 
    ### population model of dbh on height, non-linear
    ### used to filter sfm dbhs
    ###__________________________________________________________###
    gc()
    # population model with no random effects (i.e. no group-level variation)
    # non-linear model form with Gamma distribution for strictly positive response variable dbh
    # set up prior
    p_temp <- prior(normal(1, 2), nlpar = "b1") +
      prior(normal(0, 2), nlpar = "b2")
    mod_nl_pop = brms::brm(
      formula = brms::bf(
        formula = dbh_cm|weights(tree_weight) ~ (b1 * tree_height_m) + tree_height_m^b2
        , b1 + b2 ~ 1
        , nl = TRUE # !! specify non-linear
      )
      , data = treemap_trees_df
      , prior = p_temp
      , family = brms::brmsfamily("Gamma")
      , iter = 4000
    )
    # plot(mod_nl_pop)
    # summary(mod_nl_pop)
    
    ## write out model estimates to tabular file
    #### extract posterior draws to a df
    brms::as_draws_df(
      mod_nl_pop
      , variable = c("^b_", "shape")
      , regex = TRUE
    ) %>% 
      # quick way to get a table of summary statistics and diagnostics
      posterior::summarize_draws(
        "mean", "median", "sd"
        ,  ~quantile(.x, probs = c(
          0.05, 0.95
          , 0.025, 0.975
        ))
        , "rhat"
      ) %>% 
      dplyr::mutate(
        variable = stringr::str_remove_all(variable, "_Intercept")
        , formula = summary(mod_nl_pop)$formula %>% 
          as.character() %>% 
          .[1]
      ) %>% 
      write.csv(
        paste0(config$delivery_dir, "/regional_dbh_height_model_estimates.csv")
        , row.names = F
      )
    
    ### obtain model predictions over range
    # range of x var to predict
    height_range = dplyr::tibble(
      tree_height_m = seq(
        from = 0
        , to = 120 # tallest tree in the world
        , by = 0.1 # by 0.1 m increments
      )
    )
    # predict and put estimates in a data frame
    pred_mod_nl_pop_temp = predict(
      mod_nl_pop
      , newdata = height_range
      , probs = c(.05, .95)
    ) %>%
      dplyr::as_tibble() %>%
      dplyr::rename(
        lower_b = 3, upper_b = 4
      ) %>% 
      dplyr::rename_with(tolower) %>% 
      dplyr::select(-c(est.error)) %>% 
      dplyr::bind_cols(height_range) %>% 
      dplyr::rename(
        tree_height_m_tnth=tree_height_m
        , est_dbh_cm = estimate
        , est_dbh_cm_lower = lower_b
        , est_dbh_cm_upper = upper_b
      ) %>% 
      dplyr::mutate(tree_height_m_tnth=as.character(tree_height_m_tnth)) %>% 
      dplyr::relocate(tree_height_m_tnth)
    # str(pred_mod_nl_pop_temp)
    
    # save predictions for reading later
    write.csv(
      pred_mod_nl_pop_temp
      , file = paste0(config$delivery_dir, "/regional_dbh_height_model_predictions.csv")
      , row.names = F
    )

    ###__________________________________________________________###
    ### Predict and filter SfM-derived DBH
    ###__________________________________________________________###
      # str(crowns_sf_joined_stems_temp)
      # attach allometric data to CHM derived trees and canopy data
      crowns_sf_joined_stems_temp = crowns_sf_joined_stems_temp %>% 
        # join with model predictions at 0.1 m height intervals
        dplyr::mutate(
          tree_height_m_tnth = round(tree_height_m,1) %>% as.character()
        ) %>% 
        dplyr::inner_join(
          pred_mod_nl_pop_temp
          , by = dplyr::join_by(tree_height_m_tnth)  
        ) %>% 
        dplyr::select(-tree_height_m_tnth) %>% 
        dplyr::mutate(
          est_dbh_pct_diff = abs(stem_dbh_cm-est_dbh_cm)/est_dbh_cm
        )
        # what is the estimated difference
        # summary(crowns_sf_joined_stems_temp$est_dbh_pct_diff)
        # crowns_sf_joined_stems_temp %>% dplyr::glimpse()
        # crowns_sf_joined_stems_temp %>% dplyr::filter(!is.na(stem_dbh_cm)) %>% nrow()
      
      ### build training data set by filtering stems
        dbh_training_data_temp = crowns_sf_joined_stems_temp %>%
          sf::st_drop_geometry() %>% 
          dplyr::filter(
            !is.na(stem_dbh_cm)
            & stem_dbh_cm > 0
            & stem_dbh_cm >= est_dbh_cm_lower
            & stem_dbh_cm <= est_dbh_cm_upper
          ) %>% 
          dplyr::group_by(treeID) %>% 
          # select the minimum difference to regional dbh estimate
          dplyr::filter(
            est_dbh_pct_diff==min(est_dbh_pct_diff)
          ) %>% 
          # just take one if same dbh
          dplyr::filter(
            dplyr::row_number()==1
          ) %>% 
          dplyr::ungroup() %>% 
          dplyr::select(c(
            treeID # id
            , stem_dbh_cm # y
            # x vars
            , tree_height_m
            , crown_area_m2
            , min_crown_ht_m
            , tidyselect::starts_with("comp_")
          ))
      # dbh_training_data_temp %>% dplyr::glimpse()
      if(nrow(dbh_training_data_temp)>10){
        ###__________________________________________________________###
        ### Build local model to estimate missing DBHs using SfM DBHs
        ###__________________________________________________________###
        # Use the SfM-detected stems remaining after the filtering workflow 
        # for the local DBH to height allometric relationship model.
        if(local_dbh_model == "rf"){
          # set random seed
          set.seed(21)
          
          ### tuning RF model
          # If we are interested with just starting out and tuning the mtry parameter 
          # we can use randomForest::tuneRF for a quick and easy tuning assessment. 
          # tuneRf will start at a value of mtry that you supply and increase by a 
          # certain step factor until the OOB error stops improving be a specified amount.
          rf_tune_temp = randomForest::tuneRF(
            y = dbh_training_data_temp$stem_dbh_cm
            , x = dbh_training_data_temp %>% dplyr::select(-c(treeID,stem_dbh_cm))
            , stepFactor = 0.5
            , ntreeTry = 500
            , mtryStart = 0.5
            , improve = 0.01
            , plot = F
            , trace = F
          )
          # rf_tune_temp
          
          ### Run a randomForest model to predict DBH using various crown predictors
          stem_prediction_model = randomForest::randomForest(
            y = dbh_training_data_temp$stem_dbh_cm
            , x = dbh_training_data_temp %>% dplyr::select(-c(treeID,stem_dbh_cm))
            , mtry = rf_tune_temp %>% 
              dplyr::as_tibble() %>% 
              dplyr::filter(OOBError==min(OOBError)) %>% 
              dplyr::pull(mtry)
            , na.action = na.omit
          )
          # stem_prediction_model
          # str(stem_prediction_model)
          
          # # variable importance plot
          #   randomForest::varImpPlot(stem_prediction_model, main = "RF variable importance plot for DBH estimate")
          
          ## Estimated versus observed DBH
          # data.frame(
          #   dbh_training_data_temp
          #   , predicted = stem_prediction_model$predicted
          # ) %>% 
          # ggplot() +
          #   geom_abline() +
          #   geom_point(mapping = aes(x = stem_dbh_cm, y = predicted)) +
          #   scale_x_continuous(limits = c(0,max(dbh_training_data_temp$stem_dbh_cm)*1.05)) +
          #   scale_y_continuous(limits = c(0,max(dbh_training_data_temp$stem_dbh_cm)*1.05)) +
          #   labs(
          #     x = "SfM DBH (cm)"
          #     , y = "Predicted DBH (cm) by RF"
          #   ) +
          #   theme_light()
            
        }else{
          # population model with no random effects (i.e. no group-level variation)
          # Gamma distribution for strictly positive response variable dbh
          stem_prediction_model = brms::brm(
            formula = stem_dbh_cm ~ 1 + tree_height_m
            , data = dbh_training_data_temp %>% 
                dplyr::select(stem_dbh_cm, tree_height_m)
            , family = brms::brmsfamily("Gamma", link = "log")
            , prior = c(prior(gamma(0.01, 0.01), class = shape))
            , iter = 4000
            # , file = "../data/mod_lin"
          )
        }
        
        ###___________________________________________________________________###
        ### Predict missing DBH values for the top down crowns with no DBH ###
        ###___________________________________________________________________###
        # nrow(dbh_training_data_temp)
        # nrow(crowns_sf)
        crowns_sf_predict_only_temp = crowns_sf %>%
          sf::st_drop_geometry() %>% 
          dplyr::anti_join(
            dbh_training_data_temp %>% 
              dplyr::select(treeID)
            , by = dplyr::join_by("treeID")
          ) %>% 
          dplyr::select(
            dbh_training_data_temp %>% dplyr::select(-c(stem_dbh_cm)) %>% names()
          )
        # str(crowns_sf_predict_only_temp)
        
        # get predicted dbh
        predicted_dbh_cm_temp = predict(
          stem_prediction_model
          , crowns_sf_predict_only_temp %>% dplyr::select(-treeID)
        ) %>% 
        dplyr::as_tibble() %>% 
        dplyr::pull(1)
        # summary(predicted_dbh_cm_temp)
        
        ## combine predicted data with training data for full data set for all tree crowns with a matched tree top
        # nrow(crowns_sf)
        crowns_sf_with_dbh = crowns_sf %>%
          # join with regional model predictions at 0.1 m height intervals
          dplyr::mutate(
            tree_height_m_tnth = round(tree_height_m,1) %>% as.character()
          ) %>% 
          dplyr::inner_join(
            pred_mod_nl_pop_temp %>% 
              dplyr::rename(
                reg_est_dbh_cm = est_dbh_cm
                , reg_est_dbh_cm_lower = est_dbh_cm_lower
                , reg_est_dbh_cm_upper = est_dbh_cm_upper
              )
            , by = dplyr::join_by(tree_height_m_tnth)  
          ) %>% 
          dplyr::select(-tree_height_m_tnth) %>%
          # join training data
          dplyr::left_join(
            dbh_training_data_temp %>% 
              dplyr::mutate(is_training_data = T) %>% 
              dplyr::select(treeID, is_training_data, stem_dbh_cm)
            , by = dplyr::join_by("treeID")
          ) %>% 
          # join with predicted data estimates
          dplyr::left_join(
            crowns_sf_predict_only_temp %>% 
              dplyr::mutate(
                predicted_dbh_cm = predicted_dbh_cm_temp
              ) %>% 
              dplyr::select(treeID, predicted_dbh_cm)
            , by = dplyr::join_by("treeID")
          ) %>% 
          # clean up data and calculate metrics from dbh
          dplyr::mutate(
            is_training_data = dplyr::coalesce(is_training_data,F)
            , dbh_cm = dplyr::coalesce(stem_dbh_cm, predicted_dbh_cm, reg_est_dbh_cm)
            , dbh_m = dbh_cm/100
            , radius_m = dbh_m/2
            , basal_area_m2 = pi * (radius_m)^2
            , basal_area_ft2 = basal_area_m2 * 10.764
          ) %>% 
          dplyr::select(-c(stem_dbh_cm, predicted_dbh_cm))
        
        # nrow(crowns_sf_with_dbh)
        # nrow(crowns_sf)
        # nrow(tree_tops)
        
      }else{ # if(nrow(dbh_training_data_temp)>10)
        ## combine predicted data with training data for full data set for all tree crowns with a matched tree top
        # nrow(crowns_sf)
        crowns_sf_with_dbh = crowns_sf %>%
          # join with regional model predictions at 0.1 m height intervals
          dplyr::mutate(
            tree_height_m_tnth = round(tree_height_m,1) %>% as.character()
          ) %>% 
          dplyr::inner_join(
            pred_mod_nl_pop_temp %>% 
              dplyr::rename(
                reg_est_dbh_cm = est_dbh_cm
                , reg_est_dbh_cm_lower = est_dbh_cm_lower
                , reg_est_dbh_cm_upper = est_dbh_cm_upper
              )
            , by = dplyr::join_by(tree_height_m_tnth)  
          ) %>% 
          dplyr::select(-tree_height_m_tnth) %>% 
          # join training data
          dplyr::mutate(
            is_training_data = F
            , stem_dbh_cm = as.numeric(NA)
            , predicted_dbh_cm = as.numeric(NA)
          ) %>% 
          # clean up data and calculate metrics from dbh
          dplyr::mutate(
            is_training_data = dplyr::coalesce(is_training_data,F)
            , dbh_cm = dplyr::coalesce(stem_dbh_cm, predicted_dbh_cm, reg_est_dbh_cm)
            , dbh_m = dbh_cm/100
            , radius_m = dbh_m/2
            , basal_area_m2 = pi * (radius_m)^2
            , basal_area_ft2 = basal_area_m2 * 10.764
          ) %>% 
          dplyr::select(-c(stem_dbh_cm, predicted_dbh_cm))
        
      } # else  
    # ggplot(crowns_sf_with_dbh) + 
    #   geom_sf(aes(fill=dbh_cm), color=NA) +
    #   scale_fill_viridis_c(option="plasma") +
    #   coord_sf(expand = F) +
    #   theme_light()
      
    ### write the data to the disk
      # crown vector polygons
      sf::st_write(
        crowns_sf_with_dbh
        , paste0(config$delivery_dir, "/final_detected_crowns.gpkg")
        , append = FALSE
        , quiet = TRUE
      )
      # tree top vector points
      sf::st_write(
        # get tree points
        crowns_sf_with_dbh %>% 
          sf::st_drop_geometry() %>% 
          sf::st_as_sf(coords = c("tree_x", "tree_y"), crs = sf::st_crs(crowns_sf_with_dbh))
        , paste0(config$delivery_dir, "/final_detected_tree_tops.gpkg")
        , append = FALSE
        , quiet = TRUE
      )

    # clean up
      remove(list = ls()[grep("_temp",ls())])
      gc()
# start time
  xx11_silv_start_time = Sys.time()
#################################################################################
#################################################################################
# Calculate Silviculture Metrics
#################################################################################
#################################################################################
    # Common silvicultural metrics are calculated for the entire extent. 
      # Note, that stand-level summaries can be computed if stand vector data is provided.
      # metrics include:
        # "n_trees"
        # "plot_area_ha"
        # "trees_per_ha"
        # "mean_dbh_cm"
        # "qmd_cm"
        # "mean_tree_height_m"
        # "loreys_height_m"
        # "basal_area_m2"
        # "basal_area_m2_per_ha"
      
    ### stand-level summaries
      silv_metrics_temp = crowns_sf_with_dbh %>%
        sf::st_drop_geometry() %>% 
        dplyr::ungroup() %>% 
        dplyr::mutate(
          plotID = "1" # can spatially join to plot vectors if available
          , plot_area_m2 = las_ctg@data$geometry %>% 
            sf::st_union() %>% 
            sf::st_area() %>% # result is m2
            as.numeric()
          , plot_area_ha = plot_area_m2/10000 # convert to ha
        ) %>% 
        dplyr::group_by(plotID,plot_area_ha) %>% 
        dplyr::summarise(
          n_trees = dplyr::n_distinct(treeID)
          , mean_dbh_cm = mean(dbh_cm, na.rm = T)
          , mean_tree_height_m = mean(tree_height_m, na.rm = T)
          , loreys_height_m = sum(basal_area_m2*tree_height_m, na.rm = T) / sum(basal_area_m2, na.rm = T)
          , basal_area_m2 = sum(basal_area_m2, na.rm = T)
          , sum_dbh_cm_sq = sum(dbh_cm^2, na.rm = T)
        ) %>% 
        dplyr::ungroup() %>% 
        dplyr::mutate(
          trees_per_ha = (n_trees/plot_area_ha)
          , basal_area_m2_per_ha = (basal_area_m2/plot_area_ha)
          , qmd_cm = sqrt(sum_dbh_cm_sq/n_trees)
        ) %>% 
        dplyr::select(-c(sum_dbh_cm_sq)) %>% 
        # convert to imperial units
        dplyr::mutate(
          dplyr::across(
            .cols = tidyselect::ends_with("_cm")
            , ~ .x * 0.394
            , .names = "{.col}_in"
          )
          , dplyr::across(
            .cols = tidyselect::ends_with("_m")
            , ~ .x * 3.28
            , .names = "{.col}_ft"
          )
          , dplyr::across(
            .cols = tidyselect::ends_with("_m2_per_ha")
            , ~ .x * 4.359
            , .names = "{.col}_ftac"
          )
          , dplyr::across(
            .cols = tidyselect::ends_with("_per_ha") & !tidyselect::ends_with("_m2_per_ha")
            , ~ .x * 0.405
            , .names = "{.col}_ac"
          )
          , dplyr::across(
            .cols = tidyselect::ends_with("_area_ha")
            , ~ .x * 2.471
            , .names = "{.col}_ac"
          )
          , dplyr::across(
            .cols = tidyselect::ends_with("_m2")
            , ~ .x * 10.764
            , .names = "{.col}_ft2"
          )
        ) %>% 
        dplyr::rename_with(
          .fn = function(x){dplyr::case_when(
            stringr::str_ends(x,"_cm_in") ~ stringr::str_replace(x,"_cm_in","_in")
            , stringr::str_ends(x,"_m_ft") ~ stringr::str_replace(x,"_m_ft","_ft")
            , stringr::str_ends(x,"_m2_per_ha_ftac") ~ stringr::str_replace(x,"_m2_per_ha_ftac","_ft2_per_ac")
            , stringr::str_ends(x,"_per_ha_ac") ~ stringr::str_replace(x,"_per_ha_ac","_per_ac")
            , stringr::str_ends(x,"_area_ha_ac") ~ stringr::str_replace(x,"_area_ha_ac","_area_ac")
            , stringr::str_ends(x,"_m2_ft2") ~ stringr::str_replace(x,"_m2_ft2","_ft2")
            , TRUE ~ x
          )}
        ) %>% 
        dplyr::select(
          "plotID"
          , "n_trees"
          , "plot_area_ha"
          , "trees_per_ha"
          , "mean_dbh_cm"
          , "qmd_cm"
          , "mean_tree_height_m"
          , "loreys_height_m"
          , "basal_area_m2"
          , "basal_area_m2_per_ha"
          # imperial
          , "plot_area_ac"
          , "trees_per_ac"
          , "mean_dbh_in"
          , "qmd_in"
          , "mean_tree_height_ft"
          , "loreys_height_ft"
          , "basal_area_ft2"
          , "basal_area_ft2_per_ac"
        )
    
    ### export tabular
      write.csv(
          silv_metrics_temp
          , paste0(config$delivery_dir, "/final_plot_silv_metrics.csv")
          , row.names = F
        )
    
    # this would just be a vector file if available
      silv_metrics_temp = las_ctg@data$geometry %>%
        sf::st_union() %>% 
        sf::st_as_sf() %>% 
        dplyr::mutate(
          plotID = "1" # can spatially join to plot vectors if available
        ) %>% 
        # join with plot data data
        dplyr::inner_join(
          silv_metrics_temp
          , by = dplyr::join_by("plotID")
        )
  
    ## summary table
    silv_metrics_temp %>%
      sf::st_drop_geometry() %>%
      dplyr::filter(plotID==silv_metrics_temp$plotID[1]) %>%
      tidyr::pivot_longer(
        cols = -c(plotID)
        , names_to = "measure"
        , values_to = "value"
      ) %>%
      kableExtra::kbl(
        caption = "Silvicultural metrics in both metric and imperial units"
        , digits = 2
      ) %>%
      kableExtra::kable_styling()

#################################################################################
#################################################################################
# clean up
#################################################################################
#################################################################################
    # remove temp files
    if(keep_intermediate_files==F){
      unlink(config$temp_dir, recursive = T)
    }
    # clean up
      remove(list = ls()[grep("_temp",ls())])
      gc()
#################################################################################
#################################################################################
# create data to return
#################################################################################
#################################################################################
xx86_end_time = Sys.time()
  # message
    message(
      "total time was "
      , round(as.numeric(difftime(xx86_end_time, xx1_tile_start_time, units = c("mins"))),2)
      , " minutes to process "
      , scales::comma(sum(las_ctg@data$Number.of.point.records))
      , " points over an area of "
      , scales::comma(as.numeric(las_ctg@data$geometry %>% sf::st_union() %>% sf::st_area())/10000,accuracy = 0.01)
      , " hectares"
    )
  # data
  return_df = 
      # data from las_ctg
      las_ctg@data %>% 
      st_set_geometry("geometry") %>% 
      dplyr::summarise(
        geometry = sf::st_union(geometry)
        , number_of_points = sum(Number.of.point.records, na.rm = T)
      ) %>% 
      dplyr::mutate(
        las_area_m2 = sf::st_area(geometry) %>% as.numeric()
      ) %>% 
      sf::st_drop_geometry() %>% 
      dplyr::mutate(
        timer_tile_time_mins = difftime(xx2_denoise_start_time, xx1_tile_start_time, units = c("mins")) %>% 
          as.numeric()
        , timer_denoise_time_mins = difftime(xx3_classify_start_time, xx2_denoise_start_time, units = c("mins")) %>% 
          as.numeric()
        , timer_classify_time_mins = difftime(xx4_dtm_start_time, xx3_classify_start_time, units = c("mins")) %>% 
          as.numeric()
        , timer_dtm_time_mins = difftime(xx5_normalize_start_time, xx4_dtm_start_time, units = c("mins")) %>% 
          as.numeric()
        , timer_normalize_time_mins = difftime(xx6_chm_start_time, xx5_normalize_start_time, units = c("mins")) %>% 
          as.numeric()
        , timer_chm_time_mins = difftime(xx7_treels_start_time, xx6_chm_start_time, units = c("mins")) %>% 
          as.numeric()
        , timer_treels_time_mins = difftime(xx8_itd_start_time, xx7_treels_start_time, units = c("mins")) %>% 
          as.numeric()
        , timer_itd_time_mins = difftime(xx9_competition_start_time, xx8_itd_start_time, units = c("mins")) %>% 
          as.numeric()
        , timer_competition_time_mins = difftime(xx10_estdbh_start_time, xx9_competition_start_time, units = c("mins")) %>% 
          as.numeric()
        , timer_estdbh_time_mins = difftime(xx11_silv_start_time, xx10_estdbh_start_time, units = c("mins")) %>% 
          as.numeric()
        , timer_silv_time_mins = difftime(xx86_end_time, xx11_silv_start_time, units = c("mins")) %>% 
          as.numeric()
        , timer_total_time_mins = difftime(xx86_end_time, xx1_tile_start_time, units = c("mins")) %>% 
          as.numeric()
        # settings
        , sttng_input_las_dir = input_las_dir
        , sttng_use_parallel_processing = as.character(use_parallel_processing)
        , sttng_desired_chm_res = desired_chm_res
        , sttng_max_height_threshold_m = max_height_threshold_m
        , sttng_minimum_tree_height_m = minimum_tree_height_m
        , sttng_dbh_max_size_m = dbh_max_size_m
        , sttng_local_dbh_model = ifelse(local_dbh_model == "rf", local_dbh_model, "lin")
        , sttng_user_supplied_epsg = as.character(user_supplied_epsg)
        , sttng_las_grid_res_m = las_grid_res_m
        , sttng_las_grid_buff_m = las_grid_buff_m
        , sttng_competition_buffer_m = competition_buffer_m
      )

  # write 
  write.csv(
    return_df
    , paste0(config$delivery_dir, "/processed_tracking_data.csv")
    , row.names = F
  )
    