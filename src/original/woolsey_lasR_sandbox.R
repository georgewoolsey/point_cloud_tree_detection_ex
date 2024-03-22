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

xxst_time = Sys.time()

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
  lasr_denoise_filter = lasR::delete_points(filter = "-drop_noise -drop_duplicates")
  
###################
# dtm
###################
  # file name for write
    dtm_file_name = paste0(config$delivery_dir, "/dtm_", desired_dtm_res, "m.tif")
  # perform Delaunay triangulation
    lasr_triangulate = lasR::triangulate(
      # class 2 = ground; class 9 = water
      filter = lasR::keep_ground() + lasR::keep_class(c(9))
    )
  # rasterize the result of the Delaunay triangulation
    lasr_dtm = lasR::rasterize(res = desired_dtm_res, lasr_triangulate)
  # # Pits and spikes filling for raster with algorithm from St-Onge 2008 (see reference).
    lasr_dtm_pitfill = lasR::pit_fill(raster = lasr_dtm, ofile = dtm_file_name)
    # lasr_dtm_pitfill = lasR::pit_fill(raster = lasr_dtm, ofile = paste0(config$dtm_dir, "/*_dtm.tif"))

###################
# normalize
###################
  # normalize
    lasr_normalize = lasR::transform_with(lasr_triangulate, operator = "-")
  # write
    lasr_write_normalize = lasR::write_las(
      filter = "-drop_z_below 0"
      , ofile = paste0(config$las_normalize_dir, "/*_normalize.las")
      , keep_buffer = F
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
        "-drop_class 2 9 -drop_z_below "
        , minimum_tree_height_m
        , " -drop_z_above "
        , max_height_threshold_m
      )
    )
  # Pits and spikes filling for raster with algorithm from St-Onge 2008 (see reference).
    lasr_chm_pitfill = lasR::pit_fill(raster = lasr_chm, ofile = chm_file_name)
    # lasr_chm_pitfill = lasR::pit_fill(raster = lasr_chm, ofile = paste0(config$chm_dir, "/*_chm.tif"))
  
###################################
# lasR full pipeline
###################################
  if(keep_intermediate_files == T){
    # set up intermediate file write steps
      # classify write step
      lasr_write_classify = lasR::write_las(
          ofile = paste0(config$las_classify_dir, "/*_classify.las")
          , filter = "-drop_noise -drop_duplicates"
          , keep_buffer = F
        )
    # pipeline
      lasr_pipeline_temp = lasr_classify() + lasr_denoise + lasr_denoise_filter +
        lasr_write_classify + 
        lasr_triangulate +
        lasr_dtm + lasr_dtm_pitfill +
        lasr_normalize + lasr_write_normalize +
        lasr_chm + lasr_chm_pitfill
  }else{
    # pipeline
      lasr_pipeline_temp = lasr_classify() + lasr_denoise + lasr_denoise_filter +
        lasr_triangulate + 
        lasr_dtm + lasr_dtm_pitfill +
        lasr_normalize + lasr_write_normalize +
        lasr_chm + lasr_chm_pitfill
  }
  
  # retile catalog to get around lasR not working on overlapping or adjacent tiles
    # create one big a$$ las file even though this is probs bad practice
    # ... it's the only way this lasR workflow works with raw las's that are tiled overlapping or adjacent
  ctg_retile = function(ctg, size, buffer, name = NULL){
    lidR::opt_progress(ctg) = F
    lidR::opt_output_files(ctg) = paste0(config$las_grid_dir,"/", name, "_{XLEFT}_{YBOTTOM}") # label outputs based on coordinates
    lidR::opt_chunk_buffer(ctg) = buffer
    lidR::opt_chunk_size(ctg) = size # retile
    ctg_small = lidR::catalog_retile(ctg) # apply retile  
    return(ctg_small)
  }
  # make this into one big las file
  big_grid = ctg_retile(
    ctg = las_ctg
    , size = las_ctg@data$geometry %>%
        sf::st_union() %>%
        sf::st_bbox() %>%
        sf::st_as_sfc() %>%
        sf::st_area() %>%
        as.numeric() %>% 
        `*`(1.1) %>% 
        round()
    , buffer = 0 # leave this as 0 for srs, processing buffer is set in lasR::exec
    , name = "big_grid"
  )
  # reset grid and buffer to use whatever is passed to lasR::exec
  lidR::opt_chunk_size(big_grid) = 0
  lidR::opt_chunk_buffer(big_grid) = 30
  lidR::opt_progress(big_grid) = T
  
  # execute
    lasr_ans = lasR::exec(
      lasr_pipeline_temp
      , ncores = (parallel::detectCores()-1)
      # , on = list.files(config$input_las_dir, pattern = ".*\\.(laz|las)$", full.names = T)
      # , on = las_ctg
      , on = big_grid
      , buffer = las_grid_buff_m # 10
      , chunk = las_grid_res_m # 100
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
# Height normalize points and create canopy height model (CHM) raster
#################################################################################
#################################################################################
if(
  # do las and lax files already exist?
  length(raw_las_files) != length(las_normalize_flist)
  | length(raw_las_files) != length(normalize_lax_files)
  | (file.exists(paste0(config$delivery_dir, "/chm_", desired_chm_res, "m.tif")) == F)
){
    # note, this section throws the error:
        # ERROR 1: PROJ: proj_create_from_database: Cannot find proj.db
        # no documentation on this error or how to fix...
        # this is caused by lasR::write_las not writing with crs
    
    #set up chm pipeline step
      # operators = "max" is analogous to `lidR::rasterize_canopy(algorithm = p2r())`
      # for each pixel of the output raster the function attributes the height of the highest point found
      lasr_chm_step = lasR::rasterize(
        res = desired_chm_res
        , operators = "max"
        , filter = paste0(
          "-drop_class 2 9 -drop_z_below "
          , minimum_tree_height
          , " -drop_z_above "
          , max_height_threshold
        )
      )
    # Pits and spikes filling for raster with algorithm from St-Onge 2008 (see reference).
      lasr_chm_pit_step = lasR::pit_fill(
        raster = lasr_chm_step
        # , ofile = paste0(config$chm_dir, "/*_chm.tif")
        , ofile = ""
      )
    
    # build and execute lasR::processor
      # these files will be used for detecting tree stem DBH and creating a CHM
    las_norm_chm_flist = lasR::processor(
        lasR::reader(las_classify_flist, filter = "-drop_noise -drop_duplicates") + 
        lasR::normalize(extrabytes = F) +
        lasR::write_las(
          ofile = paste0(config$las_normalize_dir, "/*_normalize.las")
          , filter = "-drop_z_below 0"
        ) + 
        # create chm
        lasr_chm_step +
        # pitfill chm
        lasr_chm_pit_step
      )
    
    # las_norm_chm_flist %>% str()
    # las_norm_chm_flist$rasterize %>% terra::plot()
    
    # create spatial index files (.lax)
    las_normalize_flist = las_norm_chm_flist$write_las
    create_lax_for_tiles(las_normalize_flist)
    
    # extract raster from result
    chm_rast = extract_rast_fn(las_norm_chm_flist)
    # chm_rast %>% terra::plot()

    # fill cells that are missing still with the mean of a window
      chm_rast = chm_rast %>%
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
    # set crs
      terra::crs(chm_rast) = proj_crs
      # chm_rast %>% terra::crs()
      # chm_rast %>% terra::plot()
    
    # write to delivery directory
      terra::writeRaster(
        chm_rast
        , filename = paste0(config$delivery_dir, "/chm_", desired_chm_res, "m.tif")
        , overwrite = T
      )
      
      # paste0(config$delivery_dir, "/chm_", desired_chm_res, "m.tif") %>% 
      #   terra::rast() %>% 
      #   terra::crs()
      #   plot()
    
    # clean up
    remove(list = ls()[grep("_temp",ls())])
    gc()
}else if(file.exists(paste0(config$delivery_dir, "/chm_", desired_chm_res, "m.tif")) == T){
  chm_rast = terra::rast(paste0(config$delivery_dir, "/chm_", desired_chm_res, "m.tif"))
}

#################################################################################
#################################################################################
# normalize in parallel
#################################################################################
#################################################################################

lidR::LAS(tibble(X=as.numeric(NULL),Y=as.numeric(NULL),Z=as.numeric(NULL))) %>% lidR::is.empty()

      las_file_dir = config$las_classify_dir
      dtm = NULL
      out_dir = config$las_stem_dir
    
      ### Get a list of tiled files to ground classify
      lidar_list = list.files(las_file_dir, pattern = ".*\\.(laz|las)$", full.names = F)[1:6]
      
      
      paste0(
          out_dir
          , "/"
          , tools::file_path_sans_ext(lidar_list[1])
          , "_normalize.las"
        )
      
      paste0(
          las_file_dir
          , "/"
          , lidar_list[1]
        )
      ###______________________________________________________________________________________###
      ### In parallel, classify ground, and height normalize across the tiles and rewrite them ###
      ###______________________________________________________________________________________###
    las_normalize_fn <- function(las_file_dir, out_dir, dtm=NULL) {
    
      ### Get a list of tiled files to ground classify
      lidar_list = list.files(las_file_dir, pattern = ".*\\.(laz|las)$", full.names = F)[1:6]
      
      # configure parallel
      cores = parallel::detectCores()
      cluster = parallel::makeCluster(cores)
      # register the parallel backend with the `foreach` package
      doParallel::registerDoParallel(cluster)
      # pass to foreach to process each lidar file in parallel
        # for (i in 1:length(lidar_list)) {
        foreach::foreach(
          i = 1:length(lidar_list)
          ,.packages = c("tools","lidR","tidyverse","doParallel")
        ) %dopar% {
          
          ### Get the desired lidar tile
          des_tile_name = lidar_list[i]
          # des_tile_name
          
          ### Has the file been generated already?
          des_out_tile = paste0(
            out_dir
            , "/"
            , tools::file_path_sans_ext(des_tile_name)
            , "_normalize.las"
          )
          
          does_file_exist = file.exists(des_out_tile)
          
          ### If file exists, skip
          if(does_file_exist == TRUE){
            # message("normalized tile ", des_out_tile, " exists so skipped it ... ")
            return(NULL)
          }
  
          ### If file does not exist height normalize
          if(does_file_exist == FALSE){
  
            ### Read in the lidar tile
              las_tile = lidR::readLAS(paste0(
                las_file_dir
                , "/"
                , des_tile_name
              ))
            ### Height normalize the file
              if(class(dtm) == "SpatRaster"){
                las_tile = lidR::normalize_height(las_tile, algorithm = dtm)
              }else{
                las_tile = lidR::normalize_height(las_tile, algorithm = knnidw())
              }
            ### Remove points below 0.05
              las_tile = lidR::filter_poi(las_tile, Z >= 0)
              
            ### Overwrite the existing file
            if(!is.null(las_tile) & !lidR::is.empty(las_tile)){
              ### Overwrite the existing file
                lidR::writeLAS(
                  las_tile
                  , file = des_out_tile
                )
            }
          }
        } # end foreach
      # turn of parallel cluster
      parallel::stopCluster(cluster)
    }
    
    las_normalize_fn(
      las_file_dir = config$las_classify_dir
      , out_dir = config$las_stem_dir
      , dtm = NULL
    )
    
#################################################################################
#################################################################################
# stem detection
#################################################################################
#################################################################################

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
          lidR::writeLAS(las_reclassified, paste0(config$las_stem_dir, "/", las_name))
          
          ### Write stem polygons to the disk
          out_name = tools::file_path_sans_ext(las_name)
          out_name = paste0(config$stem_poly_tile_dir, "/", out_name, ".parquet")
          sfarrow::st_write_parquet(tree_inv_df, out_name)
          
          return(T)
        }else{return(F)} # tree_map_function() return is LAS
      }else{return(F)} # max_point_height >= min_tree_height
    }else{return(F)} # DOES FILE EXIST == F
  } # write_stem_las_fn
  
  write_stem_las_ans
  #######################################
  ### a parallel version is:
  #######################################
    flist_temp = file.path(
        config$las_normalize_dir
        , c(
          "610758.521_4889129.339_610858.521_4889229.339_tile_denoise_classify_normalize.las"
          , "610358.521_4888929.339_610458.521_4889029.339_tile_denoise_classify_normalize.las"
          , "610558.521_4889129.339_610658.521_4889229.339_tile_denoise_classify_normalize.las"
          , "610458.521_4888229.339_610558.521_4888329.339_tile_denoise_classify_normalize.las"
          , "610858.521_4888529.339_610958.521_4888629.339_tile_denoise_classify_normalize.las"
          , "610858.521_4889229.339_610958.521_4889329.339_tile_denoise_classify_normalize.las"
          , "610958.521_4888429.339_611058.521_4888529.339_tile_denoise_classify_normalize.las"
        )
      )
      # list.files(config$las_normalize_dir, pattern = ".*\\.(laz|las)$", full.names = T)[10:13]
    # configure parallel
    cores = parallel::detectCores()
    cluster = parallel::makeCluster(cores)
    # register the parallel backend with the `foreach` package
    doParallel::registerDoParallel(cluster)
    # pass to foreach to process each lidar file in parallel
      write_stem_las_ans = 
        foreach::foreach(
          i = 1:length(flist_temp)
          , .packages = c("tools","lidR","tidyverse","doParallel","TreeLS")
        ) %dopar% {
          write_stem_las_fn(las_path_name = flist_temp[i], min_tree_height = minimum_tree_height)
        } # end foreach
      # write_stem_las_ans
    # stop parallel
    parallel::stopCluster(cluster)
  #######################################
  ### a parallel version is ^
  #######################################
    
    
    start_time = Sys.time()
      
      # las_name
      
      ### See if the las file has been generated
      path_to_check = paste0(config$las_stem_dir, "/", las_name)
      does_file_exist = file.exists(path_to_check)
      # does_file_exist
      ### See if the vector file has been generated
      path_to_check = paste0(config$stem_poly_tile_dir, "/", tools::file_path_sans_ext(las_name), ".parquet")
      does_file_exist2 = file.exists(path_to_check)
      # does_file_exist2
      
      if(does_file_exist == TRUE & does_file_exist2 == TRUE){
        message("stem detect for grid number ", las_name, " already exists guy ... ")
        return(paste0(difftime(Sys.time(), start_time, units = c("mins")), " minutes"))
      }
      
      ### Read in the desired las file
      las_norm_tile = lidR::readLAS(las_path_name)
      las_norm_tile = lidR::filter_poi(las_norm_tile, Z >= 0)

      # plot(las_norm_tile)
      
      # get the maximum point height
      max_point_height = max(las_norm_tile@data$Z)
      
      ###____________________________________________________________###
      ### If the max point height is below X feet, return classified tile ###
      ###____________________________________________________________###
      
      if(max_point_height < min_tree_height){
        message("No points >",min_tree_height,"m for grid number ", las_name, " so skipped it ... ")
        return(paste0(difftime(Sys.time(), start_time, units = c("mins")), " minutes"))
      }
      
      ###______________________________________________________________###
      ### If the max point height is above X feet, try to detect stems ###
      ###______________________________________________________________###
      
      if(max_point_height >= min_tree_height){
        ###______________________________________________________________###
        ### 1) Apply the `TreeLS::treeMap` [stem detection function](#detect_stem_fn)
        ###______________________________________________________________###
        ### Run the function to search for candidate locations
        treemap_temp = tree_map_function(las_norm_tile)
        
        ### Get a logic check
        check = class(treemap_temp)
        # check
        
        ###_______________________________________________________________###
        ### If the class of the result === Character, then no stems found ###
        ###_______________________________________________________________###
        
        if(check == "character"){
          message("No stems detected for grid number ", las_name, " so skipped it ... ")
          return(paste0(difftime(Sys.time(), start_time, units = c("mins")), " minutes"))
        }
        
        ### If the class of the result == "LAS"
        if(check == "LAS"){
          
          ###___________________________________###
          ### Classify the tree and stem points ###
          ###___________________________________###
          
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
          ###_______________________________________________________###
          ### clean up the DBH stem data frame ###
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
                treeID, H, , stem_x, stem_y, Radius, Error
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
          lidR::writeLAS(las_reclassified, paste0(config$las_stem_dir, "/", las_name))
          message("Wrote stem detect laz for grid number ", las_name, " successfully ... ")
          ### Write stem polygons to the disk
          out_name = tools::file_path_sans_ext(las_name)
          out_name = paste0(config$stem_poly_tile_dir, "/", out_name, ".parquet")
          sfarrow::st_write_parquet(tree_inv_df, out_name)
          message("Wrote stem detect vector data for grid number ", las_name, " successfully ... ")
          
          
        }
      }
    return(paste0(difftime(Sys.time(), start_time, units = c("mins")), " minutes to write"))
  }
  
  # map over the normalized point cloud tiles
    # list.files(config$las_normalize_dir, pattern = ".*\\.(laz|las)$", full.names = T) %>%
    #   purrr::map(write_stem_las_fn, min_tree_height = minimum_tree_height)
    
    