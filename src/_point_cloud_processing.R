#################################################################################
#################################################################################
# Input: raw las/laz point cloud file(s) located in user-specified directory
# 
# Desired outputs covering full extent of all input files:
#   1) Digital Terrain Model (DTM) raster
#   2) Canopy Height Model (CHM) raster
#   3) Tree top locations point vector data
#   4) Tree crown locations polygon vector data
#   5) Plot/stand-level silvicultural metrics tabular data
#################################################################################
#################################################################################
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#################################################################################
#################################################################################
# User-Defined Parameters
#################################################################################
#################################################################################

  ###____________________###
  ### Set directory for outputs ###
  ###____________________###
  # rootdir = "../data"
  rootdir = "../data"
  ###_________________________###
  ### Set input las directory ###
  ###_________________________###
  # !!!!!!!!!! ENSURE FILES ARE PROJECTED IN CRS THAT USES METRE AS MEASURMENT UNIT
  input_las_dir = "../data/big_las_raw"
  # input_las_dir = "../data/small_las_raw"
  ###_________________________###
  ### Set input TreeMap directory ###
  ###_________________________###
  input_treemap_dir = "../data/treemap"
  
  ###_________________________###
  ### Set the desired raster resolution in metres for the canopy height model
  ###_________________________###
  desired_chm_res = 0.25
  
  ###_________________________###
  ### Set the maximum height for the canopy height model
  ###_________________________###
  max_height_threshold = 60
  
  ###_________________________###
  ### Set the minimum height (m) for individual tree detection in `lidR::locate_trees`
  ###_________________________###
  minimum_tree_height = 2
  
  ###_________________________###
  ### Set the maximum dbh size (meters)
  ###_________________________###
  dbh_max_size_m = 1.5

#################################################################################
#################################################################################
# Setup
#################################################################################
#################################################################################
  # bread-and-butter
  library(tidyverse) # the tidyverse
  library(viridis) # viridis colors
  library(scales) # work with number and plot scales
  library(latex2exp) # math formulas with latex
  library(tools) # work with name structures
  
  # spatial analysis
  library(terra) # raster
  library(sf) # simple features
  library(sfarrow) # sf to Apache-Parquet files for working with large files
  
  # point cloud processing
  library(lidR)
  library(ForestTools) # for crown delineation but relies on depreciated `raster`
  library(rlas) # write las index files .lax
  
  ## !! lasR package not available on CRN
    ## uncomment to install from github see: https://r-lidar.github.io/lasR/index.html
    # library(pak)
    # pak::pkg_install("r-lidar/lasR")
    library(lasR) # not available on CRAN as of 2024-01-20
  
  ## lidR::watershed requires EBImage::watershed 
    ## uncomment to install
    # install.packages("BiocManager")
    # library(BiocManager) # required for lidR::watershed
    # BiocManager::install("EBImage")
    library(EBImage) # required for lidR::watershed
  
  ## !! TreeLS package removed from CRAN...
    ## uncomment to install from github dev repo: https://github.com/tiagodc/TreeLS
    # library(pak)
    # pak::pkg_install("tiagodc/TreeLS")
    library(TreeLS) # removed from CRAN

  # modeling
  library(randomForest)
  library(RCSF) # for the cloth simulation filter (csf) to classify points
  library(brms) # bayesian modelling using STAN engine
  
  # parallel computing
  # library(parallel) # parallel
  # library(doParallel)
  # library(foreach) # facilitates parallelization by lapply'ing %dopar% on for loop
  
#################################################################################
#################################################################################
# Configure File Structure
#################################################################################
#################################################################################
  # Use the user-defined directory to create output file structure. 
  ### Function to generate nested project directories
  create_project_structure = function(rootdir,input_las_dir){
    ###___________________________________________________###
    ### Set output delivery directory
    ###___________________________________________________###
    temp_dir = file.path(rootdir, "point_cloud_processing_temp")
    delivery_dir = file.path(rootdir, "point_cloud_processing_delivery")
  
    ### Set output directory for temporary files
    las_grid_dir = file.path(temp_dir, "00_grid")
    las_classify_dir = file.path(temp_dir, "01_classify")
    las_normalize_dir = file.path(temp_dir, "02_normalize")
    dtm_dir = file.path(temp_dir, "03_dtm")
    chm_dir = file.path(temp_dir, "04_chm")
    las_stem_dir = file.path(temp_dir, "05_las_stem")
    stem_poly_tile_dir = file.path(temp_dir, "06_stem_poly_tile")
    
    ### Create the directories
    dir.create(delivery_dir, showWarnings = FALSE)
    dir.create(temp_dir, showWarnings = FALSE)
    dir.create(las_grid_dir, showWarnings = FALSE)
    dir.create(las_classify_dir, showWarnings = FALSE)
    dir.create(las_normalize_dir, showWarnings = FALSE)
    dir.create(dtm_dir, showWarnings = FALSE)
    dir.create(chm_dir, showWarnings = FALSE)
    dir.create(las_stem_dir, showWarnings = FALSE)
    dir.create(stem_poly_tile_dir, showWarnings = FALSE)
    
    ###______________________________###
    ### Set names of the directories ###
    ###______________________________###
    
    names(rootdir) = "rootdir"
    names(input_las_dir) = "input_las_dir"
    names(input_treemap_dir) = "input_treemap_dir"
    names(delivery_dir) = "delivery_dir"
    names(temp_dir) = "temp_dir"
    names(las_grid_dir) = "las_grid_dir"
    names(las_classify_dir) = "las_classify_dir"
    names(las_normalize_dir) = "las_normalize_dir"
    names(dtm_dir) = "dtm_dir"
    names(chm_dir) = "chm_dir"
    names(las_stem_dir) = "las_stem_dir"
    names(stem_poly_tile_dir) = "stem_poly_tile_dir"
    
    ###______________________________###
    ### Append to output config list ###
    ###______________________________###
    
    config = cbind(
      rootdir, input_las_dir, input_treemap_dir
      , delivery_dir, temp_dir, las_grid_dir, las_classify_dir
      , las_normalize_dir, dtm_dir, chm_dir
      , las_stem_dir, stem_poly_tile_dir
    )
    
    config = as.data.frame(config)
    #config
    
    ### Return config 
    return(config)
    
  }
  # call the function
  config = create_project_structure(rootdir, input_las_dir)

#################################################################################
#################################################################################
# Function to extract raster from list for use later
#################################################################################
#################################################################################
  # extract only raster to work with
  extract_rast_fn = function(x) {
    if(class(x) == "SpatRaster"){
      return(x)
    } else if(
        class(x) == "list"
        & length(purrr::keep(x, inherits, "SpatRaster"))==1
      ){
      r = purrr::keep(x, inherits, "SpatRaster") %>% 
        purrr::pluck(1)
      return(r)
    }else{stop("No raster found or >1 raster found")}
  }
#################################################################################
#################################################################################
# Function to create spatial index files (.lax) for classified las
#################################################################################
#################################################################################
  ## see: https://r-lidar.github.io/lidRbook/spatial-indexing.html
  ### Function to generate .lax index files for input directory path
  create_lax_for_tiles = function(las_file_list){
    ans = 
      las_file_list %>% 
      purrr::map(function(des_file){
        ### Compile the .lax file name
        des_file_lax = tools::file_path_sans_ext(des_file)
        des_file_lax = paste0(des_file_lax, ".lax")
        
        ### See if the .lax version exists in the input directory
        does_file_exist = file.exists(des_file_lax)
        # does_file_exist
        
        ### If file does_file_exist, do nothing
        if(does_file_exist == TRUE){return(NULL)}
        
        ### If file doesnt exsist, create a .lax index
        if(does_file_exist == FALSE){rlas::writelax(des_file)}
      })
  }

#################################################################################
#################################################################################
# Tile raw las files to work with smaller chunks
#################################################################################
#################################################################################
  ### point to input las files as a lidR LAScatalog (reads the header of all the LAS files of a given folder)
  las_ctg = lidR::readLAScatalog(config$input_las_dir)
  ### Pull the las extent geometry
  las_grid = las_ctg@data$geometry %>% 
      sf::st_union() %>% 
      sf::st_make_grid(100) %>% 
      sf::st_as_sf() %>% 
      dplyr::mutate(grid_id = dplyr::row_number())
  ### define clip raw las to geometry with lasR pipeline
    lasr_clip_polygon = function(
        geometry, files, buffer, ofile_dir = tempdir()
      ){
        if(sf::st_geometry_type(geometry, FALSE) != "POLYGON"){ stop("Expected POLYGON geometry type")}
        # get bbox of polygon
        bbox = sf::st_bbox(geometry)
        # file name
        fnm = paste0(
          ofile_dir
          , "/"
          , bbox[1]
          , "_", bbox[2]
          , "_", bbox[3]
          , "_", bbox[4]
          , "_tile.las"
        )
        # check file exists
        if(file.exists(fnm)==T){stop("File exists")}
        
        # read las files with buffer step
        read = lasR::reader_rectangles(
          files
          , xmin = bbox[1]
          , ymin = bbox[2]
          , xmax = bbox[3]
          , ymax = bbox[4]
          , filter = ""
          , buffer = buffer
        )
        # write las files step
        stage = lasR::write_las(ofile = fnm, filter = lasR::drop_duplicates(), keep_buffer = T)
        # pass to lasR::processor
        ans = lasR::processor(read+stage)
        return(ans)
      }
    
    # wrap in safe to eat error and continue processing
      safe_lasr_clip_polygon = purrr::safely(lasr_clip_polygon)
    # map function over all geometries
      create_grid_las_list = 
        las_grid %>% 
          sf::st_geometry() %>% 
          purrr::map(safe_lasr_clip_polygon, files = las_ctg, buffer = 10, ofile_dir = config$las_grid_dir)
    # create spatial index files (.lax)
      create_lax_for_tiles(
        las_file_list = list.files(config$las_grid_dir, pattern = ".*\\.(laz|las)$", full.names = T)
      )
    
      # lidR::readLAScatalog(config$las_grid_dir)@data$geometry %>% 
      #   sf::st_as_sf() %>% 
      #   dplyr::mutate(id = dplyr::row_number()) %>% 
      #     ggplot() +
      #       geom_sf(aes(fill=as.factor(id)), alpha = 0.8) +
      #       geom_sf(data = las_grid, color = "black", alpha = 0) +
      #       scale_fill_viridis_d() +
      #       theme_light() + theme(legend.position = "none")
    
    # clean up
      remove(list = ls()[grep("_temp",ls())])
      gc()
      
#################################################################################
#################################################################################
# Set up file names and checks
#################################################################################
#################################################################################
  ###______________________________###
  # set up lasR read file list
  ###______________________________###
    raw_las_files = list.files(config$las_grid_dir, pattern = ".*\\.(laz|las)$", full.names = T)
    # pull crs for using in write operations
    proj_crs = las_ctg %>% sf::st_crs() %>% purrr::pluck(1)
  ###______________________________###
  # check file lists
  ###______________________________###
    # classify
    las_classify_flist = list.files(config$las_classify_dir, pattern = ".*_classify\\.(laz|las)$", full.names = T)
    classify_lax_files = list.files(config$las_classify_dir, pattern = ".*_classify\\.lax$", full.names = T)
    # normalize
    las_normalize_flist = list.files(config$las_normalize_dir, pattern = ".*_normalize\\.(laz|las)$", full.names = T)
    normalize_lax_files = list.files(config$las_normalize_dir, pattern = ".*_normalize\\.lax$", full.names = T)
    # rasters
    dtm_file_name = paste0(config$delivery_dir, "/dtm_1m.tif")
    chm_file_name = paste0(config$delivery_dir, "/chm_", desired_chm_res, "m.tif")

#################################################################################
#################################################################################
# Denoise raw point cloud
#################################################################################
#################################################################################
if(
  # do las and lax files already exist?
  min(stringr::word(basename(raw_las_files),sep = "_tile") %in%
    stringr::word(basename(las_classify_flist),sep = "_tile")) != 1
  | min(stringr::word(basename(raw_las_files),sep = "_tile") %in%
    stringr::word(basename(classify_lax_files),sep = "_tile")) != 1
){
  ###______________________________###
  # denoise with lasR::classify_isolated_points
  # in a lasR pipeline
  ###______________________________###
    # create lasR pipeline to read raw las files and remove noise
      # the function results in denoised las files written to specified directory
    lasr_denoise_pipeline = function(
      files
      , ofile = paste0(tempdir(), "/*_denoise.las")
    ){
      lasR::processor(
        lasR::reader(files) + 
        lasR::classify_isolated_points(res =  5, n = 6) +  
        lasR::write_las(ofile = ofile, filter = "-drop_noise -drop_duplicates")
      )
    }
    
    # call the function and store outfile list to variable
      # these files will be used for creating the classified las
    las_denoise_flist = lasr_denoise_pipeline(
      files = raw_las_files
      # , ofile = paste0(config$temp_dir, "/*_denoise.las")
    )
    
    # las_denoise_flist
    # lidR::readLAS(las_denoise_flist[1]) %>% plot()
    # lidR::readLAS(las_denoise_flist[1])@data %>% dplyr::count(Classification)
    
    # clean up
    remove(list = ls()[grep("_temp",ls())])
    gc()
}
#################################################################################
#################################################################################
# Classify ground points
#################################################################################
#################################################################################
if(
  # do las and lax files already exist?
  min(stringr::word(basename(raw_las_files),sep = "_tile") %in%
    stringr::word(basename(las_classify_flist),sep = "_tile")) != 1
  | min(stringr::word(basename(raw_las_files),sep = "_tile") %in%
    stringr::word(basename(classify_lax_files),sep = "_tile")) != 1
){
  ###______________________________###
  # classify ground points
  ###______________________________###
    # There is no function in lasR to classify the points...create one
      # the function results in classified las files written to specified directory
    lasr_classify_pipeline = function(
      files
      , ofile = paste0(tempdir(), "/*_classify.las")
      # csf parameters
      , smooth = FALSE
      , threshold = 0.5
      , resolution = 0.5
      , rigidness = 1L
      , iterations = 500L
      , step = 0.65
    ){
      # pass parmeters to the RCSF::CSF algorithm
      csf = function(data, smooth, threshold, resolution, rigidness, iterations, step){
        id = RCSF::CSF(data, smooth, threshold, resolution, rigidness, iterations, step)
        class = integer(nrow(data))
        class[id] = 2L
        data$Classification <- class
        return(data)
      }
      # wrap in lasR::callback
        # if the output is a data.frame with the same number of points, it updates the point cloud
      classify = lasR::callback(
        csf
        , expose = "xyz"
        , smooth = smooth, threshold = threshold
        , resolution = resolution, rigidness = rigidness
        , iterations = iterations, step = step
      )
      # define pipeline
      pipeline = lasR::reader(files, filter = "-drop_noise -drop_duplicates") + 
        classify +
        lasR::write_las(ofile = ofile)
      # pass to lasR::processor or return
      lasR::processor(pipeline)
      # return(pipeline)
    }
    
    # call the function and store outfile list to variable
      # these files will be used for creating the DTM and height normalizing
    las_classify_flist = lasr_classify_pipeline(
      files = las_denoise_flist
      , ofile = paste0(config$las_classify_dir, "/*_classify.las")
    )
    
    # create spatial index files (.lax)
    create_lax_for_tiles(las_classify_flist)
    
    # las_classify_flist
    # lidR::readLAS(las_classify_flist[1]) %>% plot(color = "Classification")
    # lidR::readLAS(las_classify_flist[1])@data %>% dplyr::count(Classification)
    
    # clean up
    remove(list = ls()[grep("_temp",ls())])
    gc()
}
#################################################################################
#################################################################################
# Create digital terrain model (DTM) raster
#################################################################################
#################################################################################
if(
  file.exists(dtm_file_name) == F
){
  ###______________________________###
  # create DTM raster using Delaunay triangulation and pit fill
  ###______________________________###
    # the function results in pitfilled dtm files written to specified directory and...
      # a mosaic'd dtm for all files included
      # smooth the mosaic'd dtm to fill na's and write to delivery directory with crs
      
      # note, this section throws the error:
        # ERROR 1: PROJ: proj_create_from_database: Cannot find proj.db
        # no documentation on this error or how to fix...
        # this is caused by lasR::write_las not writing with crs
        # this script attaches the crs when creating a rater mosaic of entire extent
    lasr_dtm_pipeline = function(
      files
      , ofile = paste0(tempdir(), "/*_dtm_1m.tif")
      # dtm parameters
      , res = 1
      , max_edge = 0
      , add_class = NULL
    ){
      # set up filter
      filter = lasR::keep_ground()
      if (!is.null(add_class)) filter = filter + lasR::keep_class(add_class)
      # Delaunay triangulation
      tri = lasR::triangulate(max_edge = max_edge, filter = filter)
      # rasterize the result of the Delaunay triangulation
      rast = lasR::rasterize(res = res, tri)
      # Pits and spikes filling for raster with algorithm from St-Onge 2008 (see reference).
      pit = lasR::pit_fill(raster = rast, ofile = ofile)
      # define pipeline
      pipeline = tri + rast + pit
      # pass to lasR::processor or return
      ans = lasR::processor(lasR::reader(files, filter = filter) + pipeline)
      return(ans)
      
    }
    
    # call the function and store outfile list to variable
      # these files will be used to mosaic over full extent
    las_dtm_flist = lasr_dtm_pipeline(
      files = las_classify_flist
      # , ofile = paste0(config$dtm_dir, "/*_dtm_1m.tif")
      , ofile = ""
      , res = 1
      , max_edge = c(0,1)
      , add_class = 9 # include water in dtm
    )
    
    # extract raster from result
    dtm_rast = extract_rast_fn(las_dtm_flist)
    # dtm_rast %>% terra::plot()

    # fill cells that are missing still with the mean of a window
      dtm_rast = dtm_rast %>%
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
      terra::crs(dtm_rast) = proj_crs
      # dtm_rast %>% terra::crs()
      # dtm_rast %>% terra::plot()
    
    # write to delivery directory
      terra::writeRaster(
        dtm_rast
        , filename = dtm_file_name
        , overwrite = T
      )
      
      # dtm_file_name %>% 
      #   terra::rast() %>% 
      #   terra::crs()
      #   plot()
    
    # clean up
    remove(list = ls()[grep("_temp",ls())])
    remove(las_dtm_flist)
    gc()
}else if(file.exists(dtm_file_name) == T){
  dtm_rast = terra::rast(dtm_file_name)
}
#################################################################################
#################################################################################
# Height normalize points 
#################################################################################
#################################################################################
if(
  # do las and lax files already exist?
  min(stringr::word(basename(raw_las_files),sep = "_tile") %in%
    stringr::word(basename(las_nomralize_flist),sep = "_tile")) != 1
  | min(stringr::word(basename(raw_las_files),sep = "_tile") %in%
    stringr::word(basename(normalize_lax_files),sep = "_tile")) != 1
){
  
  # set up las ctg
  las_classify_ctg = lidR::readLAScatalog(las_classify_flist)
  # redirect results to output 
  opt_output_files(las_classify_ctg) = paste0(config$las_normalize_dir, "/{*}_normalize")
  # turn off early exit
  opt_stop_early(las_classify_ctg) = FALSE
  # normalize
  las_normalize_ans = lidR::normalize_height(las_classify_ctg, algorithm = knnidw())
  gc()
  
  # get file list
  las_normalize_flist = list.files(config$las_normalize_dir, pattern = ".*\\.(laz|las)$", full.names = T)
  
  # create spatial index files (.lax)
  create_lax_for_tiles(las_normalize_flist)
  
  # list.files(las_normalize_flist)[1] %>% 
  #   lidR::readLAS() %>% 
  #   plot()

  # clean up
    remove(list = ls()[grep("_temp",ls())])
    gc()
}

#################################################################################
#################################################################################
# Create canopy height model (CHM) raster in lasR pipeline
#################################################################################
#################################################################################
if(
  file.exists(chm_file_name) == F
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
      # these files will be used for detecting tree tops and tree crowns
    las_chm_flist = lasR::processor(
        lasR::reader(las_normalize_flist, filter = "-drop_z_below 0") + 
        # create chm
        lasr_chm_step +
        # pitfill chm
        lasr_chm_pit_step
      )
    
    # extract raster from result
    chm_rast = extract_rast_fn(las_chm_flist)
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
        , filename = chm_file_name
        , overwrite = T
      )
      
      # chm_file_name %>% 
      #   terra::rast() %>% 
      #   terra::crs()
      #   plot()
    
    # clean up
    remove(list = ls()[grep("_temp",ls())])
    remove(list = ls()[grep("_step",ls())])
    remove(las_chm_flist)
    gc()
}else if(file.exists(chm_file_name) == T){
  chm_rast = terra::rast(chm_file_name)
}

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
  write_stem_las_fn <- function(las_path_name) {
      ### Get the desired las file
      las_name = basename(las_path_name)
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
        return(FALSE)
      }
      
      ### Read in the desired las file
      las_norm_tile = lidR::readLAS(las_path_name)
      las_norm_tile = lidR::filter_duplicates(las_norm_tile)
      # plot(las_norm_tile)
      
      # get the maximum point height
      max_point_height = max(las_norm_tile@data$Z)
      
      ###____________________________________________________________###
      ### If the max point height is below X feet, return classified tile ###
      ###____________________________________________________________###
      
      if(max_point_height < 2){
          message("No points >2m for grid number ", las_name, " so skipped it ... ")
        return(FALSE)
      }
      
      ###______________________________________________________________###
      ### If the max point height is above X feet, try to detect stems ###
      ###______________________________________________________________###
      
      if(max_point_height >= 2){
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
          return(FALSE)
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
          
          # return(las_norm_tile)
          return(TRUE)
        }
      }
  }
  
  # map over the normalized point cloud tiles
    list.files(config$las_normalize_dir, pattern = ".*\\.(laz|las)$", full.names = T) %>%
      purrr::map(write_stem_las_fn)
  
  # get file list
  las_stem_flist = list.files(config$las_stem_dir, pattern = ".*\\.(laz|las)$", full.names = T)
  
  # create spatial index files (.lax)
  create_lax_for_tiles(las_stem_flist)
  
  
