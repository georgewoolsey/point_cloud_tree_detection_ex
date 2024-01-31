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
  ### this process writes intermediate data to the disk
  ### keep those intermediate files (classfied, normalized, stem las files)
  ###____________________###
  keep_intermediate_files = F
  
  ###____________________###
  ### use parallel processing? (T/F) ###
  ### parallel processing may not work on all machines ###
  ###____________________###
  use_parallel_processing = T
  
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
  dbh_max_size_m = 1
  
#################################################################################
#################################################################################
# Setup
#################################################################################
#################################################################################
  full_start_time = Sys.time()
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
  library(raster) # for ForestTools crown delineation but depreciated 2023-10
  
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
    # library(EBImage) # required for lidR::watershed
  
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
  library(parallel) # parallel
  library(doParallel)
  library(foreach) # facilitates parallelization by lapply'ing %dopar% on for loop
  
  ##########
  # custom
  ##########
  # check_ls_size_fn = function(ls) {
  #    ls %>%
  #     purrr::map(function(x){
  #       dplyr::tibble(
  #         nm = x
  #         , size = object.size(get(x))
  #       )
  #     }) %>%
  #     dplyr::bind_rows() %>%
  #     dplyr::arrange(desc(size))
  # }
  # check_ls_size_fn(ls())
  
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
    temp_dir = file.path(normalizePath(rootdir), "point_cloud_processing_temp")
    delivery_dir = file.path(normalizePath(rootdir), "point_cloud_processing_delivery")
  
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
  # create spatial index files (.lax)
    create_lax_for_tiles(
      las_file_list = list.files(config$input_las_dir, pattern = ".*\\.(laz|las)$", full.names = T)
    )
  ### point to input las files as a lidR LAScatalog (reads the header of all the LAS files of a given folder)
    las_ctg = lidR::readLAScatalog(config$input_las_dir)
  
  ###______________________________###
  # write las coverage data to delivery
  ###______________________________###
    sf::st_write(
      las_ctg@data
      , paste0(config$delivery_dir, "/raw_las_ctg_info.gpkg")
      , quiet = TRUE, append = FALSE
    )
  
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
# Set up file names and proj epsg
#################################################################################
#################################################################################
  ###______________________________###
  # set up lasR read file list
  ###______________________________###
    raw_las_files = list.files(config$las_grid_dir, pattern = ".*\\.(laz|las)$", full.names = T)
    
    # pull crs for using in write operations
      crs_list_temp = las_ctg@data$CRS
      if(length(unique(crs_list_temp))>1){stop("the raw las files have multiple CRS settings")}else{
        proj_crs = paste0("EPSG:",unique(crs_list_temp))
      }
      
    #switch to overwrite rasters if new data is created 
      # (leave as F here even if first time executing)
    overwrite_raster = F
  
  # clean up from setup to free some memory
    # check_ls_size_fn(ls())
    remove(
      las_ctg, create_project_structure, create_grid_las_list
      , lasr_clip_polygon, safe_lasr_clip_polygon, las_grid
    )
  
#################################################################################
#################################################################################
# Denoise raw point cloud
#################################################################################
#################################################################################
###______________________________###
# check file lists
###______________________________###
  # classify
  las_classify_flist = list.files(config$las_classify_dir, pattern = ".*_classify\\.(laz|las)$", full.names = T)
  classify_lax_files = list.files(config$las_classify_dir, pattern = ".*_classify\\.lax$", full.names = T)
# execute if
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
    # select raw files which do not have classified files
    flist_temp = raw_las_files[which(
      !stringr::word(basename(raw_las_files),sep = "_tile") %in%
      stringr::word(basename(las_classify_flist),sep = "_tile")
    )]
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
      files = flist_temp
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
###______________________________###
# check file lists
###______________________________###
  # classify
  las_classify_flist = list.files(config$las_classify_dir, pattern = ".*_classify\\.(laz|las)$", full.names = T)
  classify_lax_files = list.files(config$las_classify_dir, pattern = ".*_classify\\.lax$", full.names = T)
# execute if
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
    
    #switch to overwrite rasters since this section created new data
    overwrite_raster = T
}
#################################################################################
#################################################################################
# Create digital terrain model (DTM) raster
#################################################################################
#################################################################################
###______________________________###
# check file lists
###______________________________###
  # rasters
  dtm_file_name = paste0(config$delivery_dir, "/dtm_1m.tif")
# execute if
if(
  file.exists(dtm_file_name) == F
  | overwrite_raster == T
){
  ###______________________________###
  # create DTM raster using Delaunay triangulation and pit fill
  ###______________________________###
    # the function results in pitfilled dtm files written to specified directory and...
      # a mosaic'd dtm for all files included
      # smooth the mosaic'd dtm to fill na's and write to delivery directory with crs
      
    # note, this section throws the MESSAGE:
        # ERROR 1: PROJ: proj_create_from_database: Cannot find proj.db
        # no documentation on this error or how to fix...
        # this is caused by lasR::write_las not writing with crs
        # this script attaches the crs when creating a rater mosaic of entire extent
    ######## BUT THE PROCESS EXECUTES FINE (UNLESS A DIFFERENT MESSAGE IS RETURNED)
    ######## !!!!! SO DON'T WORRY ABOUT IT ;D
  
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
      files = list.files(config$las_classify_dir, pattern = ".*_classify\\.(laz|las)$", full.names = T)
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
###______________________________###
# check file lists
###______________________________###
  # classify
  las_classify_flist = list.files(config$las_classify_dir, pattern = ".*_classify\\.(laz|las)$", full.names = T)
  # normalize
  las_normalize_flist = list.files(config$las_normalize_dir, pattern = ".*_normalize\\.(laz|las)$", full.names = T)
  normalize_lax_files = list.files(config$las_normalize_dir, pattern = ".*_normalize\\.lax$", full.names = T)
# execute if
if(
  # do las and lax files already exist?
  min(stringr::word(basename(las_classify_flist),sep = "_tile") %in%
    stringr::word(basename(las_normalize_flist),sep = "_tile")) != 1
  | min(stringr::word(basename(las_classify_flist),sep = "_tile") %in%
    stringr::word(basename(normalize_lax_files),sep = "_tile")) != 1
){
  # select raw files which do not have classified files
    flist_temp = las_classify_flist[which(
      !stringr::word(basename(las_classify_flist),sep = "_tile") %in%
      stringr::word(basename(las_normalize_flist),sep = "_tile")
    )]
  
  ### define function to normalize files either in parallel or not and call the function
  ### either path results in normalized files written to config$las_normalize_dir
  if(use_parallel_processing == T){
    # Error in unserialize(socklist[[n]]) : error reading from connection
      # means that one of the workers died when trying to process...try restarting
    ###______________________________________________________________________________________###
    ### In parallel height normalize across the tiles and rewrite them ###
    ###______________________________________________________________________________________###
    las_normalize_fn <- function(las_file_dir, out_dir, dtm=NULL) {
    
      ### Get a list of tiled files to ground classify
      lidar_list = list.files(las_file_dir, pattern = ".*\\.(laz|las)$", full.names = F)
      
      # configure parallel
      cores = parallel::detectCores()
      cluster = parallel::makeCluster(cores-1)
      # register the parallel backend with the `foreach` package
      doParallel::registerDoParallel(cluster)
      # pass to foreach to process each lidar file in parallel
        # for (i in 1:length(lidar_list)) {
        foreach::foreach(
          i = 1:length(lidar_list)
          , .packages = c("tools","lidR","tidyverse","doParallel")
          , .inorder = F
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
    
    # call the function
    las_normalize_fn(
      las_file_dir = config$las_classify_dir
      , out_dir = config$las_normalize_dir
      , dtm = NULL
    )
  }else{
    # define function to normalize files using lidR LAScatalog
    las_normalize_fn = function(in_dir_or_flist, out_dir){
      # set up las ctg
      las_classify_ctg = lidR::readLAScatalog(in_dir_or_flist)
      # redirect results to output 
      lidR::opt_output_files(las_classify_ctg) = paste0(out_dir, "/{*}_normalize")
      # turn off early exit
      lidR::opt_stop_early(las_classify_ctg) = FALSE
      # turn off early exit
      lidR::opt_progress(las_classify_ctg) = FALSE
      # normalize
      las_normalize_ans = lidR::normalize_height(las_classify_ctg, algorithm = knnidw())
      gc()
    }
    
    # call the function
    las_normalize_fn(
      in_dir_or_flist = flist_temp
      # in_dir_or_flist = config$las_classify_dir
      , out_dir = config$las_normalize_dir
    )
  } # end else if use_parallel_processing
  
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
  
  #switch to overwrite rasters since this section created new data
    overwrite_raster = T
}

#################################################################################
#################################################################################
# Create canopy height model (CHM) raster in lasR pipeline
#################################################################################
#################################################################################
###______________________________###
# check file lists
###______________________________###
  chm_file_name = paste0(config$delivery_dir, "/chm_", desired_chm_res, "m.tif")
# execute if
if(
  file.exists(chm_file_name) == F
  | overwrite_raster == T
){
    # note, this section throws the MESSAGE:
        # ERROR 1: PROJ: proj_create_from_database: Cannot find proj.db
        # no documentation on this error or how to fix...
        # this is caused by lasR::write_las not writing with crs
        # this script attaches the crs when creating a rater mosaic of entire extent
    ######## BUT THE PROCESS EXECUTES FINE (UNLESS A DIFFERENT MESSAGE IS RETURNED)
    ######## !!!!! SO DON'T WORRY ABOUT IT ;D
    
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
    
    # list of normalized files to use 
    las_normalize_flist = list.files(config$las_normalize_dir, pattern = ".*_normalize\\.(laz|las)$", full.names = T)
      
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
              write_stem_las_fn(las_path_name = flist_temp[i], min_tree_height = minimum_tree_height)
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
          purrr::map(write_stem_las_fn, min_tree_height = minimum_tree_height)
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
    
  # clean up
  remove(list = ls()[grep("_temp",ls())])
  gc()

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
        , x < 2 ~ 2 # set lower bound
        , x > 30 ~ 5  # set upper bound
        , TRUE ~ 2 + (x * 0.1)
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
          , hmin = minimum_tree_height
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
      #     , th_tree = minimum_tree_height
      #   )() # keep this additional parentheses's so it will work ?lidR::watershed
  
    # using ForestTools instead ..........
      # ...which requires the `raster` package which is depreciated
      crowns = ForestTools::mcws(
        treetops = sf::st_zm(tree_tops, drop = T) # drops z values
        , CHM = raster::raster(chm_rast) # converts to raster data ;[
        , minHeight = minimum_tree_height
      ) %>% terra::rast()
    
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
  competition_buffer_temp = 5
  ### how much of the buffered tree area is within the study boundary?
    # use this to scale the TPA estimates below
  tree_tops_pct_buffer_temp = tree_tops %>% 
    # buffer point
    sf::st_buffer(competition_buffer_temp) %>% 
    dplyr::mutate(
      point_buffer_area_m2 = as.numeric(sf::st_area(.))
    ) %>% 
    # intersect with study bounds
    sf::st_intersection(
      readLAScatalog(config$input_las_dir)@data$geometry %>% 
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
    sf::st_buffer(competition_buffer_temp) %>% 
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
    dist_buffer_temp = 50
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
    treemap_rast = terra::rast("../data/treemap/TreeMap2016.tif")
    
    ### filter treemap based on las...rast now in memory
    treemap_rast = treemap_rast %>% 
      terra::crop(
        readLAScatalog(config$input_las_dir)@data$geometry %>% 
          sf::st_union() %>% 
          terra::vect() %>% 
          terra::project(terra::crs(treemap_rast))
      ) %>% 
      terra::mask(
        readLAScatalog(config$input_las_dir)@data$geometry %>% 
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
      dplyr::rename(tm_id = value, tree_weight = count)
    # str(tm_id_weight_temp)
    
    ### get the TreeMap FIA tree list for only the plots included
    treemap_trees_df = readr::read_csv(
        "../data/treemap/TreeMap2016_tree_table.csv"
        , col_select = c(
          tm_id
          , TREE
          , STATUSCD
          , DIA
          , HT
        )
      ) %>% 
      dplyr::rename_with(tolower) %>% 
      dplyr::inner_join(
        tm_id_weight_temp
        , by = dplyr::join_by("tm_id")
      ) %>% 
      dplyr::filter(
        # keep live trees only: 1=live;2=dead
        statuscd == 1
        & !is.na(dia) 
        & !is.na(ht) 
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
      # quadratic model form with Gamma distribution for strictly positive response variable dbh
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
            , to = round(max(crowns_sf$tree_height_m)*1.05,0)
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
          dplyr::bind_cols(height_range)
        # str(pred_mod_nl_pop_temp)
        
    # # plot predictions with data
    #   ggplot(
    #     data = pred_mod_nl_pop_temp
    #     , mapping = aes(x = tree_height_m)
    #   ) +
    #     geom_ribbon(
    #       mapping = aes(ymin = lower_b, ymax = upper_b)
    #       , fill = "grey88"
    #     ) +
    #     geom_line(
    #       aes(y = estimate)
    #       , color = "navy"
    #       , lwd = 1
    #     ) +
    #     labs(
    #       y = "DBH (cm)"
    #       , x = "Tree Ht. (m)"
    #       , title = "Regional height to DBH allometry from US Forest Inventory and Analysis (FIA) data"
    #     ) +
    #     theme_light() +
    #     theme(legend.position = "none", plot.title = element_text(size = 9))
        
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
          pred_mod_nl_pop_temp %>% 
            dplyr::rename(
              tree_height_m_tnth=tree_height_m
              , est_dbh_cm = estimate
              , est_dbh_cm_lower = lower_b
              , est_dbh_cm_upper = upper_b
            ) %>% 
            dplyr::mutate(tree_height_m_tnth=as.character(tree_height_m_tnth))
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
    
    ###__________________________________________________________###
    ### Build local model to estimate missing DBHs using SfM DBHs
    ###__________________________________________________________###
      # Use the SfM-detected stems remaining after the filtering workflow 
      # for the local DBH to height allometric relationship model.
      
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
      )
      # summary(predicted_dbh_cm_temp)
      
      ## combine predicted data with training data for full data set for all tree crowns with a matched tree top
      # nrow(crowns_sf)
      crowns_sf_with_dbh = crowns_sf %>%
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
          , dbh_cm = dplyr::coalesce(stem_dbh_cm, predicted_dbh_cm)
          , dbh_m = dbh_cm/100
          , radius_m = dbh_m/2
          , basal_area_m2 = pi * (radius_m)^2
          , basal_area_ft2 = basal_area_m2 * 10.764
        ) %>% 
        dplyr::select(
          !tidyselect::ends_with("_dbh_cm")
        ) %>% 
        # join with regional model predictions at 0.1 m height intervals
        dplyr::mutate(
          tree_height_m_tnth = round(tree_height_m,1) %>% as.character()
        ) %>% 
        dplyr::inner_join(
          pred_mod_nl_pop_temp %>% 
            dplyr::rename(
              tree_height_m_tnth=tree_height_m
              , reg_est_dbh_cm = estimate
              , reg_est_dbh_cm_lower = lower_b
              , reg_est_dbh_cm_upper = upper_b
            ) %>% 
            dplyr::mutate(tree_height_m_tnth=as.character(tree_height_m_tnth))
          , by = dplyr::join_by(tree_height_m_tnth)  
        ) %>% 
        dplyr::select(-tree_height_m_tnth)
      
      # nrow(crowns_sf_with_dbh)
      # nrow(crowns_sf)
      # nrow(tree_tops)
      
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

      ### plot
        # ggplot(
        #   data = crowns_sf_with_dbh
        #   , mapping = aes(y=tree_height_m, x = dbh_cm)
        # ) +
        # geom_point(
        #   mapping = aes(color = is_training_data)
        #   , alpha = 0.6
        #   , size = 0.5
        # ) +
        # geom_smooth(
        #   method = "loess"
        #   , span = 1
        #   , color = "gray44"
        #   , alpha = 0.7
        # ) +
        # labs(
        #   x = "DBH (cm)"
        #   , y = "Tree Ht. (m)"
        #   , color = "Training Data"
        #   , title = "SfM derived tree height and DBH relationship"
        # ) +
        # scale_color_manual(values = c("gray", "firebrick")) +
        # theme_light() +
        # theme(
        #   legend.position = "bottom"
        #   , legend.direction = "horizontal"
        # )
  
    # clean up
      remove(list = ls()[grep("_temp",ls())])
      gc()

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
          , plot_area_m2 = readLAScatalog(config$input_las_dir)@data$geometry %>% 
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
      silv_metrics_temp = readLAScatalog(config$input_las_dir)@data$geometry %>%
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
    # message
    las_ctg_temp = lidR::readLAScatalog(config$input_las_dir)@data
    message(
      "total time was "
      , round(as.numeric(difftime(Sys.time(), full_start_time, units = c("mins"))),2)
      , " minutes to process "
      , scales::comma(sum(las_ctg_temp$Number.of.point.records))
      , " points over an area of "
      , scales::comma(as.numeric(las_ctg_temp$geometry %>% sf::st_union() %>% sf::st_area())/10000,accuracy = 0.01)
      , " hectares"
    )
    