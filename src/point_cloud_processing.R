## !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! ##
# must first download "RDS-2021-0074_Data.zip" at: https://doi.org/10.2737/RDS-2021-0074
# point input_treemap_dir parameter to directory that contains "TreeMap2016.tif" and "TreeMap2016_tree_table.csv"
## !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! ##

## !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! ##
## if the error: Error in any_list[[1]] : subscript out of bounds
## or if the error: Error: Invalid data: gpstime contains some NAs
## .... during the normalize step, suggests corrupted las file
## POTENTIAL FIX IS:
## !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! ##
  # # get the raw las file and header information
  # las_fnm = "C:/data/one_post_clipped.las"
  # las = rlas::read.las(las_fnm)
  # las_header = rlas::read.lasheader(las_fnm)
  # las_epsg = rlas::header_get_epsg(las_header)
  # las_wktcs = rlas::header_get_wktcs(las_header)
  # str(las)
  # # update the raw las files to fix data causing issues
  # las$gpstime = 0
  # las$NIR = NULL
  # str(las)
  # # create new las header
  # new_las_header = rlas::header_create(las)
  # new_las_header = rlas::header_set_epsg(new_las_header, las_epsg)
  # new_las_header = rlas::header_set_wktcs(new_las_header, las_wktcs)
  # # write
  # rlas::write.las(
  #   file = "C:/data/one_post_clipped_FIXED.las"
  #   , header = new_las_header
  #   , data = las
  # )
## !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! ##
## !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! ##
  
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
  # choose processing accuracy
  # XXL las catalogs (e.g. >70,000,000 pts) take a long time to process at accuracy_level = 3
    # from the lidR book https://r-lidar.github.io/lidRbook/norm.html:
      ## "Point cloud normalization without a DTM interpolates the elevation of 
      ## every single point locations using ground points. It no longer uses elevations 
      ## at discrete predefined locations (i.e. DTM). Thus the methods is exact, computationally speaking. 
      ## It means that it is equivalent to using a continuous DTM but it is important 
      ## to recall that all interpolation methods are interpolation and by definition 
      ## make guesses with different strategies. Thus by "exact" we mean "continuous"
  ###____________________###
  # accuracy_level = 1 # uses DTM to height normalize the points
  # accuracy_level = 2 # uses triangulation with high point density (20 pts/m2) to height normalize the points
  # accuracy_level = 3 # uses triangulation with very high point density (100 pts/m2) to height normalize the points
  accuracy_level = 2

  ###____________________###
  ### this process writes intermediate data to the disk
  ### keep those intermediate files (classfied, normalized, stem las files)?
  ###____________________###
  # keep_intermediate_files = F
  keep_intermediate_files = F
  
  ###____________________###
  ### use parallel processing? (T/F) ###
  ### parallel processing may not work on all machines ###
  ###____________________###
  # use_parallel_processing = F
  use_parallel_processing = T
  
  ###____________________###
  ### Set directory for outputs ###
  ###____________________###
  # output_dir = "../data"
  output_dir = "../data"
  
  ###_________________________###
  ### Set input las directory ###
  ###_________________________###
  # !!!!!!!!!! ENSURE FILES ARE PROJECTED IN CRS THAT USES METRE AS MEASURMENT UNIT
  # input_las_dir = "../data/lower_sherwin"
  input_las_dir = "../data/testtest"
  
  ###_________________________###
  ### Set input TreeMap directory ###
  ###_________________________###
  # input_treemap_dir = "../data/treemap"
  input_treemap_dir = "../data/treemap"
  
  ###_________________________###
  ### Set the desired raster resolution in metres for the digital terrain model
  ###_________________________###
  # desired_dtm_res = 1
  desired_dtm_res = 1
  
  ###_________________________###
  ### Set the desired raster resolution in metres for the canopy height model
  ###_________________________###
  # desired_chm_res = 0.25
  desired_chm_res = 0.25
  
  ###_________________________###
  ### Set the maximum height (m) for the canopy height model
  ###_________________________###
  # max_height_threshold_m = 60
  max_height_threshold_m = 60
  
  ###_________________________###
  ### Set the minimum height (m) for individual tree detection in `lidR::locate_trees`
  ###_________________________###
  # minimum_tree_height_m = 1.37
  minimum_tree_height_m = 2
  
  ###_________________________###
  ### Set the maximum dbh size (meters)
  ###_________________________###
  # dbh_max_size_m = 10
  dbh_max_size_m = 7
  
  ###_________________________###
  ### Set the model to use for local dbh-height allometry
  ### can be "rf" for random forest or "lin" for linear 
  ###_________________________###
  # local_dbh_model = "rf"
  # local_dbh_model = "lin"
  local_dbh_model = "rf"
  
  ###_________________________###
  ### Default epsg to use if your las has a blank projection
  ### this should be the epsg (CRS) that the las was exported with (e.g. from Metashape)
  ### see: https://epsg.io/
  ### leave as NA if unsure
  ###_________________________###
  # user_supplied_epsg = "6345"
  user_supplied_epsg = NA
  
  ### do you want to reproject to this crs if the file currently has one set?
  ### be careful! this is ineffiecient and potentially causes inaccuracies due to transformations
  ### see : https://gis.stackexchange.com/questions/371566/can-i-re-project-an-las-file-in-lidr
  ### leave as FALSE if unsure
  # transform_to_this_epsg = F
  transform_to_this_epsg = F
  # if you want to transform by setting transform_to_this_epsg = T
  # ...and your las files do not have an epsg set, then you must supply the old epsg if known
  # ... if unsure leave as NA
  user_supplied_old_epsg = NA
#################################################################################
#################################################################################
# Developer Parameters
# ...these parameters control the automatic "chunking" of data and script flows
# ...should only need to be adjusted if running into "R Session Aborted" errors 
# ...which is largely due to memory full issues
#################################################################################
#################################################################################
  ######################################
  # chunk for large area/pt pt clouds
  # adpative retiling w/ minimal (no?) user input
  # set maximums based on sample data testing
  ######################################
  # for super big ctgs...lots of different stuff needs to happen to reduce memory issues
  max_ctg_pts = 70e6 # max points to process in any ctg using lasR at one time
  
  # this one is less important as never experienced memory issues with large areas (just lots of pts)
  max_area_m2 = 90e3 # original = 90e3
  
  #### This maximum filters the ground points to perform Delaunay triangulation
  # see: https://github.com/r-lidar/lasR/issues/18#issuecomment-2027818414
  max_pts_m2 = dplyr::case_when(
    as.numeric(accuracy_level) <= 2 ~ 20 
    , as.numeric(accuracy_level) == 3 ~ 100
    , T ~ 20
  )
  ## at 100 pts/m2, the resulting CHM pixel values (0.25m resolution) are within -0.009% and 0.026% metres
    # ... of the values obtained by using 400 pts/m2 with 99% probability
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
  library(ForestTools) # for crown delineation
  library(rlas) # write las index files .lax
  
  ## !! lasR package not available on CRN
    ## uncomment to install from github see: https://r-lidar.github.io/lasR/index.html
    # library(pak)
    # pak::pkg_install("r-lidar/lasR", upgrade = T)
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
  library(RCSF) # for the Cloth simulation filtering (CSF) (Zhang et al 2016) algorithm to classify points
  library(RMCC) # for the Multiscale Curvature Classification (MCC) (Evans and Hudak 2016) algorithm to classify points
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
  create_project_structure = function(output_dir,input_las_dir){
    ###___________________________________________________###
    ### Set output delivery directory
    ###___________________________________________________###
    temp_dir = file.path(normalizePath(output_dir), "point_cloud_processing_temp")
    delivery_dir = file.path(normalizePath(output_dir), "point_cloud_processing_delivery")
  
    ### Set output directory for temporary files
    las_grid_dir = file.path(temp_dir, "00_grid")
    las_classify_dir = file.path(temp_dir, "01_classify")
    las_normalize_dir = file.path(temp_dir, "02_normalize")
    dtm_dir = file.path(temp_dir, "03_dtm")
    chm_dir = file.path(temp_dir, "04_chm")
    las_stem_norm_dir = file.path(temp_dir, "05_stem_norm")
    las_stem_dir = file.path(temp_dir, "06_las_stem")
    stem_poly_tile_dir = file.path(temp_dir, "07_stem_poly_tile")
    
    ### Create the directories
    dir.create(delivery_dir, showWarnings = FALSE)
    dir.create(temp_dir, showWarnings = FALSE)
    dir.create(las_grid_dir, showWarnings = FALSE)
    dir.create(las_classify_dir, showWarnings = FALSE)
    dir.create(las_normalize_dir, showWarnings = FALSE)
    dir.create(dtm_dir, showWarnings = FALSE)
    dir.create(chm_dir, showWarnings = FALSE)
    dir.create(las_stem_norm_dir, showWarnings = FALSE)
    dir.create(las_stem_dir, showWarnings = FALSE)
    dir.create(stem_poly_tile_dir, showWarnings = FALSE)
    
    ###______________________________###
    ### Set names of the directories ###
    ###______________________________###
    
    names(output_dir) = "output_dir"
    names(input_las_dir) = "input_las_dir"
    names(input_treemap_dir) = "input_treemap_dir"
    names(delivery_dir) = "delivery_dir"
    names(temp_dir) = "temp_dir"
    names(las_grid_dir) = "las_grid_dir"
    names(las_classify_dir) = "las_classify_dir"
    names(las_normalize_dir) = "las_normalize_dir"
    names(dtm_dir) = "dtm_dir"
    names(chm_dir) = "chm_dir"
    names(las_stem_norm_dir) = "las_stem_norm_dir"
    names(las_stem_dir) = "las_stem_dir"
    names(stem_poly_tile_dir) = "stem_poly_tile_dir"
    
    ###______________________________###
    ### Append to output config list ###
    ###______________________________###
    
    config = cbind(
      output_dir, input_las_dir, input_treemap_dir
      , delivery_dir, temp_dir, las_grid_dir, las_classify_dir
      , las_normalize_dir, dtm_dir, chm_dir
      , las_stem_norm_dir, las_stem_dir, stem_poly_tile_dir
    )
    
    config = as.data.frame(config)
    #config
    
    ### Return config 
    return(config)
    
  }
  # call the function
  config = create_project_structure(output_dir, input_las_dir)
  
  # remove all files in delivery and temp
  list.files(config$temp_dir, recursive = T, full.names = T) %>%
      purrr::map(file.remove)
  # list.files(config$delivery_dir, recursive = T, full.names = T) %>%
  #     purrr::map(file.remove)

#################################################################################
#################################################################################
# Function to reproject
#################################################################################
#################################################################################
  # function to reproject
  # see : https://gis.stackexchange.com/questions/371566/can-i-re-project-an-las-file-in-lidr
  reproject_las_fn <- function(filepath, new_crs = NA, old_crs = NA, outdir = getwd()) {
    if(is.null(new_crs) | is.na(new_crs)){stop("the new_crs must be provided")}
    # read individual file
    las = lidR::readLAS(filepath) 
    if(
      (is.null(sf::st_crs(las)$epsg) | is.na(sf::st_crs(las)$epsg))
      & ( is.null(old_crs) | is.na(old_crs) | old_crs=="" )
    ){
      stop("the raw las file has missing CRS and cannot be transformed. try setting old_crs if known")
    }else{
      # transform if know old crs
      if(
        (is.null(sf::st_crs(las)$epsg) | is.na(sf::st_crs(las)$epsg))
        & !is.null(old_crs) & !is.na(old_crs) & length(as.character(old_crs))>0
      ){
        sf::st_crs(las) = paste0("EPSG:", old_crs)
      }
      # get filename
      fnm = filepath %>% basename() %>% stringr::str_remove_all("\\.(laz|las)$")
      new_fnm = paste0(normalizePath(outdir),"/",fnm,"_epsg",new_crs,".las")
      # reproject
      las = sf::st_transform(las, paste0("EPSG:", new_crs))
      # write
      lidR::writeLAS(las, file = new_fnm)
      return(new_fnm)
    }
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
          # check validity of data
          l = rlas::read.lasheader(des_file) %>% length()
          if(l==0 | is.na(l) | is.null(l)){
            return(NULL)
          }else{
            ### Compile the .lax file name
            des_file_lax = tools::file_path_sans_ext(des_file)
            des_file_lax = paste0(des_file_lax, ".lax")
            
            ### See if the .lax version exists in the input directory
            does_file_exist = file.exists(des_file_lax)
            # does_file_exist
            
            ### If file does_file_exist, do nothing
            if(does_file_exist == TRUE){return(des_file)}
            
            ### If file doesnt_file_exist, create a .lax index
            if(does_file_exist == FALSE){
              rlas::writelax(des_file)
              return(des_file)
            }
          }
        }) %>% unlist()
  }

# start time
  xx1_tile_start_time = Sys.time()
#################################################################################
#################################################################################
# Tile raw las files to work with smaller chunks if needed
#################################################################################
#################################################################################
  # create spatial index files (.lax)
    flist_temp = create_lax_for_tiles(
      las_file_list = list.files(config$input_las_dir, pattern = ".*\\.(laz|las)$", full.names = T)
    )
  ### point to input las files as a lidR LAScatalog (reads the header of all the LAS files of a given folder)
    las_ctg = lidR::readLAScatalog(flist_temp)
    
  ###______________________________###
  # crs checks and transformations
  ###______________________________###
    # transform if user requested
    if(
      transform_to_this_epsg == T & 
      !is.na(user_supplied_epsg) & 
      !is.null(user_supplied_epsg) & 
      length(as.character(user_supplied_epsg))>0
    ){
      message("reprojecting raw las data as requested. may lead to result inaccuracies!")
      # map over files
        flist_temp = las_ctg@data$filename %>% 
          purrr::map(
            reproject_las_fn
            , new_crs = user_supplied_epsg
            , old_crs = user_supplied_old_epsg
            , outdir = config$input_las_dir
          ) %>% 
          c() %>% 
          unlist()
      # create spatial index files (.lax)
        flist_temp = create_lax_for_tiles(flist_temp)
      ### point to input las files as a lidR LAScatalog
        las_ctg = lidR::readLAScatalog(flist_temp)
    }
    
    # pull crs for using in write operations
    # crs_list_temp = las_ctg@data$CRS
    crs_list_temp = sf::st_crs(las_ctg)$epsg
    
    # handle missing epsg with user defined parameter
    if(is.na(crs_list_temp) & !is.na(user_supplied_epsg) & !is.null(user_supplied_epsg)){
      crs_list_temp = user_supplied_epsg
      sf::st_crs(las_ctg) = paste0("EPSG:", user_supplied_epsg)
    }
    
    if(length(unique(crs_list_temp))>1){
      stop("the raw las files have multiple CRS settings")
    }else{
      proj_crs = paste0("EPSG:",unique(crs_list_temp))
    }
    
  ### is this ctg huge or what?
    ctg_pts_so_many = sum(las_ctg@data$Number.of.point.records) > max_ctg_pts

  ###______________________________###
  # write las coverage data to delivery
  ###______________________________###
    sf::st_write(
      las_ctg@data
      , paste0(config$delivery_dir, "/raw_las_ctg_info.gpkg")
      , quiet = TRUE, append = FALSE
    )
  
  ########################
  # check for tile overlaps and so many points in whole catalog
  # ...determines tiling and processing of grid subsets
  ########################
    ctg_chunk_data =
      las_ctg@data %>%
      dplyr::mutate(
        area_m2 = sf::st_area(geometry) %>% as.numeric()
        , pts = Number.of.point.records
      ) %>% 
      sf::st_drop_geometry() %>% 
      dplyr::select(filename, area_m2, pts) %>% 
      dplyr::ungroup() %>% 
      dplyr::summarise(
        dplyr::across(.cols = c(area_m2, pts), .fns = sum)
      ) %>% 
      # attach overall area
      dplyr::bind_cols(
        las_ctg@data$geometry %>%
          sf::st_union() %>% 
          sf::st_as_sf() %>% 
          sf::st_set_geometry("geometry") %>% 
          dplyr::mutate(
            total_area_m2 = sf::st_area(geometry) %>% as.numeric()
          ) %>% 
          sf::st_drop_geometry()
      ) %>% 
      dplyr::mutate(
        # so many points chunk size
        # uneven distribution of points in ctg => some chunks bigger than max_ctg_pts
        ctg_pt_factor = pts/(max_ctg_pts*0.2) #...so make much smaller
        , chunk_max_ctg_pts = dplyr::case_when(
            # no resize
            ctg_pt_factor <= 1 ~ 0
            # yes resize
            , ctg_pt_factor > 1 ~ round(sqrt(area_m2/ctg_pt_factor), digits = -1) # round to nearest 10
          )
        , buffer_chunk_max_ctg_pts = ifelse(round(chunk_max_ctg_pts*0.05,digits=-1)<10,10,round(chunk_max_ctg_pts*0.05,digits=-1))
        # overlap chunk size
        , pct_overlap = (area_m2-total_area_m2)/total_area_m2
        , area_factor = total_area_m2/max_area_m2
        , pt_factor = pts/max_ctg_pts
        , chunk_overlap = dplyr::case_when(
            # no resize
            area_factor <= 1 & pt_factor <= 1 ~ 0
            # yes resize
            , area_factor > 1 & pt_factor > 1 ~ min(
              round(sqrt(area_m2/area_factor), digits = -1) # round to nearest 10
              , round(sqrt(area_m2/pt_factor), digits = -1) # round to nearest 10
            )
            , area_factor > 1 ~ round(sqrt(area_m2/area_factor), digits = -1) # round to nearest 10
            , pt_factor > 1 ~ round(sqrt(area_m2/pt_factor), digits = -1) # round to nearest 10
          )
        , buffer_overlap = ifelse(round(chunk_overlap*0.05,digits=-1)<10,10,round(chunk_overlap*0.05,digits=-1))
      )
    # ctg_chunk_data
  ############################################################
  ############################################################
  # use ctg_chunk_data to determine chunking
  ############################################################
  ############################################################
    # plot(las_ctg)
    ########################
    # split into grid subsets
    # this is done for ctg's with sooo many points
    # because lasR pipeline crashes memory and only way around it is 
    # to break out las into separarte chunks (requires reading/writing much data ;()
    # ....
    # objective here is to break the ctg into lasR manageable subsets: each subset goes through lasR pipeline individually
    # then bring the DTM and CHM results together via terra::sprc and mosaic, e.g.:
      # # read
      # rast_list = list.files("../data/", pattern = ".*\\.(tif|tiff)$", full.names = T) %>% purrr::map(function(x){terra::rast(x)})
      # # mosaic
      # rast_mosaic = terra::sprc(rast_list) %>% terra::mosaic(fun = "max")
    ########################
    if(
      ctg_chunk_data$chunk_max_ctg_pts[1] > 0
      & ctg_pts_so_many == T
    ){
      # if need to retile
      # retile catalog
      lidR::opt_progress(las_ctg) = F
      lidR::opt_output_files(las_ctg) = paste0(config$las_grid_dir,"/", "_{XLEFT}_{YBOTTOM}") # label outputs based on coordinates
      lidR::opt_filter(las_ctg) = "-drop_duplicates"
      # buffering here because these grid subsets will be processed independently
      lidR::opt_chunk_buffer(las_ctg) = 10
      lidR::opt_chunk_size(las_ctg) = ctg_chunk_data$chunk_max_ctg_pts[1]
      # reset las_ctg
      lidR::catalog_retile(las_ctg) # apply retile
      lidR::opt_progress(las_ctg) = T
      # create spatial index
      flist_temp = create_lax_for_tiles(
        las_file_list = list.files(config$las_grid_dir, pattern = ".*\\.(laz|las)$", full.names = T)
      )
      # switch for processing grid subsets
      grid_subset_switch = T
    }else if( # retile whole catalog if high overlap
      ctg_chunk_data$chunk_overlap[1] > 0
      & ctg_chunk_data$pct_overlap[1] > 0.1
    ){
      # if need to retile
      # retile catalog
      lidR::opt_progress(las_ctg) = F
      lidR::opt_output_files(las_ctg) = paste0(config$las_grid_dir,"/", "_{XLEFT}_{YBOTTOM}") # label outputs based on coordinates
      lidR::opt_filter(las_ctg) = "-drop_duplicates"
      # buffering is handled by lasR::exec so no need to buffer here
      # not buffering here because these grid subsets will be processed altogether with buffer set for lasR::exec
      lidR::opt_chunk_buffer(las_ctg) = 0
      lidR::opt_chunk_size(las_ctg) = ctg_chunk_data$chunk[1]
      # reset las_ctg
      lidR::catalog_retile(las_ctg) # apply retile
      lidR::opt_progress(las_ctg) = T
      # create spatial index
      flist_temp = create_lax_for_tiles(
        las_file_list = list.files(config$las_grid_dir, pattern = ".*\\.(laz|las)$", full.names = T)
      )
      # switch for processing grid subsets
      grid_subset_switch = F
    }else{grid_subset_switch = F}
      # flist_temp
      # grid_subset_switch
      # plot(lidR::readLAScatalog(flist_temp))
      
      # lidR::readLAScatalog(flist_temp)@data %>% 
      #   dplyr::select(filename, Number.of.point.records) %>% 
      #   sf::st_drop_geometry()
  
  ########################
  # data on how the chunks are processed for writing
  # get pct filter for creating dtm and normalized point clouds using triangulation
  ########################
    process_data =
      lidR::readLAScatalog(flist_temp)@data %>%
      dplyr::ungroup() %>% 
      dplyr::mutate(
        processing_grid = dplyr::case_when(
          grid_subset_switch == T ~ dplyr::row_number()
          , T ~ 1
        )
        , area_m2 = sf::st_area(geometry) %>% as.numeric()
        , pts = Number.of.point.records
        , pts_m2 = pts/area_m2
      ) %>% 
      dplyr::select(processing_grid, filename, area_m2, pts, pts_m2) %>% 
      # get rid of tiny edge files with very few points which cannot be utilized for delauny triangulation
        # !!!!!!!!!!!!!!! upgrade to remove tiles completely spatially covered by other tiles
      dplyr::filter(pts>max_ctg_pts*0.0001) %>% 
      # sometimes there are super small chunks created which can lead to 0 points after filtering for performing calcs
      dplyr::mutate(
        processing_grid = dplyr::case_when(
          pts < max_ctg_pts*0.0002 & !is.na(dplyr::lag(pts)) & !is.na(dplyr::lead(pts)) & 
            dplyr::lag(pts) < dplyr::lead(pts) ~ dplyr::lag(processing_grid)
          , pts < max_ctg_pts*0.0002 & !is.na(dplyr::lag(pts)) & !is.na(dplyr::lead(pts)) & 
            dplyr::lag(pts) > dplyr::lead(pts) ~ dplyr::lead(processing_grid)
          , pts < max_ctg_pts*0.0002 & !is.na(dplyr::lag(pts)) ~ dplyr::lag(processing_grid)
          , pts < max_ctg_pts*0.0002 & !is.na(dplyr::lead(pts)) ~ dplyr::lead(processing_grid)
          , T ~ processing_grid
        )
      ) %>% 
      dplyr::group_by(processing_grid) %>% 
      dplyr::mutate(
        pts_m2 = pts/area_m2
      # calculate factor to reduce by for triangulation
        , pts_m2_factor_optimal = dplyr::case_when(
            max_pts_m2/pts_m2 >= 1 ~ 1
            , TRUE ~ max_pts_m2/pts_m2
        )
        # area based weighted mean
          # pass this to filter_for_dtm
        , pts_m2_factor_ctg = max(stats::weighted.mean(
            x = pts_m2_factor_optimal
            , w = area_m2/sum(area_m2)
          ) %>% 
          round(digits = 2),0.01)
        , filtered_pts_m2 = pts_m2*pts_m2_factor_ctg
        , processing_grid_tot_pts = sum(pts)
      ) %>% 
      dplyr::ungroup()
    
    # plot
    process_data %>% 
      dplyr::group_by(processing_grid) %>%
      dplyr::mutate(lab = pts == max(pts)) %>%
      ggplot() +
        geom_sf(aes(fill = as.factor(processing_grid)), alpha = 0.6) +
        geom_sf_text(aes(label = ifelse(lab,as.factor(processing_grid), ""))) +
        scale_fill_manual(
          values =
            viridis::turbo(n=length(process_data$processing_grid %>% unique())) %>% 
            sample() 
        ) +
        labs(x="",y="",title="processing_grid") +
        theme_light() +
        theme(legend.position = "none")
    # # count
    # process_data %>%
    #   dplyr::distinct(processing_grid, processing_grid_tot_pts)
    
  # save processing attributes
    sf::st_write(
      process_data
      , paste0(config$delivery_dir, "/process_data_las_tiling.gpkg")
      , quiet = TRUE, append = FALSE
    )
  # clean up
    remove(list = ls()[grep("_temp",ls())])
    # clean up from setup to free some memory
    # check_ls_size_fn(ls())
    remove(create_project_structure)
    gc()
      
# start time
  xx2_class_dtm_norm_chm_start_time = Sys.time()  
#################################################################################
#################################################################################
# lasR pipeline does it all:
  # classify points = RCSF::CSF
  # denoise = lasR::classify_isolated_points
  # DTM with delauny triangulation = lasR::triangulate
  # normalize = lasR::triangulate or based on DTM (see accuracy_level)
  # CHM based on classified and normalized points with user-defined min/max heights
#################################################################################
#################################################################################
  ######################################
  # DEFINE ALL lasR pipeline steps
  ######################################
  ###################
  # read with filter
  ###################
    lasr_read = lasR::reader_las(filter = "-drop_noise -drop_duplicates")
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
    ################
    # library(RCSF) # for the Cloth simulation filtering (CSF) (Zhang et al 2016) algorithm to classify points
    ################
    lasr_classify_csf = function(
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
        data$Classification = class
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
  # send it to pipeline with defaults
    lasr_classify = lasr_classify_csf(
        # csf options
        smooth = FALSE, threshold = 0.5
        , resolution = 0.5, rigidness = 1L
        , iterations = 500L, step = 0.65
      )
  ###################
  # denoise
  ###################
    # classify isolated points
    lasr_denoise = lasR::classify_isolated_points(res =  5, n = 6)
    # lasr_denoise_filter = lasR::delete_points(filter = lasR::drop_noise())
    
    # classify write step after denoise
    lasr_write_classify = lasR::write_las(
        ofile = paste0(config$las_classify_dir, "/*_classify.las")
        # ofile = paste0(config$las_classify_dir, "/*_classify.las")
        , filter = "-drop_noise"
        , keep_buffer = F
      )
  ###################
  # dtm + normalize
  ###################
    lasr_dtm_norm_fn = function(
      dtm_file_name = paste0(config$delivery_dir, "/dtm_", desired_dtm_res, "m.tif")
      , frac_for_tri = 1
      , dtm_res = desired_dtm_res
      , norm_accuracy = accuracy_level
    ){
      # perform Delaunay triangulation
        # tri = lasR::triangulate(filter = "-keep_class 2 -keep_class 9 -keep_random_fraction 0.01")
        ####
        # set filter based on # points
        ####
        filter_for_dtm = paste0(
          "-drop_noise -keep_class 2 -keep_class 9 -keep_random_fraction "
          , ifelse(frac_for_tri>=1, "1", scales::comma(frac_for_tri, accuracy = 0.01))
        )
        ####
        # triangulate with filter
        ####
        lasr_triangulate = lasR::triangulate(
          # class 2 = ground; class 9 = water
          filter = filter_for_dtm
          , max_edge = 0
          # , max_edge = c(0,1)
          # # write to disk to preserve memory
          , ofile = ""
          # , ofile = paste0(config$las_denoise_dir, "/", "*_tri.gpkg")
        )
      # rasterize the result of the Delaunay triangulation
        lasr_dtm = lasR::rasterize(
          res = dtm_res
          , operators = lasr_triangulate
          , filter = lasR::drop_noise()
          # # write to disk to preserve memory
          , ofile = dtm_file_name
        ) 
      # normalize
      # stage = triangulate: takes foreevvveerrrrrrr
        # ... but see: https://github.com/r-lidar/lasR/issues/18#issuecomment-2027818414
        ## at this density of point, my advice is anyway to decimate your ground points. 
        ## With 1/100 of the ground points you already have 5 ground pts/m2 to compute a 
        ## DTM with a 1 m resolution! You could even decimate to 1/250. 
        ## This will solve your computation time issue in the same time.
        ## stage = dtm
          # ... see: https://github.com/r-lidar/lasR/issues/17#issuecomment-2027698100
          # also from the lidR book https://r-lidar.github.io/lidRbook/norm.html:
            ## "Point cloud normalization without a DTM interpolates the elevation of 
            ## every single point locations using ground points. It no longer uses elevations 
            ## at discrete predefined locations. Thus the methods is exact, computationally speaking. 
            ## It means that it is equivalent to using a continuous DTM but it is important 
            ## to recall that all interpolation methods are interpolation and by definition 
            ## make guesses with different strategies. Thus by “exact” we mean “continuous”.
        if(as.numeric(norm_accuracy) %in% c(2,3)){
          lasr_normalize = lasR::transform_with(
            stage = lasr_triangulate
            , operator = "-"
          )
        }else{
          lasr_normalize = lasR::transform_with(
            stage = lasr_dtm
            , operator = "-"
          )
        }
      # pipeline
      pipeline = lasr_triangulate + lasr_dtm + lasr_normalize
      return(pipeline)
    }
  # gets sent to pipeline later with options based on process_data
  # write
    lasr_write_normalize = lasR::write_las(
      filter = "-drop_z_below 0 -drop_class 18"
      , ofile = paste0(config$las_normalize_dir, "/*_normalize.las")
      , keep_buffer = F
    )
  ###################
  # chm
  ###################
    lasr_chm_fn = function(
     chm_file_name = paste0(config$delivery_dir, "/chm_", desired_chm_res, "m.tif") 
     , chm_res = desired_chm_res
     , min_height_m = minimum_tree_height_m
     , max_height_m = max_height_threshold_m
    ){
      # chm
        #set up chm pipeline step
        # operators = "max" is analogous to `lidR::rasterize_canopy(algorithm = p2r())`
        # for each pixel of the output raster the function attributes the height of the highest point found
        lasr_chm = lasR::rasterize(
          res = chm_res
          , operators = "max"
          , filter = paste0(
            "-drop_class 2 9 18 -drop_z_below "
            , min_height_m
            , " -drop_z_above "
            , max_height_m
          )
          , ofile = ""
          # , ofile = paste0(config$chm_dir, "/*_chm.tif")
        )
      # Pits and spikes filling for raster with algorithm from St-Onge 2008 (see reference).
        lasr_chm_pitfill = lasR::pit_fill(raster = lasr_chm, ofile = chm_file_name)
      # pipeline
        pipeline = lasr_chm + lasr_chm_pitfill
        return(pipeline)
    }
  # gets sent to pipeline later with options based on process_data
    
  ######################################
  # lasR full pipeline
  ######################################
  # map over process_data$processing_grid
  lasr_pipeline_fn = function(
    processing_grid_num = 1
    , keep_intrmdt = keep_intermediate_files
    # lasr_dtm_norm_fn
    , dtm_res_m = desired_dtm_res
    , normalization_accuracy = accuracy_level
    # lasr_chm_fn
     , chm_res_m = desired_chm_res
     , min_height = minimum_tree_height_m
     , max_height = max_height_threshold_m
  ){
    # output files
    dtm_file_name = paste0(config$dtm_dir, "/", processing_grid_num,"_dtm_", dtm_res_m, "m.tif")
    chm_file_name = paste0(config$chm_dir, "/", processing_grid_num,"_chm_", chm_res_m, "m.tif")
    # files to process
    flist = process_data %>% 
      dplyr::filter(processing_grid == processing_grid_num) %>% 
      dplyr::pull(filename)
    # set up catalog
      tile_las_ctg = lidR::readLAScatalog(flist)
      lidR::opt_chunk_buffer(tile_las_ctg) = 10
      lidR::opt_chunk_size(tile_las_ctg) = 0
    # fraction for triangulation
    frac_for_tri = process_data %>% 
      dplyr::filter(processing_grid == processing_grid_num) %>% 
      dplyr::pull(pts_m2_factor_ctg) %>% 
      .[1]
    ################
    # buld pipeline
    ################
    if(keep_intrmdt == T){
      # pipeline
        lasr_pipeline_temp = lasr_read +
          lasr_classify +
          lasr_denoise +
          lasr_write_classify +
          lasr_dtm_norm_fn(
            dtm_file_name = dtm_file_name
            , frac_for_tri = frac_for_tri
            , dtm_res = dtm_res_m
            , norm_accuracy = normalization_accuracy
          ) + 
          lasr_write_normalize +
          lasr_chm_fn(
            chm_file_name = chm_file_name
             , chm_res = chm_res_m
             , min_height_m = min_height
             , max_height_m = max_height
          )
    }else{
      # pipeline
        lasr_pipeline_temp = lasr_read +
          lasr_classify +
          lasr_denoise +
          lasr_dtm_norm_fn(
            dtm_file_name = dtm_file_name
            , frac_for_tri = frac_for_tri
            , dtm_res = dtm_res_m
            , norm_accuracy = normalization_accuracy
          ) + 
          lasr_write_normalize +
          lasr_chm_fn(
            chm_file_name = chm_file_name
             , chm_res = chm_res_m
             , min_height_m = min_height
             , max_height_m = max_height
          )
    }
    # message
    message(
      "starting lasR processing of processing grid "
      , processing_grid_num
      , " ("
      , process_data %>% 
        dplyr::filter(processing_grid == processing_grid_num) %>% 
        dplyr::pull(processing_grid_tot_pts) %>% 
        .[1] %>% 
        scales::comma(accuracy = 1)
      , " pts) at ..."
      , Sys.time()
    )
    # lasR execute
    lasr_ans = lasR::exec(
      lasr_pipeline_temp
      , on = tile_las_ctg
      , with = list(
        progress = T
      )
    )
    # sleep
    Sys.sleep(4) # sleep to give c++ stuff time to reset
    return(lasr_ans)
  }
      
  ######################################
  # execute pipeline
  ######################################
    # set lasR parallel processing options as of lasR 0.4.2
      lasR::set_parallel_strategy(
        strategy = lasR::concurrent_points(
            ncores = max(lasR::ncores()-1, lasR::half_cores())
          )
      )
    
    # create safe function to capture error and map over
      safe_lasr_pipeline_fn = purrr::safely(lasr_pipeline_fn)
      
    # map over processing grids
      lasr_ans_list = 
        process_data$processing_grid %>%
        unique() %>%
        purrr::map(safe_lasr_pipeline_fn) 
        
    # check for errors other than too few points for triangulation which happens on edge chunks with few points and also those with copious noise
      error_list_temp = 
        lasr_ans_list %>% 
        purrr::transpose() %>% 
        purrr::pluck("error") %>% 
        unlist() %>% 
        purrr::pluck("message")
      
      if(length(error_list_temp)>0){
        has_errors_temp = error_list_temp %>% 
          dplyr::tibble() %>%
          dplyr::rename(message=1) %>% 
          dplyr::mutate(
            is_tri_error = dplyr::case_when(
              stringr::str_detect(tolower(message), "impossible") & 
                stringr::str_detect(tolower(message), "triangulation") ~ 1
              , T ~ 0
            )
            , sum_is_tri_error = sum(is_tri_error)
            , pct_tri = sum_is_tri_error/length(unique(process_data$processing_grid))
            , keep_it = dplyr::case_when(
              is_tri_error==1 & pct_tri<0.5 ~ F
              , T ~ T
            )
          ) %>% 
          dplyr::filter(keep_it==T)
      }else{has_errors_temp = dplyr::tibble()}
      
    if(nrow(has_errors_temp)>0){stop("lasR processing failed due to:\n", has_errors_temp$message[1])}
    
    # clean up
      remove(list = ls()[grep("_temp",ls())])
      gc()
  ###################################
  # lasR cleanup and polish
  ###################################
    ###############
    # DTM 
    ###############
      # output name
      dtm_file_name = paste0(config$delivery_dir, "/dtm_", desired_dtm_res, "m.tif")
      # read
      rast_list_temp = list.files(config$dtm_dir, pattern = ".*\\.(tif|tiff)$", full.names = T) %>% purrr::map(function(x){terra::rast(x)})
      # mosaic
      dtm_rast = terra::sprc(rast_list_temp) %>% terra::mosaic(fun = "mean")
      # # fill empty cells
      #   #### this is not needed anymore as empty cells resulting from rasterizing with a triangulation are fixed
      #   #### see: https://github.com/r-lidar/lasR/issues/18
      #   dtm_rast = dtm_rast %>%
      #     terra::crop(
      #       las_ctg@data$geometry %>%
      #         sf::st_union() %>%
      #         terra::vect() %>%
      #         terra::project(terra::crs(dtm_rast))
      #     ) %>%
      #     terra::mask(
      #       las_ctg@data$geometry %>%
      #         sf::st_union() %>%
      #         terra::vect() %>%
      #         terra::project(terra::crs(dtm_rast))
      #     ) %>%
      #     terra::focal(
      #       w = 3
      #       , fun = "mean"
      #       , na.rm = T
      #       # na.policy Must be one of:
      #         # "all" (compute for all cells)
      #         # , "only" (only for cells that are NA)
      #         # , or "omit" (skip cells that are NA).
      #       , na.policy = "only"
      #     )
      #   # dtm_rast %>% terra::crs()
      #   # dtm_rast %>% terra::plot()
      # set crs
        terra::crs(dtm_rast) = proj_crs
      # write to delivery directory
        terra::writeRaster(
          dtm_rast
          , filename = dtm_file_name
          , overwrite = T
        )
    ###############
    # chm 
    ###############
      # output name
      chm_file_name = paste0(config$delivery_dir, "/chm_", desired_chm_res, "m.tif")
      # read
      rast_list_temp = list.files(config$chm_dir, pattern = ".*\\.(tif|tiff)$", full.names = T) %>% purrr::map(function(x){terra::rast(x)})
      # mosaic
      chm_rast = terra::sprc(rast_list_temp) %>% terra::mosaic(fun = "max")
      # fill empty cells
        # this helps to smooth out tile gaps which leads to too many trees being detected during itd
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
      # set crs
        terra::crs(chm_rast) = proj_crs
      # write to delivery directory
        terra::writeRaster(
          chm_rast
          , filename = chm_file_name
          , overwrite = T
        )
    
    # create spatial index files (.lax)
      # classify
      create_lax_for_tiles(
        las_file_list = list.files(config$las_classify_dir, pattern = ".*\\.(laz|las)$", full.names = T)
      )
      # normalize
      normalize_flist = create_lax_for_tiles(
        las_file_list = list.files(config$las_normalize_dir, pattern = ".*\\.(laz|las)$", full.names = T)
      )
    
    # clean up
      remove(list = ls()[grep("_temp",ls())])
      gc()
    
    # # # plots
    # dtm_rast %>%
    #   # terra::aggregate(fact = 4) %>%
    #   as.data.frame(xy = T) %>%
    #   dplyr::rename(f=3) %>%
    #   ggplot() +
    #     geom_tile(aes(x=x,y=y,fill=f)) +
    #     geom_sf(data = las_ctg@data$geometry, fill = NA) +
    #     scale_fill_viridis_c(option = "viridis") +
    #     labs(fill = "meters") +
    #     theme_void()
    # chm_rast %>%
    #   as.data.frame(xy = T) %>%
    #   dplyr::rename(f=3) %>%
    #   ggplot() +
    #     geom_tile(aes(x=x,y=y,fill=f)) +
    #     geom_sf(data = las_ctg@data$geometry, fill = NA) +
    #     scale_fill_viridis_c(option = "plasma") +
    #     labs(fill = "meters") +
    #     theme_void()
      
# start time
  xx7_treels_start_time = Sys.time()
#################################################################################
#################################################################################
# Detect tree stems 
#################################################################################
#################################################################################
  ###_____________________________________________________###
  ### chunk the normalized las if not already
  ###_____________________________________________________###
  if(grid_subset_switch==F){ # the las's are not chunked with buffer
    # ###################################
    # # tile the normalized files to process with TreeLS
    # ###################################
        norm_ctg_temp = lidR::readLAScatalog(normalize_flist)
        # retile catalog
        lidR::opt_progress(norm_ctg_temp) = F
        lidR::opt_output_files(norm_ctg_temp) = paste0(config$las_stem_norm_dir,"/", "_{XLEFT}_{YBOTTOM}") # label outputs based on coordinates
        lidR::opt_chunk_buffer(norm_ctg_temp) = 10
        lidR::opt_chunk_size(norm_ctg_temp) = 100
        lidR::catalog_retile(norm_ctg_temp) # apply retile
        # create spatial index
        # note this overwrites the normalize_flist created above if new files created
        normalize_flist = create_lax_for_tiles(
          las_file_list = list.files(config$las_stem_norm_dir, pattern = ".*\\.(laz|las)$", full.names = T)
        )
        # clean up
        remove(list = ls()[grep("_temp",ls())])
        gc()
  }
  
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
        # configure parallel
        cores = parallel::detectCores()
        cluster = parallel::makeCluster(cores-1)
        # register the parallel backend with the `foreach` package
        doParallel::registerDoParallel(cluster)
        # pass to foreach to process each lidar file in parallel
          write_stem_las_ans = 
            foreach::foreach(
              i = 1:length(normalize_flist)
              , .packages = c("tools","lidR","tidyverse","doParallel","TreeLS")
              , .inorder = F
            ) %dopar% {
              write_stem_las_fn(las_path_name = normalize_flist[i], min_tree_height = minimum_tree_height_m)
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
        normalize_flist %>%
          purrr::map(write_stem_las_fn, min_tree_height = minimum_tree_height_m)
    }
  
  # create spatial index files (.lax)
  las_stem_flist = create_lax_for_tiles(
    list.files(config$las_stem_dir, pattern = ".*\\.(laz|las)$", full.names = T)
  )
  
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
  
    # ggplot() + 
    #   xlim(0, 60) + 
    #   geom_function(fun = ws_fn)

  ###___________________________________________________###
  ### Individual tree detection using CHM (top down)
  ###___________________________________________________###
    ### ITD on CHM
    # call the locate_trees function and pass the variable window
  ###############################################################################
  # make and process tiles if not in memory
  ###############################################################################
    # # !!!!!! lidR::locate_trees didn't work with large rasters, given error:
    # # !!!!!! Error: Large on-disk rasters are not supported by locate_tree. Load the raster manually.
    # # ... see: https://github.com/r-lidar/lidR/blob/9e052574adb40513e6d88ac141cfd0d41c3798ba/R/utils_raster.R
    # # ... see: https://stackoverflow.com/questions/77863431/manually-load-large-raster-to-perform-lidrlocate-trees
    # # check if need to split raster into tiles
      split_raster_fn = function(raster, n = 10){
        # puts raster in memory = raster_in_memory
        raster = raster*1 
        # check if is not in memory = raster_is_proxy
        raster_is_proxy = !terra::inMemory(raster)
        # check if memory is available = raster_fits_in_memory
        if(raster_is_proxy){
          # check memory needed
          nc = terra::nrow(raster)*terra::ncol(raster)
          n = n*terra::nlyr(raster)
          memneed = nc * n * 8L
          memavail = terra::free_RAM()*1000
          memavail = 0.6 * memavail
          is_mem_avail = memneed < memavail
        }else{is_mem_avail = T}
        # return check for splitting up raster into tiles
        split = dplyr::case_when(
          raster_is_proxy == F ~ F
          , raster_is_proxy == T & is_mem_avail == T ~ F
          , raster_is_proxy == T & is_mem_avail == F ~ T
        )
        return(split)
      }
      split_raster = split_raster_fn(chm_rast)
      
  # flow to split chm raster
  if(split_raster==T){
    # returns a list of files chm_tiles 
    chm_tiles = terra::makeTiles(
      chm_rast
      # specify rows and columns for tiles
      , y = c(
        round(terra::nrow(chm_rast)/4)
        , round(terra::ncol(chm_rast)/4)
      )
      , filename = paste0(config$temp_dir,"/tile_.tif")
      , na.rm = T
      , buffer = round(10/terra::res(chm_rast))[1] # 10m buffer
      , overwrite = T
    )
    # chm_tiles
    
    #################
    # function to locate tree tops an crowns for each tile
    #################
    trees_crowns_fn = function(rast_pth, ws = ws_fn, hmin = minimum_tree_height_m, dir = config$temp_dir){
      # read raster
      r = terra::rast(rast_pth)
      ###################
      # locate seeds
      ###################
      tree_tops = lidR::locate_trees(
        r
        , algorithm = lmf(
          ws = ws
          , hmin = hmin
        )
      )
      # write
      tree_tops_file = paste0(
        dir
        ,"/tree_tops_"
        , stringr::str_replace_all(basename(rast_pth), pattern = ".tif", replacement = ".gpkg") 
      )
      
      sf::st_write(
        tree_tops
        , tree_tops_file
        , append = F
      )
      ###################
      # crowns
      ###################
      crowns = ForestTools::mcws(
        treetops = sf::st_zm(tree_tops, drop = T) # drops z values
        , CHM = r
        , minHeight = hmin
      )
      
      # write
      crowns_file = paste0(
        dir
        ,"/crowns_"
        , basename(rast_pth)
      )
      
      terra::writeRaster(
        crowns
        , filename = crowns_file
        , overwrite = T
      )
      
      ###################
      # crowns vector
      ###################
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
      
      # write
      crowns_sf_file = stringr::str_replace_all(crowns_file, pattern = ".tif", replacement = ".gpkg")
      
      sf::st_write(
        crowns_sf
        , crowns_sf_file
        , append = F
      )
      
      # return
      return(
        dplyr::tibble(
          chm_tile = rast_pth
          , tree_tops_file = tree_tops_file
          , crowns_terra_file = crowns_file
          , crowns_sf_file = crowns_sf_file
        )
      )
    }
    
    # pass tiles to the function and return data frame of written files
    trees_crowns_data = chm_tiles %>% 
      purrr::map(trees_crowns_fn) %>% 
      dplyr::bind_rows()
    
    write.csv(trees_crowns_data, paste0(config$temp_dir, "/trees_crowns_data.csv"), row.names = F)
    # trees_crowns_data = readr::read_csv(paste0(config$temp_dir, "/trees_crowns_data.csv"))
    
    gc()
    
    #################
    # bring tiles together...but how?
    #################
    # trees_crowns_data %>% head()
    
    # find overlapping buffers to select largest crown within overlaps
    rast_list_temp = trees_crowns_data$chm_tile %>% 
      purrr::map(function(x){
        r = terra::rast(x) %>% 
          terra::classify(rcl = c(-Inf,Inf,1), others = 1)
        r[is.na(r)] = 1
        return(r)
      })
    
    # mosaic to get only buffers
    buffers_sf_temp = terra::sprc(rast_list_temp) %>% 
      terra::mosaic(fun = "sum") %>% 
      terra::classify(rcl = c(2,Inf,1), others = NA) %>% 
      # terra::plot(colNA="red")
      terra::as.polygons() %>% 
      sf::st_as_sf() %>% 
      sf::st_union()
    
    # read all crowns
    crowns_sf = trees_crowns_data$crowns_sf_file %>% 
      purrr::map(function(x){
        sf::st_read(x) %>% 
          dplyr::mutate(fnm = basename(x))
      }) %>% 
      dplyr::bind_rows()
    
    # get only crowns in buffer...make sure to return full crown and not just part in buffer
    buffer_crowns_temp = crowns_sf %>% 
      dplyr::inner_join(
        crowns_sf %>% 
          sf::st_intersection(buffers_sf_temp) %>% 
          sf::st_drop_geometry() %>% 
          dplyr::distinct(layer,fnm)
        , by = dplyr::join_by(layer,fnm)
      )
    
    # remove buffer crowns from main file to add only selected crowns in later
    crowns_sf = crowns_sf %>% 
      dplyr::anti_join(
        buffer_crowns_temp %>% sf::st_drop_geometry()
        , by = dplyr::join_by(layer,fnm)
      )
    
    # function to compare 2 crown tiles and get rid of overlaps by selecting the largest crown for each overlap
    remove_overlap_fn = function(x1, x2){
      # identify equal vectors
        equals_temp = x1 %>% 
          sf::st_join(x2, join = st_equals, left = F) %>% 
          sf::st_drop_geometry()
      
      # get spatial intersection and filter
        intersect_temp = x1 %>% 
          sf::st_intersection(x2) %>% 
          dplyr::mutate(area = sf::st_area(.) %>% as.numeric) %>% 
          sf::st_drop_geometry() %>% 
          dplyr::filter(area>0) %>% 
          # remove exact matches in x1
          dplyr::anti_join(
            equals_temp
            , by = dplyr::join_by(layer == layer.x, fnm == fnm.x)
          ) %>% 
          # remove exact matches in x2
          dplyr::anti_join(
            equals_temp
            , by = dplyr::join_by(layer.1 == layer.y, fnm.1 == fnm.y)
          ) %>% 
          # make one comparison based on x2...similar to sf::st_join(largest = T)
          dplyr::group_by(layer.1,fnm.1) %>% 
          dplyr::filter(area == max(area)) %>% 
          dplyr::ungroup() %>% 
          # what NOT to keep...get list of polygons that intersect and get rid of the smaller one
          dplyr::mutate(
            layer = dplyr::case_when(
              crown_area_m2 > crown_area_m2.1 ~ layer.1
              , crown_area_m2 < crown_area_m2.1 ~ layer
              , T ~ layer.1
            )
            , fnm = dplyr::case_when(
              crown_area_m2 > crown_area_m2.1 ~ fnm.1
              , crown_area_m2 < crown_area_m2.1 ~ fnm
              , T ~ fnm.1
            )
          ) %>%
          dplyr::distinct(layer, fnm)
      
      x1_new = x1 %>% 
        # filter to keep biggest when area is not equal
        dplyr::anti_join(
          intersect_temp
          , by = dplyr::join_by("layer","fnm")
        ) 
      
      x2_new = x2 %>% 
        # filter to keep biggest when area is not equal
        dplyr::anti_join(
          intersect_temp
          , by = dplyr::join_by("layer","fnm")
        ) %>% 
        # filter to keep x1 when area is equal
        dplyr::anti_join(
          equals_temp %>% 
            dplyr::distinct(layer.y, fnm.y)
          , by = dplyr::join_by(layer == layer.y, fnm == fnm.y)
        ) 
      
      # identify remaining overlaps
      olap_temp = x1_new %>% 
        sf::st_intersection(x2_new) %>% 
        dplyr::mutate(area = sf::st_area(.) %>% as.numeric()) %>% 
        dplyr::filter(area > 0) %>% 
        sf::st_union()
      
      if(dplyr::coalesce(length(olap_temp),0)>0){
        # combine filtered crowns
        new_dta = dplyr::bind_rows(x1_new, x2_new) %>% 
          # remove remaining overlaps
          sf::st_difference(olap_temp) %>% 
          dplyr::mutate(area = sf::st_area(.) %>% as.numeric()) %>% 
          sf::st_make_valid() %>% 
          dplyr::filter(
            area > 0.1
            & sf::st_is_valid(.)
            & !sf::st_is_empty(.)
          ) %>%
          dplyr::select(-c(area))
      }else{
        # combine filtered crowns
        new_dta = dplyr::bind_rows(x1_new, x2_new) %>% 
          dplyr::mutate(area = sf::st_area(.) %>% as.numeric()) %>% 
          sf::st_make_valid() %>% 
          dplyr::filter(
            area > 0.1
            & sf::st_is_valid(.)
            & !sf::st_is_empty(.)
          ) %>%
          dplyr::select(-c(area))
      }
      
      return(new_dta)
    }
    
    # list of tiles with crowns in buffered area
    fnm_list_temp = buffer_crowns_temp$fnm %>% unique()
    
    # start with the first tile file
    if(nrow(buffer_crowns_temp)>0){
      keep_buffer_crowns_temp = buffer_crowns_temp %>% 
        dplyr::filter(fnm == fnm_list_temp[1])
    }else{keep_buffer_crowns_temp = dplyr::slice_sample(crowns_sf, n = 0)}
    
    # sequentially add tiles and remove overlaps
    if(length(fnm_list_temp)>1){
      i = 2
      while(i<=length(fnm_list_temp)){
        # read in next file
        x2_temp = buffer_crowns_temp %>% 
          dplyr::filter(fnm == fnm_list_temp[i])
        # remove overlaps
        keep_buffer_crowns_temp = remove_overlap_fn(keep_buffer_crowns_temp, x2_temp)
        # clean and increment
        remove(x2_temp)
        gc()
        i = i+1
      }  
    }
    
    # add keeps back to crowns data
    crowns_sf = crowns_sf %>% 
      dplyr::bind_rows(keep_buffer_crowns_temp) %>% 
      # generate tree id
      dplyr::mutate(treeID = dplyr::row_number()) %>% 
      dplyr::relocate(treeID)
    
    # clean up
      remove(list = ls()[grep("_temp",ls())])
      gc()
    
    # join tree tops
    tree_tops_temp = 
      1:nrow(trees_crowns_data) %>%
      purrr::map(function(x){
        sf::st_read(trees_crowns_data$tree_tops_file[x]) %>% 
          dplyr::mutate(fnm = basename(trees_crowns_data$crowns_sf_file[x])) %>% 
          dplyr::rename(
            layer = treeID
            , tree_height_m = Z
          ) %>% 
          dplyr::mutate(
            tree_x = sf::st_coordinates(.)[,1]
            , tree_y = sf::st_coordinates(.)[,2]
          )
      }) %>% 
      dplyr::bind_rows()
    # tree_tops_temp %>% dplyr::glimpse()
      
    # keep tree tops with crown match
      # note that the spatial location of a tree point might not fall within a crown vector
      # based on the vector trimming above...that's ok for our purposes as it won't impact the calculations
    crowns_sf = crowns_sf %>% 
      dplyr::inner_join(
        tree_tops_temp %>% sf::st_drop_geometry()
        , by = dplyr::join_by(layer==layer, fnm==fnm)
      ) %>% 
      dplyr::select(-c(layer,fnm))
    
    # clean up
    remove(list = ls()[grep("_temp",ls())])
    gc()
    ####### !!!!! TO AVOID POTENTIAL MEMORY ISSUES...COMPETITION METRICS NOT CALCULATED FOR LARGE CHM DATA SETS
    competition_buffer_m = as.numeric(NA)
    # start time
    xx9_competition_start_time = Sys.time()
  }else{ # if(split_raster==T)
    # identify tree tops
    tree_tops = lidR::locate_trees(
      chm_rast
      , algorithm = lmf(
        ws = ws_fn
        , hmin = minimum_tree_height_m
      )
    )
    # delineate crowns
    crowns = ForestTools::mcws(
      treetops = sf::st_zm(tree_tops, drop = T) # drops z values
      , CHM = chm_rast
      , minHeight = minimum_tree_height_m
    )
    
    ### Write the crown raster to the disk
    terra::writeRaster(
      crowns
      , paste0(config$delivery_dir, "/top_down_detected_tree_crowns.tif")
      , overwrite = TRUE
    )

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
            sf::st_as_sf() %>% 
            sf::st_transform(sf::st_crs(tree_tops))
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
    }
  # clean up
    remove(list = ls()[grep("_temp",ls())])
    gc()
  
# start time
  xx10_estdbh_start_time = Sys.time()
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
          , SPECIES_SYMBOL
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
      dplyr::select(-c(statuscd,dia,ht))
    
    # save training data
    ### export tabular
      write.csv(
          treemap_trees_df
          , paste0(config$delivery_dir, "/regional_dbh_height_model_training_data.csv")
          , row.names = F
        )
    
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
      , iter = 4000, warmup = 2000, chains = 4
      , cores = lasR::half_cores()
      , file = paste0(config$delivery_dir, "/regional_dbh_height_model")
      , file_refit = "always"
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
            , tidyselect::starts_with("min_crown_ht_m")
            , tidyselect::starts_with("comp_")
          ))
      # dbh_training_data_temp %>% dplyr::glimpse()
      if(nrow(dbh_training_data_temp)>10){
        ###__________________________________________________________###
        ### Build local model to estimate missing DBHs using SfM DBHs
        ###__________________________________________________________###
        # Use the SfM-detected stems remaining after the filtering workflow 
        # for the local DBH to height allometric relationship model.
        if(tolower(local_dbh_model) == "rf"){
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
            , iter = 4000, warmup = 2000, chains = 4
            , cores = lasR::half_cores()
            , file = paste0(config$delivery_dir, "/local_dbh_height_model")
            , file_refit = "always"
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
    
    ### write the data to the disk
    if(split_raster==T){
      # split up the detected crowns
      crowns_sf_with_dbh = crowns_sf_with_dbh %>% 
        dplyr::arrange(as.numeric(tree_x),as.numeric(tree_y)) %>% 
        # groups of 500k
        dplyr::mutate(grp = ceiling(dplyr::row_number()/500e3))
        
      write_fnl_temp = crowns_sf_with_dbh$grp %>% 
        unique() %>% 
        purrr::map(function(x){
          ### write the data to the disk
          # crown vector polygons
          sf::st_write(
            crowns_sf_with_dbh %>% 
              dplyr::filter(grp == x) %>% 
              dplyr::select(-c(grp))
            , paste0(config$delivery_dir, "/final_detected_crowns_",x,".gpkg")
            , append = FALSE
            , quiet = TRUE
          )
          # tree top vector points
          sf::st_write(
            # get tree points
            crowns_sf_with_dbh %>% 
              dplyr::filter(grp == x) %>% 
              dplyr::select(-c(grp)) %>% 
              sf::st_drop_geometry() %>% 
              sf::st_as_sf(coords = c("tree_x", "tree_y"), crs = sf::st_crs(crowns_sf_with_dbh))
            , paste0(config$delivery_dir, "/final_detected_tree_tops_",x,".gpkg")
            , append = FALSE
            , quiet = TRUE
          )
          return(
            dplyr::tibble(
              crowns_file = paste0(config$delivery_dir, "/final_detected_crowns_",x,".gpkg")
              , trees_file = paste0(config$delivery_dir, "/final_detected_tree_tops_",x,".gpkg")
            )
          )
        }) %>% 
        dplyr::bind_rows()
    }else{
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
    }

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
        timer_tile_time_mins = difftime(xx2_class_dtm_norm_chm_start_time, xx1_tile_start_time, units = c("mins")) %>% 
          as.numeric()
        , timer_class_dtm_norm_chm_time_mins = difftime(xx7_treels_start_time, xx2_class_dtm_norm_chm_start_time, units = c("mins")) %>% 
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
        , sttng_accuracy_level = accuracy_level
        , sttng_pts_m2_for_triangulation = max_pts_m2
        , sttng_normalization_with = ifelse(as.numeric(accuracy_level) %in% c(2,3),"triangulation","dtm")
        , sttng_competition_buffer_m = competition_buffer_m
      )

  # write 
  write.csv(
    return_df
    , paste0(config$delivery_dir, "/processed_tracking_data.csv")
    , row.names = F
  )
    