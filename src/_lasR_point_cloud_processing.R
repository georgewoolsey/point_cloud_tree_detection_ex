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
  input_las_dir = "../data/las_raw"
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
  
  # spatial analysis
  library(terra) # raster
  library(sf) # simple features
  
  # point cloud processing
  library(lidR)
  library(ForestTools) # for crown delineation but relies on depreciated `raster`
  
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
  
  remotes::install_github("r-lidar/lasR")

  # modeling
  library(randomForest)
  library(RCSF) # for the cloth simulation filter (csf) to classify points
  library(brms) # bayesian modelling using STAN engine
  
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
    delivery_dir = file.path(rootdir, "point_cloud_processing_delivery")
    
    ### Create the directories
    dir.create(delivery_dir, showWarnings = FALSE)
    
    ###______________________________###
    ### Set names of the directories ###
    ###______________________________###
    
    names(rootdir) = "rootdir"
    names(input_las_dir) = "input_las_dir"
    names(input_treemap_dir) = "input_treemap_dir"
    names(delivery_dir) = "delivery_dir"
    
    ###______________________________###
    ### Append to output config list ###
    ###______________________________###
    
    config = cbind(rootdir, input_las_dir, input_treemap_dir, delivery_dir)
    
    config = as.data.frame(config)
    #config
    
    ### Return config 
    return(config)
    
  }
  # call the function
  config = create_project_structure(rootdir, input_las_dir)

#################################################################################
#################################################################################
# Point Cloud Processing
#################################################################################
#################################################################################
  ###______________________________###
  # set up lasR read
  ###______________________________###
  # This is the first stage that must be called in each lasR pipeline
    # It specifies which files must be read. The stage does nothing and returns nothing 
    # if it is not associated to another processing stage. It only initializes the pipeline. 
    # reader() is the main function that dispatches into to other functions.
  lasr_read_step = lasR::reader(
    x = list.files(input_las_dir, pattern = ".*\\.(laz|las)$", full.names = T)
    # , buffer = 20 # will be larger if the internal routine suggests
  )
  
  ###______________________________###
  # denoise the raw las
  ###______________________________###
  las_denoise_temp = lasR::processor(
    lasr_read_step + 
      lasR::classify_isolated_points() + 
      lasR::write_las(filter = "-drop_noise -drop_duplicates")
  )
  # lidR::readLAS(las_denoise_temp) %>% plot()
  # lidR::readLAS(las_denoise_temp)@data %>% dplyr::count(Classification)
  # lidR::readLAS(las_denoise_temp) %>% lidR::header()
  
  ###______________________________###
  # classify points
  ###______________________________###
  # There is no function in lasR to classify the points...create one to pass to lasR pipeline
  # classify the points outside of lasR pipeline and re-write to temp dir
  lidR::readLAScatalog(dirname(las_denoise_temp))
  
  classify_points_fn <- function(data, header){
    # classify ground with noise removed
    las_class = lidR::classify_ground(
      lidR::LAS(data, header = header)
      , algorithm = csf(rigidness = 1, sloop_smooth = TRUE)
    )
    return(las_class@data)
  }
  # wrap new function in lasR::callback
  lasr_call_classify_step <- lasR::callback(
    classify_points_fn
    , expose = "xyzc"
    , header = lidR::readLASheader(las_denoise_temp[1])
    , drop_buffer = F
  )
  
  # testing
    # las_class_temp = lasR::processor(
    #   lasR::reader(las_denoise_temp) + 
    #     lasr_call_classify_step + 
    #     lasR::write_las()
    # )
    # lidR::readLAS(las_class_temp)@data %>% dplyr::count(Classification)
  
  ###______________________________###
  # create DTM using Delaunay triangulation
  ###______________________________###
    # Delaunay triangulation
    lasr_tri_step = lasR::triangulate(filter = lasR::keep_ground())
    # rasterize the result of the Delaunay triangulation
    lasr_dtm_step = lasR::rasterize(res = 1, lasr_tri_step)
    # Pits and spikes filling for raster with algorithm from St-Onge 2008 (see reference).
    lasr_pitfill_step = lasR::pit_fill(lasr_dtm_step, ofile = paste0(config$delivery_dir,"/dtm_1m.tif"))
  
  ###______________________________###
  # execute lasR pipeline
  ###______________________________###
  # dtm_temp = 
  lasR::processor(
    lasR::reader(las_denoise_temp) + 
      lasr_call_classify_step + 
      lasr_tri_step +
      lasr_dtm_step +
      lasr_pitfill_step
  )
  
  terra::rast(paste0(config$delivery_dir,"/dtm_1m.tif")) %>% plot()
  # terra::rast(paste0(config$delivery_dir,"/dtm_1m.tif")) %>% terra::crs()
  
  # filter_temp = "-drop_noise -drop_duplicates"
  # triangulate(filter = filter_temp)
  # lasR::processor(lasr_read_step + lasR::classify_isolated_points() + triangulate(filter = filter_temp))
  