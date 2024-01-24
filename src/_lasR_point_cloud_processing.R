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
  # input_las_dir = "../data/big_las_raw"
  input_las_dir = "../data/small_las_raw"
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
  library(parallel) # parallel
  library(doParallel)
  library(foreach) # facilitates parallelization by lapply'ing %dopar% on for loop
  
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
    las_classify_dir = file.path(temp_dir, "00_classify")
    las_normalize_dir = file.path(temp_dir, "01_normalize")
    dtm_dir = file.path(temp_dir, "02_dtm")
    chm_dir = file.path(temp_dir, "03_chm")
    treetops_dir = file.path(temp_dir, "04_treetops")
    treecrowns_dir = file.path(temp_dir, "05_treecrowns")
    
    ### Create the directories
    dir.create(delivery_dir, showWarnings = FALSE)
    dir.create(temp_dir, showWarnings = FALSE)
    dir.create(las_classify_dir, showWarnings = FALSE)
    dir.create(las_normalize_dir, showWarnings = FALSE)
    dir.create(dtm_dir, showWarnings = FALSE)
    dir.create(chm_dir, showWarnings = FALSE)
    dir.create(treetops_dir, showWarnings = FALSE)
    dir.create(treecrowns_dir, showWarnings = FALSE)
    
    ###______________________________###
    ### Set names of the directories ###
    ###______________________________###
    
    names(rootdir) = "rootdir"
    names(input_las_dir) = "input_las_dir"
    names(input_treemap_dir) = "input_treemap_dir"
    names(delivery_dir) = "delivery_dir"
    names(temp_dir) = "temp_dir"
    names(las_classify_dir) = "las_classify_dir"
    names(las_normalize_dir) = "las_normalize_dir"
    names(dtm_dir) = "dtm_dir"
    names(chm_dir) = "chm_dir"
    names(treetops_dir) = "treetops_dir"
    names(treecrowns_dir) = "treecrowns_dir"
    
    ###______________________________###
    ### Append to output config list ###
    ###______________________________###
    
    config = cbind(
      rootdir, input_las_dir, input_treemap_dir
      , delivery_dir, temp_dir, las_classify_dir
      , las_normalize_dir, dtm_dir, chm_dir
      , treetops_dir, treecrowns_dir
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
# Function to create spatial index files (.lax) for classified las
#################################################################################
#################################################################################
  ## see: https://r-lidar.github.io/lidRbook/spatial-indexing.html
  ## !!!! NOTE: THIS UTILIZES PARALLEL COMPUTING WHICH MAY OR MAY NOT WORK ON PROCESSING MACHINE
  ## !!!! NOTE: THIS UTILIZES PARALLEL COMPUTING WHICH MAY OR MAY NOT WORK ON PROCESSING MACHINE
  ### Function to generate .lax index files for input directory path
  create_lax_for_tiles = function(las_file_list){
    ## desired_las_dir = config$input_las_dir
    ###__________________________________________###
    ### Create a lax index file for the las file ###
    ###__________________________________________###
    # message(paste0("Initializing .lax indexing for ", desired_las_dir, " ... "))
    # las_list = list.files(desired_las_dir, pattern = ".las")
    # laz_list = list.files(desired_las_dir, pattern = ".laz")
    # lidar_list = append(las_list, laz_list)
    lidar_list = las_file_list
    
    # message("Indexing ", length(lidar_list), " las files ... ")
    
    # start_time = Sys.time()
    # configure parallel
    cores = parallel::detectCores()
    cluster = parallel::makeCluster(cores)
    # register the parallel backend with the `foreach` package
    doParallel::registerDoParallel(cluster)
    # pass to foreach to process each lidar file in parallel
    foreach::foreach(i = 1:length(lidar_list)) %dopar% {
      
      ### Get the desired file
      des_file = lidar_list[i]
      # des_file
      
      ### Compile the .lax file name
      des_file_lax = tools::file_path_sans_ext(des_file)
      des_file_lax = paste0(des_file_lax, ".lax")
      
      # des_file_lax_path = paste0(desired_las_dir, "/", des_file_lax)
      # # des_file_lax_path
      
      ### See if the .lax version exists in the input directory
      does_file_exsist = file.exists(des_file_lax)
      # does_file_exsist
      
      ### If file exsists, do nothing
      if(does_file_exsist == TRUE){return(NULL)}
      
      ### If file doesnt exsist, create a .lax index
      if(does_file_exsist == FALSE){
        
        ### Append the directory path to the las file
        # path = paste0(desired_las_dir, "/", des_file)
        
        ### Write index
        rlas::writelax(des_file)
        
      }
      
    }
    parallel::stopCluster(cluster)
    # end_time = Sys.time()
    # total_time = difftime(end_time, start_time, units = c("mins"))
    # message("Total lax index time took ", total_time, " minutes ... ")
  }

#################################################################################
#################################################################################
# Set up file names and checks
#################################################################################
#################################################################################
  ###______________________________###
  # set up lasR read file list
  ###______________________________###
    raw_las_files = list.files(config$input_las_dir, pattern = ".*\\.(laz|las)$", full.names = T)
    # pull crs for using in write operations
    proj_crs = lidR::readLAScatalog(raw_las_files) %>% sf::st_crs() %>% purrr::pluck(1)

#################################################################################
#################################################################################
# Denoise raw point cloud
#################################################################################
#################################################################################
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

#################################################################################
#################################################################################
# Classify ground points and height normalize
#################################################################################
#################################################################################
    
  ###______________________________###
  # classify ground points
  ###______________________________###
    # There is no function in lasR to classify the points...create one
    lasr_classify_ground = function(
      data
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
      return(classify)
    }
 
  ###______________________________###
  # variable window function for tree top identification
  ###______________________________### 
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
    
  ###______________________________###
  # set up lasR pipeline steps
  ###______________________________###
    # read denoise files
      lasr_read_step = lasR::reader(las_denoise_flist, filter = "-drop_noise -drop_duplicates")
    # classify ground
      lasr_classify_step = lasr_classify_ground()
    # write ground classification
      lasr_write_classify_step = lasR::write_las(
        ofile = paste0(config$las_classify_dir, "/*_classify.las")
      )
    # Delaunay triangulation for use with normalize and DTM
      lasr_tri_step = lasR::triangulate(
        max_edge = c(0,1)
        , filter = lasR::keep_ground()
      )
    # height normalize
      lasr_normalize_step = lasR::transform_with(
        stage = lasr_tri_step
        , operator = "-"
      )
    # write height normalized
      lasr_write_normalize_step = lasR::write_las(
        ofile = paste0(config$las_normalize_dir, "/*_normalize.las")
      )
    # create chm
      lasr_chm_step = lasR::rasterize(
        res = 0.5
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
        , ofile = paste0(config$chm_dir, "/*_chm_1m.tif")
      )
    # Identify tree tops using a local maximum algorithm identifies points that are locally maximum with a custom window size
      lasr_treetops_step = lasR::local_maximum(
        ws = 3 #ws_fn
        , min_height = minimum_tree_height
        , ofile = paste0(config$treetops_dir, "/*_treetops.gpkg")
        , use_attribute = "Z"
      )
    # delineate crowns using region growing for individual tree segmentation based on Dalponte and Coomes (2016)
      lasr_treecrowns_step = lasR::region_growing(
        raster = lasr_chm_pit_step
        , seeds = lasr_treetops_step
        , th_tree = minimum_tree_height
        , th_seed = 0.45
        , th_cr = 0.55
        , max_cr = 20
        , ofile = paste0(config$treecrowns_dir, "/*_treecrowns.tif")
      )
    # create dtm
      lasr_dtm_step = lasR::rasterize(
        res = 1
        , lasr_tri_step
      )
    # Pits and spikes filling for raster with algorithm from St-Onge 2008 (see reference).
      lasr_dtm_pit_step = lasR::pit_fill(
        raster = lasr_dtm_step
        , ofile = paste0(config$dtm_dir, "/*_dtm_1m.tif")
      )
  
  ###______________________________###
  # pass to lasR::processor
  ###______________________________###
    ans = lasR::processor(
      # read denoise files
        lasr_read_step +
      # classify ground
        lasr_classify_step +
      # write ground classification
        lasr_write_classify_step +
      # Delaunay triangulation for use with normalize and DTM
        lasr_tri_step +
      # height normalize
        lasr_normalize_step +
      # write height normalized
        lasr_write_normalize_step +
      # create chm
        lasr_chm_step +
      # Pits and spikes filling for raster with algorithm from St-Onge 2008 (see reference).
        lasr_chm_pit_step +
      # Identify tree tops using a local maximum algorithm identifies points that are locally maximum with a custom window size
        lasr_treetops_step +
      # delineate crowns using region growing for individual tree segmentation based on Dalponte and Coomes (2016)
        lasr_treecrowns_step +
      # create dtm
        lasr_dtm_step +
      # Pits and spikes filling for raster with algorithm from St-Onge 2008 (see reference).
        lasr_dtm_pit_step
    )
    
    ans %>% str()
    