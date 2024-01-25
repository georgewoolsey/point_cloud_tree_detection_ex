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
    las_grid_dir = file.path(temp_dir, "00_grid")
    las_classify_dir = file.path(temp_dir, "01_classify")
    las_normalize_dir = file.path(temp_dir, "02_normalize")
    dtm_dir = file.path(temp_dir, "03_dtm")
    chm_dir = file.path(temp_dir, "04_chm")
    treetops_dir = file.path(temp_dir, "05_treetops")
    treecrowns_dir = file.path(temp_dir, "06_treecrowns")
    
    ### Create the directories
    dir.create(delivery_dir, showWarnings = FALSE)
    dir.create(temp_dir, showWarnings = FALSE)
    dir.create(las_grid_dir, showWarnings = FALSE)
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
    names(las_grid_dir) = "las_grid_dir"
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
      , delivery_dir, temp_dir, las_grid_dir, las_classify_dir
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
      does_file_exist = file.exists(des_file_lax)
      # does_file_exist
      
      ### If file does_file_exist, do nothing
      if(does_file_exist == TRUE){return(NULL)}
      
      ### If file doesnt exsist, create a .lax index
      if(does_file_exist == FALSE){
        
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
  length(raw_las_files) != length(las_classify_flist)
  | length(raw_las_files) != length(classify_lax_files)
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
  length(raw_las_files) != length(las_classify_flist)
  | length(raw_las_files) != length(classify_lax_files)
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
  length(raw_las_files) != length(las_normalize_flist)
  | length(raw_las_files) != length(normalize_lax_files)
){
    

  # turn on parallel
  # las_normalize_fn = function(flist = las_classify_flist, out_dir = config$las_normalize_dir){
  #   las_classify_ctg = lidR::readLAScatalog(flist)
  #   opt_output_files(las_classify_ctg) = paste0(out_dir, "/{*}_normalize")
  #   cores = parallel::detectCores()
  #   cl = parallel::makeCluster(cores)
  #   on.exit(parallel::stopCluster(cl))
  #   lidR::normalize_height(las_classify_ctg, algorithm = knnidw())
  # }
  # ### execute
  # las_normalize_ans = las_normalize_fn()
  # las_normalize_ans
  
  las_classify_ctg = lidR::readLAScatalog(las_classify_flist)
  opt_output_files(las_classify_ctg) = paste0(config$las_normalize_dir, "/{*}_normalize")
  las_normalize_ans = lidR::normalize_height(las_classify_ctg, algorithm = knnidw())
  gc()
  
  # create spatial index files (.lax)
    las_normalize_flist = list.files(config$las_normalize_dir, pattern = ".*\\.(laz|las)$", full.names = T)
    create_lax_for_tiles(
      las_file_list = las_normalize_flist
    )
  
  # list.files(las_normalize_flist)[1] %>% 
  #   lidR::readLAS() %>% 
  #   plot()
  # 
}
    

#################################################################################
#################################################################################
# Height normalize points and create canopy height model (CHM) raster
#################################################################################
#################################################################################

# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# this is really slow with the lasR::normalize pipeline...try another way
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

if(
  FALSE
  # do las and lax files already exist?
  # length(raw_las_files) != length(las_normalize_flist)
  # | length(raw_las_files) != length(normalize_lax_files)
  # | (file.exists(chm_file_name) == F)
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
        , filename = chm_file_name
        , overwrite = T
      )
      
      # chm_file_name %>% 
      #   terra::rast() %>% 
      #   terra::crs()
      #   plot()
    
    # clean up
    remove(list = ls()[grep("_temp",ls())])
    gc()
}else if(file.exists(chm_file_name) == T){
  chm_rast = terra::rast(chm_file_name)
}

#################################################################################
#################################################################################
# do something
#################################################################################
#################################################################################
