################################################################################
################################################################################
################################################################################
### 2024-03-18 lasR 0.3.2 updates
################################################################################
################################################################################
################################################################################

########## !!!!!!!!!!!!!!!!! run point_cloud_processing.r to line 328...thru: 

library(tidyverse)
library(sf)
library(terra)
library(lidR)
library(lasR)
las_ctg = lidR::readLAScatalog(
  "c:/data/usfs/point_cloud_tree_detection_ex/data/testtest/"
  # "c:/data/usfs/point_cloud_tree_detection_ex/data/lower_sherwin/"
)
plot(las_ctg, mapview = TRUE) #, map.type = "Esri.WorldImagery")
lidR::las_check(las_ctg)
las_ctg@data$CRS
las_ctg
# reproject
las = lidR::readLAS(las_ctg@data$filename[1]) # read individual file

las_ctg@data %>% dplyr::filter(dplyr::row_number()==1) %>% 
  sf::st_centroid() %>% 
  sf::st_buffer(20) %>%  
  ggplot() +
    geom_sf(fill = "black") +
    geom_sf(data = las_ctg@data %>% dplyr::filter(dplyr::row_number()==1), fill = NA)
  

las$Classification %>% table()

# las %>% 
#   lidR::clip_roi(las_ctg@data %>% dplyr::filter(dplyr::row_number()==1) %>% 
#   sf::st_centroid() %>% 
#   sf::st_buffer(100)) %>% 
#   plot(color = "Classification")

w = lasR::write_las(ofile = "c:/data/usfs/point_cloud_tree_detection_ex/data/testtest/usgs_cp.las")
tri = lasR::triangulate(filter = lasR::keep_ground())
dtm = lasR::rasterize(res = 1, operators = tri)
norm = lasR::transform_with(stage = tri)
chm = lasR::rasterize(res = 1, filter = "-keep_z_above 4")
pit = lasR::pit_fill(raster = chm)
ans = lasR::exec(
  # w + tri + dtm + norm + chm + pit
  tri + dtm + norm + chm + pit
  , on = las
    # las %>% 
    # lidR::clip_roi(las_ctg@data %>% dplyr::filter(dplyr::row_number()==1) %>% 
    # sf::st_centroid() %>% 
    # sf::st_buffer(1000))
)
ans
ans[[1]] %>% plot()
ans[[2]] %>% plot()
ans[[2]] %>% 
  terra::focal(
    w = 3
    , fun = "mean"
    , na.rm = T
    # na.policy Must be one of:
      # "all" (compute for all cells)
      # , "only" (only for cells that are NA)
      # , or "omit" (skip cells that are NA).
    , na.policy = "only"
  ) %>% plot()
ans[[4]] %>% plot()

mapview::mapviewOptions(basemaps = "Esri.WorldImagery")
ans[[2]] %>% 
  terra::focal(
    w = 3
    , fun = "mean"
    , na.rm = T
    # na.policy Must be one of:
      # "all" (compute for all cells)
      # , "only" (only for cells that are NA)
      # , or "omit" (skip cells that are NA).
    , na.policy = "only"
  ) %>% 
  stars::st_as_stars() %>% 
  sf::st_set_crs(paste0("EPSG:", 6430)) %>% 
  mapview::mapview(na.color = "transparent", alpha.regions = 0.7)


sf::st_crs(las)$epsg
# sf::st_crs(las) = paste0("EPSG:", 6430)
sf::st_crs(las)$epsg

is.null(sf::st_crs(las))
plot(las, mapview = TRUE) #, map.type = "Esri.WorldImagery")
las_new = sf::st_transform(las, paste0("EPSG:", "4326")) # this is the only transform for "6430" https://epsg.io/6430
las_new = sf::st_transform(las, paste0("EPSG:", "26913"))
# las_new = sf::st_transform(las, crs = "26913")
sf::st_crs(las_new)
plot(las_new, mapview = TRUE) #, map.type = "Esri.WorldImagery")
# 
# function to reproject
# see : https://gis.stackexchange.com/questions/371566/can-i-re-project-an-las-file-in-lidr
reproject_las_fn <- function(filepath, new_crs = NULL, old_crs = NULL, outdir = getwd()) {
  if(is.null(new_crs)){stop("the new_crs must be provided")}
  # read individual file
  las = lidR::readLAS(filepath)
  if(
    (is.null(sf::st_crs(las)) | is.na(sf::st_crs(las)))
    & is.null(old_crs)
  ){
    stop("the raw las file has missing CRS and cannot be transformed. try setting old_crs if known")
  }else{
    # transform if know old crs
    if(
      is.null(sf::st_crs(las)$epsg) | is.na(sf::st_crs(las)$epsg)
      & !is.null(old_crs)
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
# map over files
new_flist_temp = las_ctg@data$filename %>%
  purrr::map(
    reproject_las_fn
    , new_crs = "26913"
    , old_crs = "6430"
    , outdir = "c:/data/usfs/point_cloud_tree_detection_ex/data/testtest"
  ) %>%
  c() %>%
  unlist()

new_flist_temp %>% unlist()
las = lidR::readLAS(new_flist_temp[1])



library(future)

# remove all files in delivery and temp
list.files(config$temp_dir, recursive = T, full.names = T) %>%
    purrr::map(file.remove)
list.files(config$delivery_dir, recursive = T, full.names = T) %>%
    purrr::map(file.remove)

xxst_time = Sys.time()

######################################
# chunk for large area/pt pt clouds
# adpative retiling w/ minimal (no?) user input
######################################
  # for super big ctgs...lots of different stuff needs to happen to reduce memory issues
  max_ctg_pts = 70e6 # max points to process in any ctg using lasR at one time
  ctg_pts_so_many = sum(las_ctg@data$Number.of.point.records) > max_ctg_pts
  # choose processing
  # accuracy_level = 1 # uses DTM to height normalize the points
  # accuracy_level = 2 # uses triangulation with low point density (20 pts/m2) to height normalize the points
  # accuracy_level = 3 # uses triangulation with high point density (100 pts/m2) to height normalize the points
  accuracy_level = 2
  # set maximums based on sample data testing
  #### These maximums chunk the data
    # this one is less important as never experienced issues with large areas
    max_area_m2 = 90e3 # original = 90e3
  #### This maximum filters the ground points to perform Delaunay triangulation
    # see: https://github.com/r-lidar/lasR/issues/18#issuecomment-2027818414
    max_pts_m2 = dplyr::case_when(
      as.numeric(accuracy_level) <= 2 ~ 20 
      , as.numeric(accuracy_level) == 3 ~ 100
      , T ~ 20
    )
    ## at 100 pts/m2, the resulting CHM pixel values (0.25m resolution) are within -0.009% and 0.026%
      # ... of the values obtained by using 400 pts/m2 with 99% probability
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
    plot(las_ctg)
    ########################
    # split into grid subsets
    # this is done for ctg's with sooo many points
    # because lasR pipeline crashes memory and only way around it is 
    # to break out steps into separarte chunks (requires reading data multiple times..sooo inefficient)
    # and then to use lidR for classify and normalize steps which means lasR is marginalized
    # ....
    # objective here is to break the ctg into lasR manageable subsets: each subset goes through lasR pipeline individually
    # then bring the DTM and CHM results together via terra::sprc and mosaic:
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
      # different buffer depending on accuracy level
      lidR::opt_chunk_buffer(las_ctg) = ifelse(
        # buffering here because these grid subsets will be processed independently
        as.numeric(accuracy_level) %in% c(2,3)
        , 10
        # not buffering here because these grid subsets will be processed altogether with buffer set for lasR::exec
        , 0
      )
      lidR::opt_chunk_size(las_ctg) = ctg_chunk_data$chunk_max_ctg_pts[1] # retile
      # # opt_chunk_alignment(las_ctg) = c(1000*runif(1), 1000*runif(1)) # this doesn't matter so much
      # # https://cran.r-project.org/web/packages/lidR/vignettes/lidR-LAScatalog-engine.html#control-of-the-chunk-alignment
      # reset las_ctg
      lidR::catalog_retile(las_ctg) # apply retile
      lidR::opt_progress(las_ctg) = T
      # create spatial index
      flist_temp = create_lax_for_tiles(
        las_file_list = list.files(config$las_grid_dir, pattern = ".*\\.(laz|las)$", full.names = T)
      )
      # switch for processing grid subsets
      grid_subset_switch_temp = ifelse(
        # buffering here because these grid subsets will be processed independently
        as.numeric(accuracy_level) %in% c(2,3)
        , T
        # not buffering here because these grid subsets will be processed altogether with buffer set for lasR::exec
        , F
      )
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
      lidR::opt_chunk_buffer(las_ctg) = 0 # ctg_chunk_data$buffer[1]
      lidR::opt_chunk_size(las_ctg) = ctg_chunk_data$chunk[1] # retile
      # reset las_ctg
      lidR::catalog_retile(las_ctg) # apply retile
      lidR::opt_progress(las_ctg) = T
      # create spatial index
      flist_temp = create_lax_for_tiles(
        las_file_list = list.files(config$las_grid_dir, pattern = ".*\\.(laz|las)$", full.names = T)
      )
      # switch for processing grid subsets
      grid_subset_switch_temp = F
    }else{grid_subset_switch_temp = F}
      # flist_temp
      # grid_subset_switch_temp
      plot(lidR::readLAScatalog(flist_temp))
      # lidR::readLAScatalog(flist_temp)@data %>% dplyr::glimpse()
      # lidR::readLAScatalog(flist_temp)@data %>% 
      #   dplyr::select(filename, Number.of.point.records) %>% 
      #   sf::st_drop_geometry()
  
  ########################
  # get pct filter for creating dtm and normalized point clouds using triangulation
  ########################
    process_data =
      lidR::readLAScatalog(flist_temp)@data %>%
      dplyr::ungroup() %>% 
      dplyr::mutate(
        processing_grid = dplyr::case_when(
          grid_subset_switch_temp == T ~ dplyr::row_number()
          , T ~ 1
        )
        , area_m2 = sf::st_area(geometry) %>% as.numeric()
        , pts = Number.of.point.records
        , pts_m2 = pts/area_m2
      ) %>% 
      sf::st_drop_geometry() %>% 
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
    lidR::readLAScatalog(process_data$filename)@data %>%
      dplyr::inner_join(process_data, by = dplyr::join_by("filename")) %>%
      dplyr::group_by(processing_grid) %>%
      dplyr::mutate(
        pts = Number.of.point.records, tot_pts = sum(pts)
        , lab = pts == max(pts)
      ) %>%
      ggplot() +
        # geom_sf(aes(fill = tot_pts), alpha = 0.6) +
        geom_sf(aes(fill = as.factor(processing_grid)), alpha = 0.6) +
        geom_sf_text(aes(label = ifelse(lab,as.factor(processing_grid), ""))) +
        # scale_fill_viridis_c(labels = scales::comma_format())
        scale_fill_viridis_d(option = "turbo") +
        labs(x="",y="",title="processing_grid") +
        theme_light() +
        theme(legend.position = "none")
    # count
    process_data %>%
      dplyr::distinct(processing_grid, processing_grid_tot_pts)
    
    # save processing attributes
    write.csv(
      process_data
      , paste0(config$delivery_dir, "/processing_las_tiling.csv")
      , row.names = F
    )
  # clean up
    remove(list = ls()[grep("_temp",ls())])
    gc()
    

######################################
# DEFINE ALL lasR pipeline steps
######################################
######################################
# lasR steps
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
  ################
  # library(RMCC) # for the Multiscale Curvature Classification (MCC) (Evans and Hudak 2016) algorithm to classify points
  ################
    lasr_classify_mcc = function(s = 1.5, t = 0.3){
      mcc = function(data, s, t)
      {
        id = RMCC::MCC(data, s, t)
        class = integer(nrow(data))
        class[id] = 2L
        data$Classification = class
        return(data)
      }
      
      classify = lasR::callback(mcc, expose = "xyz", s = s, t = t)
      return(classify)
    }
  ################
  # choose classification algorithm
  ################
    classification_alg = "csf"
    if(tolower(classification_alg)=="mcc"){
      lasr_classify = lasr_classify_mcc(s = 1.5, t = 0.3)
    }else{
      lasr_classify = lasr_classify_csf(
        # csf options
        smooth = FALSE, threshold = 0.5
        , resolution = 0.5, rigidness = 1L
        , iterations = 500L, step = 0.65
      )
    }
  
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
  # dtm
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

  ###################
  # normalize
  ###################
      
      ##### ^^^^^ this (with stage = tri) was taking forever with high density point clouds
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

######################################
# set processing file options using lidR
######################################
  # # with multiple high density clouds...memory is/was issue
  #   # force lasR::exec to operate with specified buffer using lidR LASCatalog settings
  #     # these settings take precedence within lasR
  #   tile_las_ctg = lidR::readLAScatalog(process_flist)
  #   lidR::opt_chunk_buffer(tile_las_ctg) = 10
  #   lidR::opt_chunk_size(tile_las_ctg) = 0
# ######################################
# # when we're having memory issues
# # ...let's talk about lasR::callback
# ######################################
#   # from lasR: https://r-lidar.github.io/lasR/articles/tutorial.html
#   #     "callback() exposes the point cloud as a data.frame.
#   #       This is the only way to expose the point clouds to users in a manageable way.
#   #       One of the reasons why lasR is more memory-efficient and faster than lidR
#   #       is that it does not expose the point cloud as a data.frame.
#   #       Thus, the pipelines using callback() are not significantly different from lidR.
#   #       The advantage of using lasR here is the ability to pipe different stages."
#   ###################
#   # classify points first with lidR? then pass classified las's to lasR
#   # ...worth a shot ;\
#   ###################
#     # define function to classify files using lidR LAScatalog
#     lidr_classify_fn = function(
#       ctg, out_dir, buff = 10, chnk = 0
#       # show progress?
#       , prog = F
#       # parallel?
#       , parallel = use_parallel_processing
#     ){
#       if(parallel){
#         # set parallel
#         future::plan(strategy = multisession, workers = lasR::half_cores())
#       }
#       # loads only the points of interest when querying a chunk
#       lidR::opt_filter(ctg) = "-drop_noise -drop_duplicates"
#       # set buffer and chunk (chunking already done above)
#       lidR::opt_chunk_buffer(ctg) = buff
#       lidR::opt_chunk_size(ctg) = chnk
#       # redirect results to output
#       lidR::opt_output_files(ctg) = paste0(out_dir, "/{*}_classify")
#       # turn off early exit
#       lidR::opt_stop_early(ctg) = FALSE
#       # turn off progress
#       lidR::opt_progress(ctg) = prog
#       # classify
#       lidr_classify_ans = lidR::classify_ground(
#         ctg
#         , algorithm = csf() # this uses the default options...tried to pass custom but failed?
#       )
#       # off parallel
#       if(parallel){
#         # off parallel
#         future::plan(strategy = sequential)
#       }
#       # return
#       return(lidr_classify_ans)
#     }
# 
#     # call the function
#     message("point classification with lidR (parallel = ",use_parallel_processing, ") starting at ", Sys.time())
#     lidr_classify_ans_temp = lidr_classify_fn(ctg = tile_las_ctg, out_dir = config$las_classify_dir, prog = T)
# 
#     # ggplot() +
#     #   geom_sf(data = lidr_classify_ans_temp@data, fill = "red", alpha = 0.5, color = NA) +
#     #   geom_sf(data = tile_las_ctg@data, fill = NA, lwd = 1)
# 
#     # create spatial index
#     flist_temp = create_lax_for_tiles(
#       list.files(config$las_classify_dir, pattern = ".*\\.(laz|las)$", full.names = T)
#     )
#     # with multiple high density clouds...memory is/was issue
#     # force lasR::exec to operate with specified buffer using lidR LASCatalog settings
#       # these settings take precedence within lasR
#     tile_las_ctg = lidR::readLAScatalog(flist_temp)
#     lidR::opt_chunk_buffer(tile_las_ctg) = 10
#     lidR::opt_chunk_size(tile_las_ctg) = 0
# 
#     # clean up
#     remove(list = ls()[grep("_temp",ls())])
#     gc()
# ######################################
# # when we keep having having memory issues
# # ...let's split up the pipeline even though this requires reading data multiple times
# ######################################
#   # set lasR parallel processing options as of lasR 0.4.2
#     lasR::set_parallel_strategy(
#       strategy = lasR::concurrent_points(
#           ncores = max(lasR::ncores()-1, lasR::half_cores())
#         )
#     )
#   ######################################
#   # lasR dtm pipeline
#   ######################################
#     lasr_pipeline_dtm = lasR::reader_las(filter = filter_for_dtm) + 
#       lasr_triangulate + lasr_dtm
#     
#     # message
#     message(
#       "starting lasR DTM processing at"
#       , "..."
#       , Sys.time()
#     )
#   
#     # lasR execute
#       lasr_dtm_ans = lasR::exec(
#         lasr_pipeline_dtm
#         # , on = process_flist # defined above in retile step
#         , on = tile_las_ctg
#         # set lasR exec processing options as of lasR 0.4.2
#         , with = list(
#           progress = T
#         )
#       )
#   
#   ######################################
#   # clean dtm
#   ######################################
#     # fill dtm cells that are missing still with the mean of a window
#     dtm_rast = terra::rast(dtm_file_name)
#     dtm_rast = dtm_rast %>%
#       terra::crop(
#         las_ctg@data$geometry %>% 
#           sf::st_union() %>% 
#           terra::vect() %>% 
#           terra::project(terra::crs(dtm_rast))
#       ) %>% 
#       terra::mask(
#         las_ctg@data$geometry %>% 
#           sf::st_union() %>% 
#           terra::vect() %>% 
#           terra::project(terra::crs(dtm_rast))
#       ) %>% 
#       terra::focal(
#         w = 3
#         , fun = "mean"
#         , na.rm = T
#         # na.policy Must be one of:
#           # "all" (compute for all cells)
#           # , "only" (only for cells that are NA)
#           # , or "omit" (skip cells that are NA).
#         , na.policy = "only"
#       )
#     # dtm_rast %>% terra::crs()
#     # dtm_rast %>% terra::plot()
#     
#     # write to delivery directory
#       terra::writeRaster(
#         dtm_rast
#         , filename = dtm_file_name
#         , overwrite = T
#       )
#   ######################################
#   # lasR chm pipeline
#   ######################################
#     if(as.numeric(accuracy_level) %in% c(2,3)){
#         # normalize
#         lasr_normalize = lasR::transform_with(stage = lasr_triangulate, operator = "-")
#         # pipeline
#         lasr_pipeline_chm = lasr_read + 
#           lasr_triangulate +  lasr_normalize +
#           lasr_write_normalize +
#           lasr_chm + lasr_chm_pitfill
#       }else{
#         # load dtm
#         lasr_ld_dtm = lasR::load_raster(dtm_file_name)
#         # normalize
#         lasr_normalize = lasR::transform_with(stage = lasr_ld_dtm, operator = "-")
#         # pipeline
#         lasr_pipeline_chm = lasr_read + 
#           lasr_ld_dtm + lasr_normalize +
#           lasr_write_normalize +
#           lasr_chm + lasr_chm_pitfill
#       }
#     
#     # message
#     message(
#       "starting lasR CHM processing at"
#       , "..."
#       , Sys.time()
#     )
#   
#     # lasR execute
#       lasr_chm_ans = lasR::exec(
#         lasr_pipeline_chm
#         # , on = process_flist # defined above in retile step
#         , on = tile_las_ctg
#         # set lasR exec processing options as of lasR 0.4.2
#         , with = list(
#           progress = T
#         )
#       )
    
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
      "starting lasR processing of processing grid (`process_data`) "
      , processing_grid_num
      , " at ..."
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
  # map over processing grids
    lasr_ans_list = 
      process_data$processing_grid %>%
      unique() %>%
      # c(8) %>% 
      purrr::map(lasr_pipeline_fn) 
    # %>% 
    #   c() %>% 
    #   unlist()
      
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
    # fill empty cells
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
  
  
    
  # lasr_ans %>% str()
  # lasr_ans$pit_fill %>% terra::plot()
  # lasr_ans$pit_fill %>% terra::crs()
  # lasr_ans[[length(lasr_ans)]] %>% terra::plot()
  
  # # fill dtm cells that are missing still with the mean of a window
  #   dtm_rast = terra::rast(dtm_file_name)
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
  #   
  # # write to delivery directory
  #   terra::writeRaster(
  #     dtm_rast
  #     , filename = dtm_file_name
  #     , overwrite = T
  #   )
  
  # # fill chm cells that are missing still with the mean of a window
  #   chm_rast = terra::rast(chm_file_name)
  #   chm_rast = chm_rast %>%
  #     terra::crop(
  #       las_ctg@data$geometry %>% 
  #         sf::st_union() %>% 
  #         terra::vect() %>% 
  #         terra::project(terra::crs(chm_rast))
  #     ) %>% 
  #     terra::mask(
  #       las_ctg@data$geometry %>% 
  #         sf::st_union() %>% 
  #         terra::vect() %>% 
  #         terra::project(terra::crs(chm_rast))
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
  #   # chm_rast %>% terra::crs()
  #   # chm_rast %>% terra::plot()
  #   
  # # write to delivery directory
  #   terra::writeRaster(
  #     chm_rast
  #     , filename = chm_file_name
  #     , overwrite = T
  #   )
  
  # create spatial index files (.lax)
    # denoise
    create_lax_for_tiles(
      las_file_list = list.files(config$las_denoise_dir, pattern = ".*\\.(laz|las)$", full.names = T)
    )
    # classify
    create_lax_for_tiles(
      las_file_list = list.files(config$las_classify_dir, pattern = ".*\\.(laz|las)$", full.names = T)
    )
    # normalize
    create_lax_for_tiles(
      las_file_list = list.files(config$las_normalize_dir, pattern = ".*\\.(laz|las)$", full.names = T)
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
          # , difftime(xx7_treels_start_time, xx1_tile_start_time, units = c("mins")) %>% as.numeric()
          , "mins"
        )
  
dtm_rast %>% 
  # terra::aggregate(fact = 4) %>% 
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

# ###########################################################################################################################
# ###########################################################################################################################
# ###########################################################################################################################
# ###########################################################################################################################
# ###########################################################################################################################
# ###########################################################################################################################
# # testing differences in filtering for triangulation with high density point clds
# # ran kaibab_low, high_aggressive.las with different filtering for the number of points used for triangulation
#   # 400 pts/m2
#   # filter1 = 100% of points used (400 pts/m2)
#   # filter0.75 = 75% of points used (300 pts/m2)
#   # ....
#   # filter0.05 = 5% of points used (20 pts/m2)
#   
#   ### this is sloppy code but will suffice
# library(tidyverse)
# library(terra)
# library(brms)
# library(tidybayes)
#   
# r1 = terra::rast("c:/data/usfs/point_cloud_tree_detection_ex/data/03_delivery/filter1_chm_0.25m.tif")
# r2 = terra::rast("c:/data/usfs/point_cloud_tree_detection_ex/data/03_delivery/filter0.75_chm_0.25m.tif")
# r3 = terra::rast("c:/data/usfs/point_cloud_tree_detection_ex/data/03_delivery/filter0.5_chm_0.25m.tif")
# r4 = terra::rast("c:/data/usfs/point_cloud_tree_detection_ex/data/03_delivery/filter0.25_chm_0.25m.tif")
# r5 = terra::rast("c:/data/usfs/point_cloud_tree_detection_ex/data/03_delivery/filter0.19_chm_0.25m.tif")
# r6 = terra::rast("c:/data/usfs/point_cloud_tree_detection_ex/data/03_delivery/filter0.13_chm_0.25m.tif")
# r7 = terra::rast("c:/data/usfs/point_cloud_tree_detection_ex/data/03_delivery/filter0.05_chm_0.25m.tif")
# 
# plot((r2-r1)/r1)
# 
# l1 = as.matrix((r2-r1)/r1) %>% c()
# l1 = l1[!is.na(l1)]
# l2 = as.matrix((r3-r1)/r1) %>% c()
# l2 = l2[!is.na(l2)]
# l3 = as.matrix((r4-r1)/r1) %>% c()
# l3 = l3[!is.na(l3)]
# l4 = as.matrix((r5-r1)/r1) %>% c()
# l4 = l4[!is.na(l4)]
# l5 = as.matrix((r6-r1)/r1) %>% c()
# l5 = l5[!is.na(l5)]
# l6 = as.matrix((r7-r1)/r1) %>% c()
# l6 = l6[!is.na(l6)]
# 
# dta = dplyr::bind_rows(
#     dplyr::tibble(
#       f = rep("300 pts.m2", length(l1))  
#       , pct_diff = l1
#     )
#     , dplyr::tibble(
#       f = rep("200 pts.m2", length(l2))  
#       , pct_diff = l2
#     )
#     , dplyr::tibble(
#       f = rep("100 pts.m2", length(l3))  
#       , pct_diff = l3
#     )
#     , dplyr::tibble(
#       f = rep("75 pts.m2", length(l4))  
#       , pct_diff = l4
#     )
#     , dplyr::tibble(
#       f = rep("50 pts.m2", length(l5))  
#       , pct_diff = l5
#     )
#     , dplyr::tibble(
#       f = rep("20 pts.m2", length(l6))  
#       , pct_diff = l6
#     )
#   ) %>% 
#   dplyr::mutate(
#     f = factor(
#       f
#       , ordered = T
#       , levels = c(
#         "300 pts.m2"
#         , "200 pts.m2"
#         , "100 pts.m2"
#         , "75 pts.m2"
#         , "50 pts.m2"
#         , "20 pts.m2"
#       )
#     )
#   )
# 
# dta %>% dplyr::count(f)
# dta %>% dplyr::slice_sample(prop = 0.5) %>% ggplot(aes(x = f, y = pct_diff)) + geom_boxplot()
# 
# # center
# mean_y = mean(dta$pct_diff)
# sd_y = sd(dta$pct_diff)
# dta$pct_diff_c = (dta$pct_diff-mean_y)/sd_y
# summary(dta$pct_diff_c)
# 
# # stanvars
# gamma_a_b_from_omega_sigma = function(mode, sd) {
#   if (mode <= 0) stop("mode must be > 0")
#   if (sd   <= 0) stop("sd must be > 0")
#   rate = (mode + sqrt(mode^2 + 4 * sd^2)) / (2 * sd^2)
#   shape = 1 + mode * rate
#   return(list(shape = shape, rate = rate))
# }
# 
# mean_y_c = mean(dta$pct_diff_c)
# sd_y_c = sd(dta$pct_diff_c)
# omega = sd_y_c / 2
# sigma = 2 * sd_y_c
# (s_r = gamma_a_b_from_omega_sigma(mode = omega, sd = sigma))
# 
# stanvars = 
#   brms::stanvar(mean_y_c,    name = "mean_y_c") + 
#   brms::stanvar(sd_y_c,      name = "sd_y_c") +
#   brms::stanvar(s_r$shape, name = "alpha") +
#   brms::stanvar(s_r$rate,  name = "beta")
# 
# m1 = brms::brm(
#   formula = pct_diff_c ~ 1 + (1|f)
#   , data = dta
#   , iter = 2000, warmup = 1000, chains = 4, cores = max(lasR::ncores()-2, lasR::half_cores())
#   , prior = c(
#     brms::prior(normal(mean_y_c, sd_y_c * 5), class = Intercept)
#     , brms::prior(gamma(alpha, beta), class = sd)
#     , brms::prior(cauchy(0, sd_y_c), class = sigma)
#   )
#   , stanvars = stanvars
#   , file = "c:/data/usfs/point_cloud_tree_detection_ex/data/03_delivery/brms_m1"
# )
# # chains
# plot(m1)
# # plot dist
# dta %>%
#   dplyr::distinct(f) %>% 
#   tidybayes::add_epred_draws(m1) %>%
#   # transform b/c used centered/standardized y var
#   dplyr::mutate(
#     y_hat = (.epred*sd_y) + mean_y
#     , f = f %>% forcats::fct_rev()
#   ) %>% 
#   dplyr::ungroup() %>% 
#   ggplot(aes(x = y_hat, y = f)) +
#     geom_vline(xintercept = 0, linetype = "dashed") +
#     tidybayes::stat_dotsinterval(point_interval = "median_hdi", quantiles = 100) + 
#     scale_x_continuous(limits = c(-0.001,0.001), labels = scales::percent_format(accuracy = 0.01)) +
#     labs(y = "", x = "% difference from 400 pts.m2") +
#     theme_light()
#   
# # hdi
# dta %>%
#   dplyr::distinct(f) %>% 
#   tidybayes::add_epred_draws(m1) %>%
#   # transform b/c used centered/standardized y var
#   dplyr::mutate(
#     y_hat = (.epred*sd_y) + mean_y
#     , lt_pct = ifelse(abs(y_hat)<0.05,1,0)
#   ) %>% 
#   tidybayes::median_hdi(y_hat, .width = c(0.5,0.9,0.95,0.99)) %>% 
#   dplyr::mutate(
#     dplyr::across(.cols = c(y_hat,.upper,.lower), .fns = ~ scales::percent(.x,accuracy = 0.001))
#   ) %>% 
#   dplyr::arrange(f,.width) %>% 
#   kableExtra::kbl() %>% 
#   kableExtra::kable_styling()
# 
# # comparisons
# dta %>%
#   dplyr::distinct(f) %>% 
#   tidybayes::add_epred_draws(m1) %>%
#   dplyr::ungroup() %>% 
#   # transform b/c used centered/standardized y var
#   dplyr::mutate(
#     y_hat = (.epred*sd_y) + mean_y
#   ) %>% 
#   tidybayes::compare_levels(variable = y_hat, by = f, comparison = "pairwise") %>% 
#   dplyr::ungroup() %>% 
#   dplyr::mutate(
#     f = f %>% 
#       factor() %>% 
#       forcats::fct_reorder(stringr::word(f) %>% as.numeric())
#   ) %>% 
#   ggplot(
#       aes(
#         x = y_hat, y = f
#         , fill = after_stat(abs(x) < .0005)
#       )
#     ) +
#     geom_vline(xintercept = 0, linetype = "dashed") +
#     tidybayes::stat_halfeye(point_interval = "median_hdi") + 
#     # tidybayes::stat_dotsinterval(quantiles = 100) + 
#     labs(
#       fill = paste0("diff. <", scales::percent(.0005, accuracy = 0.01))
#       , y = "", x = "difference"
#     ) +
#     scale_x_continuous(labels = scales::percent_format(accuracy = 0.01)) +
#     scale_fill_manual(values = c("gray", "steelblue")) +
#     theme_light()
# 
# 
# # # m2 with continuous pts???
# # m2 = brms::brm(
# #   formula = pct_diff_c ~ 1 + (1|f)
# #   , data = dta
# #   , iter = 2000, warmup = 1000, chains = 4
# #   , prior = c(
# #     brms::prior(normal(mean_y_c, sd_y_c * 5), class = Intercept)
# #     , brms::prior(gamma(alpha, beta), class = sd)
# #     , brms::prior(cauchy(0, sd_y_c), class = sigma)
# #   )
# #   , stanvars = stanvars
# #   , file = "c:/data/usfs/point_cloud_tree_detection_ex/data/03_delivery/brmsm1"
# # )

##########################################################################################
##########################################################################################
##########################################################################################
##########################################################################################
# # group rows based on cumsum with reset
# cumsum_group <- function(x, threshold) {
#   cumsum <- 0
#   group <- 1
#   result <- numeric()
#   
#   for (i in 1:length(x)) {
#     cumsum <- cumsum + x[i]
#     
#     if (cumsum > threshold) {
#       group <- group + 1
#       cumsum <- x[i]
#     }
#     
#     result = c(result, group)
#     
#   }
#   
#   return (result)
# }
# 
# # cumsum with reset
# cumsum_with_reset <- function(x, threshold) {
#   cumsum <- 0
#   group <- 1
#   result <- numeric()
#   
#   for (i in 1:length(x)) {
#     cumsum <- cumsum + x[i]
#     
#     if (cumsum > threshold) {
#       group <- group + 1
#       cumsum <- x[i]
#     }
#     
#     result = c(result, cumsum)
#     
#   }
#   
#   return (result)
# }