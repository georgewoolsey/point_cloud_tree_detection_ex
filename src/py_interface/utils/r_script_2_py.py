#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This is the translated python functions from R code using rpy2
"""
import pathlib

# Version 05.01.2024
# Author: Jiawei Li

import rpy2.robjects as ro
import rpy2.robjects.methods
from py_interface.utils import tools



def reproject_las_fn(config:rpy2.robjects.vectors.ListVector,
                     las_catagory: rpy2.robjects.methods.RS4
                     ) -> tuple:
    ro.globalenv['las_ctg'] = las_catagory
    ro.globalenv['user_supplied_epsg'] = ro.r('config$user_supplied_epsg')
    ro.globalenv['user_supplied_old_epsg'] = ro.r('config$user_supplied_old_epsg')
    ro.globalenv['transform_to_this_epsg'] = ro.r('config$transform_to_this_epsg')
    ro.globalenv['config'] = config
    ro.r("""
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
          fnm = filepath %>% basename() %>% stringr::str_remove_all("/.(laz|las)$")
          new_fnm = paste0(normalizePath(outdir),"/",fnm,"_epsg",new_crs,".las")
          # reproject
          las = sf::st_transform(las, paste0("EPSG:", new_crs))
          # write
          lidR::writeLAS(las, file = new_fnm)
          return(new_fnm)
        }
      }
      if(
      transform_to_this_epsg == T & 
      !is.na(user_supplied_epsg) & 
      !is.null(user_supplied_epsg) & 
      length(as.character(user_supplied_epsg))>0
    ){
      message("reprojecting raw las data as requested. may lead to result inaccuracies!")
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
    """)
    return ro.r('config'), ro.r['las_ctg']

def create_lax_for_tiles(config) -> tuple():
    ro.globalenv['config'] = config
    file_list = tools.list_files_r(ro.r('config$input_las_dir')[0], ['*.laz', '*.las'])
    ro.globalenv['las_file_list'] = file_list
    ro.r("""
     create_lax_for_tiles <- function(las_file_list){
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
    flist_temp = create_lax_for_tiles(las_file_list)
    config$flist_temp = flist_temp
      """)
    return ro.r('config')

def readlascatalog(config: rpy2.robjects.vectors.DataFrame) -> tuple:
    ro.globalenv['config'] = config
    ro.r('las_ctg = lidR::readLAScatalog(config$flist_temp)')
    return ro.r('config'), ro.r['las_ctg']
def handle_missing_crs(config: rpy2.robjects.vectors.DataFrame,las_catagory: rpy2.robjects.methods.RS4,
                       ) -> tuple:
    ro.globalenv['las_ctg'] = las_catagory
    ro.globalenv['user_supplied_epsg'] = ro.r('config$user_supplied_epsg')
    ro.globalenv['user_supplied_old_epsg'] = ro.r('config$user_supplied_old_epsg')
    ro.globalenv['config'] = config
    ro.r("""
    crs_list_temp = sf::st_crs(las_ctg)$epsg
    
    # handle missing epsg with user defined parameter
    if(is.na(crs_list_temp) & !is.na(user_supplied_epsg) & !is.null(user_supplied_epsg)){
      crs_list_temp = user_supplied_epsg
      sf::st_crs(las_ctg) = paste0("EPSG:", user_supplied_epsg)
    }
    config$crs_list_temp = crs_list_temp
    if(length(unique(crs_list_temp))>1){
      stop("the raw las files have multiple CRS settings")
    }else{
      proj_crs = paste0("EPSG:",unique(crs_list_temp))
      config$proj_crs = proj_crs
    }
    """)
    return ro.r('config'), ro.r['las_ctg']

def check_ctg_size(config: rpy2.robjects.vectors.DataFrame, las_catagory: rpy2.robjects.methods.RS4,
                   ) -> rpy2.robjects.vectors.DataFrame:
    ro.globalenv['las_ctg'] = las_catagory
    ro.globalenv['config'] = config
    ro.globalenv['max_ctg_pts'] = ro.r('config$max_ctg_pts')
    ro.r("""
    ### is this ctg huge or what?
    ctg_pts_so_many = sum(las_ctg@data$Number.of.point.records) > max_ctg_pts
    config$ctg_pts_so_many = ctg_pts_so_many
    """)
    return ro.r['config']

def write_las_coverage(config:rpy2.robjects.vectors.DataFrame,
                       las_catagory: rpy2.robjects.methods.RS4
                       ) -> None:

    ro.globalenv['las_ctg'] = las_catagory
    ro.globalenv['config'] = config
    ro.r('config$raw_las_ctg_info = paste0(config$delivery_dir, "/raw_las_ctg_info.gpkg")')
    ro.r('sf::st_write(las_ctg@data, config$raw_las_ctg_info, quiet = TRUE, append = FALSE)')
    return

def create_chunk_ctg(config: rpy2.robjects.vectors.DataFrame,
                     las_catagory: rpy2.robjects.methods.RS4
                   ) -> tuple:
    ro.globalenv['las_ctg'] = las_catagory
    ro.globalenv['config'] = config
    ro.r('max_ctg_pts = config$max_ctg_pts')
    ro.r('max_area_m2 = config$max_area_m2')
    ro.r("""
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
    """)
    return ro.r('config'),ro.r['ctg_chunk_data']

def determine_chunking(config:rpy2.robjects.vectors.DataFrame = None,
                       ctg_chunk_data: rpy2.robjects.vectors.DataFrame = None) -> tuple:
    if config is not None:
        ro.globalenv['config'] = config
    if ctg_chunk_data is not None:
        ro.globalenv['ctg_chunk_data'] = ctg_chunk_data
    file_list = tools.list_files_r(ro.r('config$input_las_dir')[0], ['*.laz', '*.las'])
    ro.globalenv['las_file_list'] = file_list
    ro.r("""if(
      ctg_chunk_data$chunk_max_ctg_pts[1] > 0
      & config$ctg_pts_so_many == T
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
      flist_temp = create_lax_for_tiles(las_file_list)
      # switch for processing grid subsets
      grid_subset_switch = T
      config$grid_subset_switch = grid_subset_switch
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
      flist_temp = create_lax_for_tiles(las_file_list = list.files(las_file_list))
      # switch for processing grid subsets
      grid_subset_switch = F
      config$grid_subset_switch = grid_subset_switch
    }else{grid_subset_switch = F
    config$grid_subset_switch = grid_subset_switch}
    process_data =
      lidR::readLAScatalog(flist_temp)@data %>%
      dplyr::ungroup() %>% 
      dplyr::mutate(
        processing_grid = dplyr::case_when(
          config$grid_subset_switch == T ~ dplyr::row_number()
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
      """)

    ro.r('config$process_data_las_tiling = paste0(config$delivery_dir, "/process_data_las_tiling.gpkg")')
    ro.r("""sf::st_write(
        process_data
        , config$process_data_las_tiling
    , quiet = TRUE, append = FALSE)
    """)
    return ro.r('config'), ro.r('process_data')

def lasr_classify_csf(config: rpy2.robjects.vectors.DataFrame = None) -> rpy2.robjects.vectors.DataFrame:
    if config is not None:
        ro.globalenv['config'] = config
    ro.r("""
    lasr_read = lasR::reader_las(filter = "-drop_noise -drop_duplicates")
    config$lasr_read = lasr_read
    lasr_classify_csf <- function(
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
    lasr_classify = lasr_classify_csf(
        # csf options
        smooth = FALSE, threshold = 0.5
        , resolution = 0.5, rigidness = 1L
        , iterations = 500L, step = 0.65
      )
    config$lasr_classify = lasr_classify
      """)
    return ro.r('config')
def denoise(config=None):
    if config is not None:
        ro.globalenv['config'] = config
    ro.r("""
    lasr_denoise = lasR::classify_isolated_points(res=5, n=6)
    lasr_write_classify = lasR::write_las(ofile = paste0(config$las_classify_dir, "/*_classify.las")
        # ofile = paste0(config$las_classify_dir, "/*_classify.las")
        , filter = "-drop_noise"
        , keep_buffer = F
      )
    """)
    return ro.r('config')
def dtm_with_normalize_pipe(config = None):
    if config is not None:
        ro.globalenv['config'] = config
    ro.r("""
    lasr_dtm_norm_fn = function(
      dtm_file_name = paste0(config$delivery_dir, "/dtm_", config$desired_dtm_res, "m.tif")
      , frac_for_tri = 1
      , dtm_res = config$desired_dtm_res
      , norm_accuracy = config$accuracy_level
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
        pipeline = lasr_triangulate + lasr_dtm + lasr_normalize
      return(pipeline)
    }
    lasr_write_normalize = lasR::write_las(
      filter = "-drop_z_below 0 -drop_class 18"
      , ofile = paste0(config$las_normalize_dir, "/*_normalize.las")
      , keep_buffer = F
    )
    config$lasr_write_normalize = lasr_write_normalize""")
    return config
def chm_pipe(config = None):
    if config is not None:
        ro.globalenv['config'] = config
    ro.r("""
    lasr_chm_fn = function(
     chm_file_name = paste0(config$delivery_dir, "/chm_", config$desired_chm_res, "m.tif") 
     , chm_res = config$desired_chm_res
     , min_height_m = config$minimum_tree_height_m
     , max_height_m = config$max_height_threshold_m
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
    }""")
    return config

def lasR_pipe(config=None):
    if config is not None:
        ro.globalenv['config'] = config
    ro.r("""
    lasr_pipeline_fn = function(
    processing_grid_num = 1
    , keep_intrmdt = config$keep_intermediate_files
    # lasr_dtm_norm_fn
    , dtm_res_m = config$desired_dtm_res
    , normalization_accuracy = config$accuracy_level
    # lasr_chm_fn
     , chm_res_m = config$desired_chm_res
     , min_height = config$minimum_tree_height_m
     , max_height = config$max_height_threshold_m
  ){
    # output files
    dtm_file_name = paste0(config$dtm_dir, "/", processing_grid_num,"_dtm_", dtm_res_m, "m.tif")
    config$dtm_file_name = dtm_file_name
    chm_file_name = paste0(config$chm_dir, "/", processing_grid_num,"_chm_", chm_res_m, "m.tif")
    config$chm_file_name = chm_file_name
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
    """)
    return ro.r('config')
def lasR_pipe_execute(config=None):
    if config is not None:
        ro.globalenv['config'] = config
    ro.r("""
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
      
    if(nrow(has_errors_temp)>0){stop("lasR processing failed due to:\n", has_errors_temp$message[1])}""")
    return ro.r('config')

def write_dtm(config=None):
    if config is not None:
        ro.globalenv['config'] = config
    dtm_file_list = tools.list_files_r(ro.r('config$dtm_dir')[0], ['*.tif', '*.tiff'])
    ro.globalenv['dtm_file_list'] = dtm_file_list
    ro.r("""
    # output name
      dtm_file_name = paste0(config$delivery_dir, "/dtm_", config$desired_dtm_res, "m.tif")
      # read
      rast_list_temp = dtm_file_list %>% purrr::map(function(x){terra::rast(x)})
      dtm_rast = terra::sprc(rast_list_temp) %>% terra::mosaic(fun = "mean")
      terra::crs(dtm_rast) = config$proj_crs
      terra::writeRaster(
          dtm_rast
          , filename = dtm_file_name
          , overwrite = T
        )
    """)
    return ro.r('config')
def write_chm(config=None):
    if config is not None:
        ro.globalenv['config'] = config
    chm_file_list = tools.list_files_r(ro.r('config$chm_dir')[0], ['*.tif', '*.tiff'])
    ro.globalenv['chm_file_list'] = chm_file_list
    las_classify_dir_file_list = tools.list_files_r(ro.r('config$las_classify_dir')[0], ['*.laz', '*.las'])
    ro.globalenv['las_classify_dir_file_list'] = las_classify_dir_file_list
    las_normalize_dir_file_list = tools.list_files_r(ro.r('config$las_normalize_dir')[0], ['*.laz', '*.las'])
    ro.globalenv['las_normalize_dir_file_list'] = las_normalize_dir_file_list

    ro.r("""
    # output name
      chm_file_name = paste0(config$delivery_dir, "/chm_", config$desired_chm_res, "m.tif")
      # read
      rast_list_temp = chm_file_list %>% purrr::map(function(x){terra::rast(x)})
      chm_rast = terra::sprc(rast_list_temp) %>% terra::mosaic(fun = "max")
      terra::crs(chm_rast) = config$proj_crs
      chm_rast = chm_rast %>%
          terra::crop(
            las_ctg@data$geometry %>%
              sf::st_union() %>%
              terra::vect()
          ) %>%
          terra::mask(
            las_ctg@data$geometry %>%
              sf::st_union() %>%
              terra::vect()
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
      config$chm_rast = chm_rast
      # write to delivery directory
        terra::writeRaster(
          chm_rast
          , filename = chm_file_name
          , overwrite = T
        )
     classify_flist = create_lax_for_tiles(
        las_classify_dir_file_list
      )
      config$classify_flist = classify_flist
      # normalize
      normalize_flist = create_lax_for_tiles(las_normalize_dir_file_list)
    config$normalize_flist = normalize_flist
    """)
    return ro.r('config')

def detect_tree_stems(config=None):
    if config is not None:
        ro.globalenv['config'] = config
    ro.r("""
    if(config$grid_subset_switch==F){ # the las's are not chunked with buffer
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
        }""")
    if ro.r('config$grid_subset_switch==F')[0] is True:
        las_stem_norm_dir_file_list = tools.list_files_r(ro.r('config$las_stem_norm_dir')[0], ['*.laz', '*.las'])
        ro.globalenv['las_stem_norm_dir_file_list'] = las_stem_norm_dir_file_list
        ro.r("""# create spatial index
            # note this overwrites the normalize_flist created above if new files created
            
            normalize_flist = create_lax_for_tiles(las_stem_norm_dir_file_list)
            """)
    return ro.r('config')

def tree_map_func(config=None):
    if config is not None:
        ro.globalenv['config'] = config
    ro.r("""
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
    """)
    return ro.r('config')

def write_stem_las_fn (config=None):
    if config is not None:
        ro.globalenv['config'] = config
    ro.r("""
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
    """)
    return ro.r('config')

def call_stem_las_fn(config=None):
    if config is not None:
        ro.globalenv['config'] = config
    ro.r("""
      # map over the normalized point cloud tiles
      write_stem_las_ans = 
        normalize_flist %>%
          purrr::map(write_stem_las_fn, min_tree_height = config$minimum_tree_height_m)
    """)
    las_stem_dir_list = tools.list_files_r(ro.r('config$las_stem_dir')[0], ['*.laz', '*.las'])
    ro.globalenv['las_stem_dir_list'] = las_stem_dir_list
    ro.r("""las_stem_flist = create_lax_for_tiles(las_stem_dir_list)
    config$las_stem_flist = las_stem_flist""")
    return ro.r('config')

def combine_stem_vector(config):
    parquetfile = tools.list_files_r(ro.r('config$stem_poly_tile_dir')[0], ['*.parquet'])
    ro.globalenv['parquetfile'] = parquetfile
    ro.r("""
    if(
        length(parquetfile) > 0
      ){
        dbh_locations_sf = parquetfile %>% 
            purrr::map(sfarrow::st_read_parquet) %>% 
            dplyr::bind_rows() %>% 
            sf::st_as_sf() %>% 
            sf::st_make_valid() %>% 
            sf::st_set_crs(proj_crs)
    dbh_locations_sf = dbh_locations_sf %>% 
          dplyr::filter(
            !is.na(radius_m)
            & dbh_m <= config$dbh_max_size_m
            & sf::st_is_valid(.)
            & !sf::st_is_empty(.)
          ) %>% 
          dplyr::mutate(
            condition = "detected_stem"
          )
    sf:::st_write(
            dbh_locations_sf
            , dsn = paste0(config$delivery_dir, "/bottom_up_detected_stem_locations.gpkg")
            , append = FALSE
            , delete_dsn = TRUE
            , quiet = TRUE
          )
      }else{dbh_locations_sf = NA}
      config$dbh_locations_sf = dbh_locations_sf""")
    return config

def chm_individual_tree_detec(config):
    ro.globalenv['config'] = config
    ro.r("""
    # define the variable window function
        ws_fn <- function(x) {
      y = dplyr::case_when(
        is.na(x) ~ 1e-3 # requires non-null
        , x < 0 ~ 1e-3 # requires positive
        , x < 2 ~ 1 # set lower bound
        , x > 30 ~ 5  # set upper bound
        , TRUE ~ 0.75 + (x * 0.14)
      )
      return(y)}
      # check if need to split raster into tiles
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
      config$split_raster <- split_raster
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
    trees_crowns_fn = function(rast_pth, ws = ws_fn, hmin = config$minimum_tree_height_m, dir = config$temp_dir){
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
    trees_crowns_data = chm_tiles %>% 
      purrr::map(trees_crowns_fn) %>% 
      dplyr::bind_rows()
    config$trees_crowns_data = trees_crowns_data
    
    write.csv(trees_crowns_data, paste0(config$temp_dir, "/trees_crowns_data.csv"), row.names = F)
    
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
    
  }else{ #if(split_raster==T)
    # identify tree tops
    tree_tops = lidR::locate_trees(
      chm_rast
      , algorithm = lmf(
        ws = ws_fn
        , hmin = config$minimum_tree_height_m
      )
    )
    # delineate crowns
    crowns = ForestTools::mcws(
      treetops = sf::st_zm(tree_tops, drop = T) # drops z values
      , CHM = chm_rast
      , minHeight = config$minimum_tree_height_m
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
    """)
    return config

def model_missing_dbh(config):
    ro.globalenv['config'] = config
    ro.r("""
    ### Join the top down crowns with the stem location points
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
    }""")
    ro.r("""
    #Filter the SfM DBHs
    if(tolower(proj_crs)=="epsg:na"){
      stop(paste0(
        "Cannot make regional DBH-Height model with blank CRS. Set `user_supplied_epsg` parameter if known."
        , "See outputs in:"
        , config$delivery_dir
      ))
    }
    """)
    ro.r("""
    #read in treemap data
    treemap_rast = terra::rast(paste0(config$input_treemap_dir, "/TreeMap2016.tif"))
    
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
    """)
    ro.r("""
    #get weights for weighting each tree in the population models
    tm_id_weight_temp = terra::freq(treemap_rast) %>%
      dplyr::select(-layer) %>% 
      dplyr::rename(tm_id = value, tree_weight = count) %>% 
      dplyr::mutate(tm_id = as.character(tm_id))""")
    ro.r("""
    ### get the TreeMap FIA tree list for only the plots included
    treemap_trees_df = readr::read_csv(
        paste0(config$input_treemap_dir, "/TreeMap2016_tree_table.csv")
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
        gc()""")
    ro.r("""
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
        if(tolower(config$local_dbh_model) == "rf"){
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
      gc()""")
    return config
def calculate_silviculture_metrics(config):
    ro.globalenv['config'] = config
    ro.r("""
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
    """)
    return config