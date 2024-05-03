#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This is the translated python functions from R code using rpy2
"""
import pathlib

# Version 05.01.2024
# Author: Jiawei Li
import rpy2.robjects as ro
import functools
from pathlib import Path
import rpy2.robjects.methods
from rpy2.robjects.vectors import IntVector, StrVector, FloatVector,BoolVector, ListVector, NA_Logical


def reproject_las_fn(config:rpy2.robjects.vectors.DataFrame,
                     las_catagory: rpy2.robjects.methods.RS4
                     ) -> rpy2.robjects.methods.RS4:
    ro.globalenv['las_ctg'] = las_catagory
    ro.globalenv['user_supplied_epsg'] = ro.r('config$user_supplied_epsg')
    ro.globalenv['user_supplied_old_epsg'] = ro.r('user_supplied_old_epsg')
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
    }
    config$proj_crs = proj_crs
    ### is this ctg huge or what?
    ctg_pts_so_many = sum(las_ctg@data$Number.of.point.records) > max_ctg_pts
    config$ctg_pts_so_many = ctg_pts_so_many
    
    """)
    return ro.r['las_ctg']


def write_las_coverage(las_catagory: rpy2.robjects.methods.RS4,
                       config:rpy2.robjects.vectors.DataFrame,) -> None:

    ro.globalenv['las_ctg'] = las_catagory
    ro.globalenv['config'] = config
    ro.globalenv['raw_las_ctg_info'] = ro.r('las_ctg@data')
    ro.r('sf::st_write(las_ctg@data, paste0(config$delivery_dir, "/raw_las_ctg_info.gpkg"),quiet = TRUE, append = FALSE')
    return
def create_lax_for_tiles(las_file_list: (str, pathlib.PurePath, list)) -> (StrVector, ListVector, NA_Logical):
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
      """)
    if isinstance(las_file_list, (str, pathlib.PurePath)):
        def_file = ro.r(str(las_file_list))
        return def_file
    elif isinstance(las_file_list, list):
        def_file = ro.r(las_file_list)
        return def_file
def bulk_reproject_las(las_ctg, input_las_dir, outdif, new_crs=None, old_crs=None):
    r_source_code = ro.r("""
    function(las_ctg, input_las_dir, user_supplied_epsg=NA, user_supplied_old_epsg=NA){
    message("reprojecting raw las data as requested. may lead to result inaccuracies!")
      # map over files
        flist_temp = las_ctg@data$filename %>% 
          purrr::map(
            reproject_las_fn
            , new_crs = user_supplied_epsg
            , old_crs = user_supplied_old_epsg
            , outdir = input_las_dir
          ) %>% 
          c() %>% 
          unlist()
      # create spatial index files (.lax)
        flist_temp = create_lax_for_tiles(flist_temp)
      ### point to input las files as a lidR LAScatalog
        las_ctg = lidR::readLAScatalog(flist_temp)
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
    }
    """)
