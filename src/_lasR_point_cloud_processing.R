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
# do something
#################################################################################
#################################################################################

opt_output_files(ctg) <-  paste0(tempdir(), "/{*}_norm")
ctg_norm <- normalize_height(ctg, dtm)
    