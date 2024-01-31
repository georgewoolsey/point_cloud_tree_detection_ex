##################################################################################
##################################################################################
# combining multiple runs of `point_cloud_processing.R` script with areas that overlap
# or are adjacent. This file should just be able to use the files:
#  1) "chm_rast_{xx}m.tif"
#  2) and dbh_locations_sf "/bottom_up_detected_stem_locations.gpkg"
#  3) and raw_las_ctg_info.gpkg
##################################################################################
##################################################################################
remove(list = ls()[])
gc()

#################################################################################
#################################################################################
# User-Defined Parameters
#################################################################################
#################################################################################
# setup
setwd("c:/Data/usfs/point_cloud_tree_detection_ex/data/")

# deliver files to a directory named
deliver_files_dir = "point_cloud_processing_BHEF_202306_combined"

# directory name string match to search for
# re-name the point_cloud_processing_delivery directory from `point_cloud_processing.R`
dir_matches = "point_cloud_processing_BHEF_202306_Units"

###_________________________###
### Set input TreeMap directory ###
###_________________________###
input_treemap_dir = "../data/treemap"

###_________________________###
### Set the minimum height (m) for individual tree detection in `lidR::locate_trees`
###_________________________###
minimum_tree_height = 2

#################################################################################
#################################################################################
# end User-Defined Parameters
#################################################################################
#################################################################################
# create dir
delivery_dir = paste0(getwd(),"/",deliver_files_dir)
dir.create(delivery_dir, showWarnings = FALSE)
# library
library(tidyverse) # the tidyverse
library(viridis) # viridis colors
library(scales) # work with number and plot scales
# spatial analysis
library(terra) # raster
library(sf) # simple features
# point cloud processing
library(lidR)
library(ForestTools) # for crown delineation
# modeling
library(randomForest)
library(RCSF) # for the cloth simulation filter (csf) to classify points
library(brms) # bayesian modelling using STAN engine

##################################################################################
##################################################################################
# combine chm rasters by taking the maximum chm height
##################################################################################
##################################################################################
  # read list of chms
  chm_list_temp = list.dirs(getwd(), recursive = F) %>% 
    stringr::str_subset(dir_matches) %>% 
    purrr::map(function(x){
      # does file exist?
      if(
        length(list.files(x, pattern = "chm_0.25m.tif")) == 1
      ){
        terra::rast(paste0(x,"/chm_0.25m.tif"))
      }else{NULL}
    })
    # plot(chm_list_temp[[1]] %>% terra::aggregate(6))

  # mosiac 
  chm_rast = do.call(
    terra::mosaic
    , c(chm_list_temp, fun = "max")
  )
  # plot(chm_rast %>% terra::aggregate(6))
  
  # write
  terra::writeRaster(
    chm_rast
    , paste0(delivery_dir, "/chm_0.25m.tif")
    , overwrite = TRUE
  )
  
  # clean up
    remove(list = ls()[grep("_temp",ls())])
    gc()

##################################################################################
##################################################################################
# combine bottom_up_detected_stem_locations.gpkg (dbh_locations_sf)
# duplicates get removed in the # Model Missing DBH's section
##################################################################################
##################################################################################
  # read list of files and combine
  dbh_locations_sf = list.dirs(getwd(), recursive = F) %>% 
    stringr::str_subset(dir_matches) %>% 
    purrr::map(function(x){
      # does file exist?
      if(
        length(list.files(x, pattern = "bottom_up_detected_stem_locations.gpkg")) == 1
      ){
        sf::st_read(paste0(x,"/bottom_up_detected_stem_locations.gpkg"))
      }else{NULL}
    }) %>% 
    dplyr::bind_rows()
  
  # ggplot(dbh_locations_sf) + geom_sf(shape=".")
    
  ### Write the detected DBHs
    sf:::st_write(
      dbh_locations_sf
      , dsn = paste0(delivery_dir, "/bottom_up_detected_stem_locations.gpkg")
      , append = FALSE
      , delete_dsn = TRUE
      , quiet = TRUE
    )
  
  # clean up
    remove(list = ls()[grep("_temp",ls())])
    gc()
##################################################################################
##################################################################################
# combine raw_las_ctg_info.gpkg (las_ctg@dta)
##################################################################################
##################################################################################
  # read list of files and combine
  las_ctg_dta = list.dirs(getwd(), recursive = F) %>%
    stringr::str_subset(dir_matches) %>% 
    purrr::map(function(x){
      # does file exist?
      if(
        length(list.files(x, pattern = "raw_las_ctg_info.gpkg")) == 1
      ){
        sf::st_read(paste0(x,"/raw_las_ctg_info.gpkg")) %>% 
          sf::st_union() %>% 
          sf::st_as_sf()
      }else{NULL}
    }) %>% 
    dplyr::bind_rows() %>% 
    sf::st_union() %>% 
    sf::st_as_sf()
  
  # ggplot(las_ctg_dta) + geom_sf()
    
  ### Write the detected DBHs
    sf:::st_write(
      las_ctg_dta
      , dsn = paste0(delivery_dir, "/raw_las_ctg_info.gpkg")
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
      crowns = ForestTools::mcws(
        treetops = sf::st_zm(tree_tops, drop = T) # drops z values
        , CHM = chm_rast
        , minHeight = minimum_tree_height
      )
    
    # str(crowns)
    # plot(crowns, col = (viridis::turbo(2000) %>% sample()))
    # crowns %>% terra::freq() %>% nrow()
    
  ### Write the crown raster to the disk
    terra::writeRaster(
      crowns
      , paste0(delivery_dir, "/top_down_detected_tree_crowns.tif")
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
    sf::st_intersection(las_ctg_dta) %>% 
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
              -dplyr::any_of(c("geometry","geom"))
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
      , paste0(delivery_dir, "/top_down_detected_tree_tops.gpkg")
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
      , paste0(delivery_dir, "/top_down_detected_crowns.gpkg")
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
        las_ctg_dta %>% 
          terra::vect() %>% 
          terra::project(terra::crs(treemap_rast))
      ) %>% 
      terra::mask(
        las_ctg_dta %>% 
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
          paste0(delivery_dir, "/regional_dbh_height_model_estimates.csv")
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
        , paste0(delivery_dir, "/final_detected_crowns.gpkg")
        , append = FALSE
        , quiet = TRUE
      )
      # tree top vector points
      sf::st_write(
        # get tree points
        crowns_sf_with_dbh %>% 
          sf::st_drop_geometry() %>% 
          sf::st_as_sf(coords = c("tree_x", "tree_y"), crs = sf::st_crs(crowns_sf_with_dbh))
        , paste0(delivery_dir, "/final_detected_tree_tops.gpkg")
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
          , plot_area_m2 = las_ctg_dta %>% 
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
          , paste0(delivery_dir, "/final_plot_silv_metrics.csv")
          , row.names = F
        )

    # this would just be a vector file if available
      silv_metrics_temp = las_ctg_dta %>% 
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

