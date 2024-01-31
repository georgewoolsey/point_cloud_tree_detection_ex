remove(list = ls()[])
gc()

# setup
setwd("c:/Data/usfs/point_cloud_tree_detection_ex/data/")
delivery_dir = paste0(getwd(),"/point_cloud_processing_BHEF_202306_combined")
# list.files(delivery_dir)

# library
library(tidyverse) # the tidyverse
library(viridis) # viridis colors
library(scales) # work with number and plot scales
# spatial analysis
library(terra) # raster
library(sf) # simple features
# visualization
library(kableExtra)
library(patchwork) # ! better align plots in grid
library(mapview) # interactive html maps
# remove.packages("ggmap")
# devtools::install_github("stadiamaps/ggmap")
library(ggmap)

# option to put satellite imagery as base layer
  mapview::mapviewOptions(
    homebutton = FALSE
    , basemaps = c("Esri.WorldImagery","OpenStreetMap")
  )

# read data
# dtm_rast = terra::rast(paste0(delivery_dir, "/dtm_1m.tif"))
chm_rast = terra::rast(paste0(delivery_dir, "/chm_0.25m.tif"))
dbh_locations_sf = sf::st_read(paste0(delivery_dir, "/bottom_up_detected_stem_locations.gpkg"))
crowns = terra::rast(paste0(delivery_dir, "/top_down_detected_tree_crowns.tif"))
crowns_sf_with_dbh = sf::st_read(paste0(delivery_dir, "/final_detected_crowns.gpkg"))
treetops_sf_with_dbh = sf::st_read(paste0(delivery_dir, "/final_detected_tree_tops.gpkg"))
silv_metrics_temp = readr::read_csv(paste0(delivery_dir, "/final_plot_silv_metrics.csv"))
las_ctg_dta = sf::st_read(paste0(delivery_dir, "/raw_las_ctg_info.gpkg"))

##################################################################################
##################################################################################
# summary table
##################################################################################
##################################################################################
  
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

##################################################################################
##################################################################################
### Relationship between height and DBH
##################################################################################
##################################################################################
  
### plot
  ggplot(
    data = crowns_sf_with_dbh
    , mapping = aes(y=tree_height_m, x = dbh_cm)
  ) +
  geom_point(
    mapping = aes(color = is_training_data)  
    , alpha = 0.6
    , size = 0.5
  ) + 
  geom_smooth(
    method = "loess"
    , se = F
    , span = 1
    , color = "gray44"
    , alpha = 0.7
  ) +
  labs(
    x = "DBH (cm)"
    , y = "Tree Ht. (m)"
    , color = "Training Data"
    , title = "SfM derived tree height and DBH relationship"
  ) +
  scale_color_manual(values = c("gray", "firebrick")) +
  theme_light() +
  theme(
    legend.position = "bottom"
    , legend.direction = "horizontal"
  ) +
  guides(
    color = guide_legend(override.aes = list(shape = 15, size = 6, alpha = 1))
  )

##################################################################################
##################################################################################
### DBH histogram training vs non-training
##################################################################################
##################################################################################
### plot
  ggplot(
    data = crowns_sf_with_dbh
    , mapping = aes(x = dbh_cm, group = is_training_data, fill = is_training_data)
  ) +
  geom_density(alpha = 0.6, binwidth = 2, color = NA) + 
  labs(
    x = "DBH (cm)"
    , y = "density"
    , fill = "Training Data"
    , title = "SfM derived tree DBH distribution"
  ) +
  scale_fill_manual(values = c("gray", "firebrick")) +
  scale_x_continuous(breaks = scales::extended_breaks(n=20)) +
  theme_light() +
  theme(
    legend.position = "bottom"
    , legend.direction = "horizontal"
  )

##################################################################################
##################################################################################
# chm on html mapview
##################################################################################
##################################################################################
  # aggregate raster and map
  chm_rast %>%
    terra::aggregate(fact=4) %>% 
    stars::st_as_stars() %>% 
    mapview::mapview(
      layer.name = "canopy ht.(m)"
      , alpha.regions = 0.7
    )
##################################################################################
##################################################################################
# chm + trees
##################################################################################
##################################################################################

  ggplot() +
    geom_tile(
      data = chm_rast %>% terra::aggregate(fact=2) %>% as.data.frame(xy=T) %>% dplyr::rename(f=3)
      , mapping = aes(x=x,y=y,fill=f)
    ) +
    geom_sf(
      data = treetops_sf_with_dbh
      # , size = 0.3
      , shape = "."
    ) +
    coord_sf(expand = F) +
    scale_fill_viridis_c(option = "plasma") +
    labs(
      fill="canopy\nht (m)"
      , title = "BHEF Units 1-4 UAS Flights 2023-06"
    ) +  
    theme_light() +
    theme(
      # legend.position =  "top"
      # , legend.direction = "horizontal"
      legend.title = element_text(size = 8, face = "bold")
      , legend.margin = margin(c(0,0,0,0))
      , plot.title = element_text(size = 10, face = "bold") #, hjust = 0.5
      , axis.title = element_blank()
      , axis.text = element_blank()
      , axis.ticks = element_blank()
      , panel.grid = element_blank()
      , plot.margin = margin(0, 0, 0, 0, "cm")
      , plot.caption = element_text(color = "black", hjust = 0, vjust = 3)
    )
  ggplot2::ggsave(
    filename = paste0(delivery_dir,"/plt_chm_treetops.jpeg")
    , plot = ggplot2::last_plot()
    , width = 15
    , height = 13.5
    , units = "in"
    , dpi = "print"
  )
# clean up
  gc()
  remove(list = ls()[grep("_temp",ls())])
##################################################################################
##################################################################################
# try some ggmap !!!!! not working rn
##################################################################################
##################################################################################
if(F){
  ##################hack to align plots for ggmap
  plt_crs = 3857
  ggmap_bbox_fn <- function(map, my_crs=3857) {
      if (!inherits(map, "ggmap")) stop("map must be a ggmap object")
      # Extract the bounding box (in lat/lon) from the ggmap to a numeric vector, 
      # and set the names to what sf::st_bbox expects:
      map_bbox <- setNames(unlist(attr(map, "bb")), c("ymin", "xmin", "ymax", "xmax"))
      # Convert the bbox to an sf polygon, transform it to 3857, 
      # and convert back to a bbox (convoluted, but it works)
      bbox_3857 <- st_bbox(st_transform(st_as_sfc(st_bbox(map_bbox, crs = 4326)), my_crs))
      # Overwrite the bbox of the ggmap object with the transformed coordinates 
      attr(map, "bb")$ll.lat <- bbox_3857["ymin"]
      attr(map, "bb")$ll.lon <- bbox_3857["xmin"]
      attr(map, "bb")$ur.lat <- bbox_3857["ymax"]
      attr(map, "bb")$ur.lon <- bbox_3857["xmax"]
      map
  }
  
  # bounding box
    bb_temp <- 
      # use extent of 
      treetops_sf_with_dbh %>% 
      sf::st_bbox() %>% 
      sf::st_as_sfc() %>% 
      sf::st_transform(crs=5070) %>% 
      sf::st_buffer(as.numeric(100)) %>% 
      sf::st_transform(crs=4326) %>% # same as get_map return
      sf::st_bbox()
      
    # set bbox for get call
    bbox_temp <- c(
      bottom = bb_temp[[2]]
      , top = bb_temp[[4]]
      , right = bb_temp[[3]]
      , left = bb_temp[[1]]
    )
    # get map
    hey_ggmap <- ggmap::get_stadiamap(
      bbox = bbox_temp
      , zoom = 16
      , maptype = "stamen_toner_lite" #"toner-hybrid" # "stamen_terrain" 
      , crop = T
    )
    # ggmap(hey_ggmap)
    # apply align function
    hey_ggmap_aligned <- ggmap_bbox_fn(hey_ggmap, plt_crs) # Use the function
    
    # plot
    # plt_trees = 
      ggmap(hey_ggmap_aligned) +
        geom_sf(
        data= treetops_sf_with_dbh %>% 
          dplyr::filter(tree_height_m>=quantile(crowns_sf_with_dbh$tree_height_m, probs=0.75)) %>% 
          sf::st_transform(crs=4326)
        , size = 0.5
      )
      geom_sf(
        data = crowns_sf_with_dbh %>% 
          dplyr::filter(tree_height_m>=quantile(crowns_sf_with_dbh$tree_height_m, probs=0.75)) %>% 
          sf::st_transform(crs=plt_crs)
        , mapping = aes(fill = tree_height_m)
        , color = NA
      ) 
      +
      geom_sf(
        data= treetops_sf_with_dbh %>% 
          dplyr::filter(tree_height_m>=quantile(crowns_sf_with_dbh$tree_height_m, probs=0.75)) %>% 
          sf::st_transform(crs=plt_crs)
        , size = 0.5
      ) +
      coord_sf(expand = F) +
      scale_fill_viridis_c(option = "plasma") +
      labs(
        fill="canopy\nht (m)"
        , title = "BHEF Units 1-4 UAS Flights 2023-06"
      ) +  
      theme_light() +
      theme(
        # legend.position =  "top"
        # , legend.direction = "horizontal"
        legend.title = element_text(size = 8, face = "bold")
        , legend.margin = margin(c(0,-2,0,0))
        , plot.title = element_text(size = 9, face = "bold") #, hjust = 0.5
        , axis.title = element_blank()
        , axis.text = element_blank()
        , axis.ticks = element_blank()
        , panel.grid = element_blank()
        , plot.margin = margin(0, 0, 0, 0, "cm")
        , plot.caption = element_text(color = "black", hjust = 0, vjust = 3)
      )
    
    plt_trees
    ggplot2::ggsave(
      filename = paste0(delivery_dir,"/plt_chm_treetops.jpeg")
      , plot = plt_trees
      , width = 11
      , height = 10
      , units = "in"
      , dpi = "print"
    )
}
