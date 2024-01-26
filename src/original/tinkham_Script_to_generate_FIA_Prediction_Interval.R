###############################################################################
###                                                                         ###
###   This script reads in FIA tree records and Site Index observations,    ###
###   summarizes plot basal area to determine which plots are dominated by  ###
###   ponderosa pine and within a user defined Site Index and total basal   ###
###   area threshold. Then the remaining ponderosa pine trees are used to   ###
###   create a height to DBH relationship and establish a 90% prediction    ###
###   bound.                                                                ###
###                                                                         ###
###############################################################################

library(ggeffects)
library(ggplot2)
library(tidyverse)
library(nlme)
library(propagate)
library(ggpubr)
library(aomisc)

### User defined values
stemmap_BA <- 40      ### Field inventory basal area in ft^2/acre
PIPO_dominance <- 70  ### Percentage of stand basal area that must be PIPO dominated
stemmap_SI <- 50      ### Field inventory Site Index in height (ft) by age 50

### Read in FIA data downloaded from FIA datamart and filtered to appropriate counties
setwd("C:/Users/WadeTinkham/Documents/Research/UAS_Projects/Hanna_Spatial_Pattern/DBH_Modeling")
fia_trees <- read.csv("SD_TREE.csv")
fia_SI <- read.csv("SD_SITETREE.csv")

### Fill in missing DBH and height values for plots that were recently harvested
for(i in 1:nrow(fia_trees)){
  if(is.na(fia_trees$DIA[i]) == TRUE) {
    fia_trees$DIA[i] = fia_trees$PREVDIA_FLD[i]
    fia_trees$HT[i] = fia_trees$PREV_ACTUALHT_RMRS[i]
  }
}

### Calculate basal area and summarize it by species for each plot
fia_trees$BA <- fia_trees$DIA^2 * 0.005454
fia_trees$UNIQUE_PLOT <- str_c(fia_trees$PLOT, fia_trees$SUBP, fia_trees$INVYR, sep = "_")

summary <- fia_trees %>%
  group_by(SPCD, UNIQUE_PLOT) %>%
  dplyr::summarize(BAHA = sum(BA, na.rm = TRUE) * 24,
            TPA = n() * 24)

### Determine total basal area in each plot and then the percentage basal area by species
summary_2 <- summary %>%
  group_by(UNIQUE_PLOT) %>%
  dplyr::summarize(Total_BAHA = sum(BAHA))

summary <- left_join(summary, summary_2)

summary$SPC_Perctage <- summary$BAHA / summary$Total_BAHA


### Filter out plots not dominated by ponderosa pine (SPCD = 122), so that 
### the meet the user defined percent basal area dominance and 
###  are within 20% of the field inventoried stem map basal area value
summary <- subset(summary, SPCD == 122)
summary <- na.omit(summary)
summary <- subset(summary, SPC_Perctage > PIPO_dominance/100)
summary <- subset(summary, Total_BAHA > stemmap_BA * 0.8 & Total_BAHA < stemmap_BA * 1.2)

### Summarize the FIA Site Index Data and then match this with the FIA tree data
SI_summary <- fia_SI %>%
  group_by(PLOT) %>%
  dplyr::summarise(SITREE = mean(SITREE, na.rm = TRUE), 
            SIBASE = mean(SIBASE, na.rm = TRUE))

fia_trees <- fia_trees %>% 
  left_join(SI_summary, by = c("PLOT"))

### Filter FIA data to keep records from plots with a Site Index within +/- 3m height of the 
### stem map estimated Site Index
fia_trees <- subset(fia_trees, SITREE.y > stemmap_SI - 3*3.28 & SITREE.y < stemmap_SI + 3*3.28 ) ### Filter to appropriate Site Index
fia_trees <- subset(fia_trees, HT = ACTUALHT) ### This removes trees with broken tops

### Select out only ponderosa pine trees from plots and calculate metric attributes
fia_trees <- fia_trees[,c(1:21,213:216)]
fia_trees$dbh.cm <- fia_trees$DIA * 2.54
fia_trees$ht.m <- fia_trees$HT / 3.28
fia_trees <- subset(fia_trees, SPCD == 122)

### Save out csv of filtered FIA trees
write.csv(fia.trees, "filtered_fia_pipo_trees.csv", row.names = FALSE)

###############################################################################
###                                                                         ###
###     Create regional Height to DBH relationship                          ###
###                                                                         ###
###############################################################################

ggplot(data = fia_trees, aes(x = ht.m, y = dbh.cm)) +
  geom_point(alpha = 0.4, size = 0.6) +
  geom_smooth(method = "lm") +
  labs(x = "FIA Height (m)", y = "FIA DBH (cm)") +
  theme_bw()

ggsave("FIA_Height_to_DBH.tiff",  width = 4, height = 4, dpi = 300)

dbh.mod.lm <- lm(dbh.cm ~ ht.m, data = fia_trees)
summary(dbh.mod.lm)
dbh.mod.poly <- nls(dbh.cm ~ b * ht.m^z, start = list(b = 2.2, z = 1), data=fia_trees)
summary(dbh.mod.poly)
model.output <- summary(dbh.mod.poly)
R2nls(dbh.mod.poly)

### Generate prediction bounds for relationship
new.dat <- data.frame(ht.m=seq(1,
                               round(max(fia_trees$ht.m+4.5, na.rm = TRUE), 0),
                               0.5))
mod.pred <- predictNLS(dbh.mod.poly, new.dat, interval = "prediction", alpha = 0.1)
new.dat.pred <- data.frame(ht.itd = new.dat$ht.m, 
                           dbh.itd = mod.pred$summary$Sim.Mean,
                           upr = mod.pred$summary$`Sim.95%`, 
                           lwr = mod.pred$summary$`Sim.5%`)
for(i in 1:nrow(new.dat.pred)){
  if(new.dat.pred$lwr[i] < 0) {
  new.dat.pred$lwr[i] = 0
  }
}

new.dat.pred$range <- new.dat.pred$upr - new.dat.pred$lwr
new.dat.pred$margin <- new.dat.pred$range / 2

### Save prediction bound to file
write.csv(new.dat.pred, "fia_prediction_interval.csv", row.names = FALSE)

### Generate plot of relationship with prediction bounds
formula <- y~poly(x, 2, raw=TRUE)

ggplot(data=new.dat.pred, aes(x = ht.itd, y = dbh.itd))+
  geom_smooth(method="nls", se=FALSE, color = "black", alpha = 0.2, linewidth = 2,
              method.args=list(formula= y ~ b * x ^ z,
                               start = list(b=2.2, z=0.9)))+
  stat_smooth(method = "lm", formula = formula, color ="red", linewidth = 0.5) +
  geom_ribbon(data=new.dat.pred, aes(x=ht.itd, ymin=lwr, ymax=upr), fill="grey", alpha=0.5)+
  theme_bw(base_size = 12)+
  geom_point(data=fia_trees, aes(x=ht.m, y=dbh.cm), alpha=0.3, size = 0.5)+
  xlab("FIA Height (m)")+
  ylab("FIA DBH (cm)")+
  xlim(0, max(new.dat.pred$ht.itd+1, na.rm = TRUE))+
  ylim(0, max(new.dat.pred$upr+1, na.rm = TRUE))+
  annotate("text", x=10, y=max(new.dat.pred$upr-1, na.rm=TRUE), 
           label=bquote(DBH == .(round(model.output$parameters[1,1],3)) (.(round(model.output$parameters[1,2],3)) )
                        * " \u00D7 Height"^.(round(model.output$parameters[2,1],3)) (.(round(model.output$parameters[2,2],3)))),
           parse=FALSE, size = 3.25)+
  annotate("text", x=10, y = max(new.dat.pred$upr-5, na.rm=TRUE), size = 3.25,
           label=bquote("Pseudo R"^2 == .(round(R2nls(dbh.mod.poly)$PseudoR2[1],3)))) +
  annotate("text", x=10, y = max(new.dat.pred$upr-9, na.rm=TRUE), size = 3.25,
           label=bquote("Residual Standard Error" == .(round(model.output[["sigma"]],2))))

### Save out FIA relationship prediction bounds
### Black line is power function and red line is linear function
ggsave("FIA_Height_to_Diameter_Pridiction_Bounds.tiff", width = 4, height = 4, dpi=300)

