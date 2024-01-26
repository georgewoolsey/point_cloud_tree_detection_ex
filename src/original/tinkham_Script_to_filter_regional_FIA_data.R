###############################################################################
###                                                                         ###
###   This script reads in Colorado FIA tree records, summarizes plot       ###
###   basal area to determine which plots are dominated by ponderosa pine   ###
###   and then creates a height to DBH relationship for these trees.        ###
###                                                                         ###
###############################################################################

library(ggeffects)
library(ggplot2)
library(tidyverse)
library(nlme)
library(propagate)
library(ggpubr)
library(aomisc)

### Read in FIA data downloaded from FIA datamart and filtered to appropriate counties
setwd("C:/Users/WadeTinkham/Documents/Research/UAS_Projects/Hanna_Spatial_Pattern/DBH_Modeling")
fia_trees <- read.csv("SD_TREE.csv")
fia_SI <- read.csv("SD_SITETREE.csv")

### Calculate basal area and summarize it by species for each plot
fia_trees$BA <- fia_trees$DIA^2 * 0.005454
fia_trees$UNIQUE_PLOT <- str_c(fia_trees$PLOT, fia_trees$SUBP, sep = "_")

summary <- fia_trees %>%
  group_by(SPCD, PLOT, UNIQUE_PLOT, INVYR) %>%
  summarise(BAHA = sum(BA) * 10,
            TPA = n() * 10)

### Filter out plots not dominated by ponderosa pine (SPCD = 122) and 
### that are too open grown
summary <- subset(summary, SPCD == 122)
summary <- na.omit(summary)
summary <- subset(summary, BAHA > 60)

### Select out trees from plots dominated by ponderosa pine with 
### similar site indices
fia.trees <- fia_trees %>%
  filter(PLOT %in% c(summary$PLOT))
fia.trees <- fia.trees[,c(1:21)]
fia.trees$dbh.cm <- fia.trees$DIA * 2.54
fia.trees$ht.m <- fia.trees$HT / 3.28
#fia.trees <- subset(fia.trees, ht.m > 5)
fia.trees <- subset(fia.trees, SPCD == 122)

SI_summary <- fia_SI %>%
  group_by(PLOT) %>%
  summarise(SITREE = mean(SITREE), SIBASE = mean(SIBASE))

fia.trees <- fia.trees %>% 
  left_join(SI_summary, by = c("PLOT"))

fia.trees <- subset(fia.trees, SITREE > 34.9) ### Filter to appropriate Site Index
fia.trees <- subset(fia.trees, HT = ACTUALHT) ### This removes trees with broken tops

### Save out csv of filtered FIA trees
write.csv(fia.trees, "fia_pipo_trees.csv", row.names = FALSE)

###############################################################################
###                                                                         ###
###     Create regional Height to DBH relationship                          ###
###                                                                         ###
###############################################################################

ggplot(data = fia.trees, aes(x = ht.m, y = dbh.cm)) +
  geom_point(alpha = 0.4, size = 0.6) +
  geom_smooth(method = "lm") +
  labs(x = "FIA Height (m)", y = "FIA DBH (cm)") +
  theme_bw()

ggsave("FIA_Height_to_DBH.tiff",  width = 4, height = 4, dpi = 300)

dbh.mod.lm <- lm(dbh.cm ~ ht.m, data = fia.trees)
summary(dbh.mod.lm)
dbh.mod <- nls(dbh.cm ~ b * ht.m^z, start = list(b = 2.2, z = 1), data=fia.trees)
summary(dbh.mod)
model.output <- summary(dbh.mod)
R2nls(dbh.mod)

### Generate prediction bounds for relationship
new.dat <- data.frame(ht.m=seq(1.5,round(max(fia.trees$ht.m+0.5, na.rm = TRUE),0),0.5))
mod.pred <- predictNLS(dbh.mod, new.dat, interval = "prediction", alpha = 0.1)
new.dat.pred <- data.frame(ht.itd = new.dat$ht.m, dbh.itd = mod.pred$summary$Sim.Mean,
                           upr = mod.pred$summary$`Sim.95%`, lwr = mod.pred$summary$`Sim.5%`)
for(i in 1:nrow(new.dat.pred)){
  if(new.dat.pred$lwr[i] < 0) {
  new.dat.pred$lwr[i] = 0
  }
}

new.dat.pred$range <- new.dat.pred$upr - new.dat.pred$lwr
new.dat.pred$margin <- new.dat.pred$range / 2

write.csv(new.dat.pred, "fia_prediction_interval.csv", row.names = FALSE)

### Generate plot of relationship with prediction bounds
formula <- y~poly(x, 2, raw=TRUE)

ggplot(data=new.dat.pred, aes(x = ht.itd, y = dbh.itd))+
  geom_smooth(method="nls", se=FALSE, color = "black", alpha = 0.2,
              method.args=list(formula= y ~ b * x ^ z,
                               start = list(b=2.2, z=0.9)))+
  stat_smooth(method = "lm", formula = formula, color ="black") +
  geom_ribbon(data=new.dat.pred, aes(x=ht.itd, ymin=lwr, ymax=upr), fill="grey", alpha=0.5)+
  theme_bw(base_size = 12)+
  geom_point(data=fia.trees, aes(x=ht.m, y=dbh.cm), alpha=0.3, size = 0.5)+
  xlab("FIA Height (m)")+
  ylab("FIA DBH (cm)")+
  xlim(0, max(fia.trees$ht.m+1, na.rm = TRUE))+
  annotate("text", x=7, y=max(fia.trees$dbh.cm-1, na.rm=TRUE), 
           label=bquote(DBH == .(round(model.output$parameters[1,1],3)) * " \u00D7 Height"^.(round(model.output$parameters[2,1],3))),
           parse=FALSE, size = 3.25)+
  annotate("text", x=7, y = max(fia.trees$dbh.cm-5, na.rm=TRUE),
           label=bquote("Pseudo R"^2 == .(round(R2nls(dbh.mod)$PseudoR2[1],3))))

### Save out FIA relationship prediction bounds
### Black line is power function and red line is linear function
ggsave("FIA_Height_to_Diameter_Pridiction_Bounds.tiff", width = 4, height = 4, dpi=300)

