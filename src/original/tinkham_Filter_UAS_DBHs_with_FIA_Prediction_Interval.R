

library(plyr)



setwd("C:/Users/WadeTinkham/Documents/Research/UAS_Projects/DBH_Modeling_Paper")
fia_interval <- read.csv("fia_prediction_interval.csv")
uas_trees <- read.csv("final_uas_trees_unfiltered.csv")
summary(uas_trees)
names(uas_trees)

### Standardize Parameters
uas_trees$predicted_dbh <- 1.6121 * uas_trees$tree_heigh + uas_trees$tree_heigh^0.0276
uas_trees$uas.dbh <- uas_trees$dbh * 100
uas_trees$ht_interval <- round_any(uas_trees$tree_heigh, 0.5)

uas_trees_w_dbh <- subset(uas_trees, uas.dbh > 0)
uas_trees_no_dbh <- uas_trees[is.na(uas_trees$uas.dbh),]

for(i in 1:nrow(uas_trees_w_dbh)) {
  upr <- fia_interval$upr[fia_interval$ht.itd == uas_trees_w_dbh$ht_interval[i]]
  lwr <- fia_interval$lwr[fia_interval$ht.itd == uas_trees_w_dbh$ht_interval[i]]
  
  if(uas_trees_w_dbh$uas.dbh[i] < lwr || uas_trees_w_dbh$uas.dbh[i] > upr) {
    uas_trees_w_dbh$uas.dbh[i] <- NA
    print("Bad UAS DBH Value")
  } else {
    print("Good UAS DBH Value")
  }
}

uas_trees_merged <- rbind(uas_trees_no_dbh, uas_trees_w_dbh)

write.csv(uas_trees_merged, "final_uas_trees_filtered.csv", row.names = FALSE)


ggplot(data = uas_trees, aes(x=uas.dbh, fill = "green")) +
  geom_histogram(binwidth = 5, center = 1, alpha = 0.4) +
  geom_histogram(data = uas_trees_merged, aes(x = uas.dbh, fill="blue"), binwidth = 5, center = 1, alpha=0.4) +
  xlab("UAS Extracted DBH Values (cm)") +
  xlim(0,200) +
  scale_fill_manual(name = "Source", labels = c("Unfiltered", "FIA Filtered"), values = c("blue", "green")) +
  guides(fill = guide_legend(overrides.aes = list(alpha = 0.4))) +
  theme_bw() 
  
ggsave("Analysis/Figures/DBH_filtered_vs_unfiltered.tiff", width = 4, height = 3, dpi = 300)

