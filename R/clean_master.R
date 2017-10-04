library("tidyverse")
raw <- read_csv("f:/Downloads/master_file.csv")

# The only Xpaths for which there is more than one type are those with US dollar
# amounts. For those, there were two data type names, used in different years: 
# "USAmountNNType" and "USAmountType." For our purposes, these are
# interchangeable, so we just choose one and move on.

types <- raw %>%
  group_by(Xpath, Type) %>%
  summarize() %>%
  ungroup() %>%
  group_by(Xpath) %>%
  top_n(1) %>%
  ungroup()

write_csv(types, "f:/Downloads/types.csv")
getwd()
