rm(list = ls())

####Read in Libraries####
library(tidyverse)
library(data.table)
library(janitor)
library(progress)
library(RJSONIO)


####Create Helper Function####
get_data_from_url <- function(nba_url, progress_bar = FALSE){
  ##Set Headers
  headers <- c(
    `Host` = 'stats.nba.com',
    `User-Agent` = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv =72.0) Gecko/20100101 Firefox/72.0',
    `Accept` = 'application/json, text/plain, */*',
    `Accept-Language` = 'en-US,en;q=0.5',
    `Accept-Encoding` = 'gzip, deflate, br',
    `x-nba-stats-origin` = 'stats',
    `x-nba-stats-token` = 'true',
    `Connection` = 'keep-alive',
    `Referer` = 'https =//stats.nba.com/',
    `Pragma` = 'no-cache',
    `Cache-Control` = 'no-cache'
  )
  
  ##Progress Bar
  # if(progress_bar == TRUE){
  #   pb$tick(1)
  #   Sys.sleep(1 / 100)
  # }
  
  ##Get Data
  res <-
    httr::GET(nba_url,
              httr::add_headers(.headers = headers))
  res_json <-
    res$content %>%
    rawToChar() %>%
    RJSONIO::fromJSON(simplifyVector = TRUE, nullValue = NA)
  res_info <- data.frame(do.call(rbind, res_json$resultSets[[1]]$rowSet))
  names(res_info) <- res_json$resultSets[[1]]$headers
  res_info_clean <-  
    res_info %>%
    mutate_if(is.list, unlist) %>% 
    clean_names()
  return(res_info_clean)
}


####Read in Data####
##PlayerInfo
players_url <- "http://stats.nba.com/stats/leaguedashplayerbiostats/?Season=2020-21&PerMode=Totals&LeagueID=00&SeasonType=Regular%20Season"
player_info <-
  players_url %>% 
  get_data_from_url()

##Player Stats
player_pt_shot_url <- "http://stats.nba.com/stats/leaguedashplayerptshot/?Season=2020-21&PerMode=Totals&LeagueID=00&SeasonType=Regular%20Season"
player_pt_shot_stats <- 
  player_pt_shot_url %>% 
  get_data_from_url()

##Player Game Log
player_game_log_urls <- paste0("http://stats.nba.com/stats/playergamelog/?Season=2020-21&SeasonType=Regular%20Season&PlayerID=", player_info$player_id)
# pb <- progress_bar$new(format = "[:bar] :current/:total (:percent)", total = length(player_game_log_urls))
# pb$tick(0)
# counter = 0
# player_game_logs_list <- 
#   player_game_log_urls %>% 
#   map_df(~list(get_data_from_url(., progress_bar = FALSE), counter = counter + 1, print(counter)))
# 
player_game_log_list <- list()
for(i in seq_along(player_game_log_urls)[-c(1:45)]){
  print(i)
  player_game_log_list[[i]] <- 
    player_game_log_urls[i] %>% 
    get_data_from_url()
}

