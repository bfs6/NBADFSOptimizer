rm(list = ls())

####Read in Libraries####
library(tidyverse)
library(data.table)
library(janitor)
library(RJSONIO)
library(odbc)
library(DBI)
library(RMySQL)


####Connect to AWS MySQL Database####
lapply(dbListConnections(MySQL()), dbDisconnect)
host = "dfs-nba.c0p55ontqjka.us-east-2.rds.amazonaws.com"
port = 3306
dbname = "dfs_nba"
user = "root"
password = "MoSalah11!"

my_db <- dbConnect(RMySQL::MySQL(),
                   dbname = dbname, host = host, port = port,
                   user = user, password = password)


####Create Helper Function####
get_data_from_url <- function(nba_url){
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
##Common Team Info
common_team_url <- "http://stats.nba.com/stats/commonTeamYears/?LeagueID=00"
common_team_info <- 
  common_team_url %>% 
  get_data_from_url() %>% 
  filter(max_year == 2020)

##Team Game Log
team_game_logs_prior <- dbReadTable(my_db, "team_game_logs", row.names = FALSE)
team_game_log_base_url <- "http://stats.nba.com/stats/teamgamelog/?Season=2020-21&SeasonType=Regular%20Season&TeamID="
team_game_log_urls <- paste0(team_game_log_base_url, common_team_info$team_id)
team_game_logs <- 
  team_game_log_urls %>%
  map_dfr(~list(get_data_from_url(.), Sys.sleep(1)))
team_game_logs_add <- 
  team_game_logs %>% 
  anti_join(team_game_logs_prior, by = c("game_id", "team_id"))
dbWriteTable(my_db, "team_game_logs", team_game_logs_add, row.names = FALSE, append = TRUE)

##Player Info
players_url <- "http://stats.nba.com/stats/leaguedashplayerbiostats/?Season=2020-21&PerMode=Totals&LeagueID=00&SeasonType=Regular%20Season"
player_info <-
  players_url %>% 
  get_data_from_url()
dbWriteTable(my_db, "player_info", player_info, row.names = FALSE, overwrite = TRUE)

##Player Stats
player_pt_shot_url <- "http://stats.nba.com/stats/leaguedashplayerptshot/?Season=2020-21&PerMode=Totals&LeagueID=00&SeasonType=Regular%20Season"
player_pt_shot_stats <- 
  player_pt_shot_url %>% 
  get_data_from_url()
dbWriteTable(my_db, "player_pt_shot_stats", player_pt_shot_stats, row.names = FALSE, overwrite = TRUE)

# ##Player Game Log
# player_game_logs_prior <- dbReadTable(my_db, "player_game_logs", row.namees = FALSE)
# player_game_log_urls <- paste0("http://stats.nba.com/stats/playergamelog/?Season=2020-21&SeasonType=Regular%20Season&PlayerID=", player_info$player_id)
# player_game_log_list <- list()
# for(i in seq_along(player_game_log_urls)){
#   print(i)
#   player_game_log_list[[i]] <- 
#     player_game_log_urls[i] %>% 
#     get_data_from_url()
#   Sys.sleep(1)
# }
# player_game_logs <- 
#   player_game_log_list %>% 
#   map_df(~bind_rows(.)) 
# player_game_logs_add <- 
#   player_game_logs %>% 
#   anti_join(player_game_logs_prior, by = c("game_id", "player_id"))
# dbWriteTable(my_db, "player_game_logs", player_game_logs_add, row.names = FALSE, append = TRUE)

##Player ID and Game ID
team_game_ids_add <- 
  team_game_logs_add %>% 
  select(team_id, game_id) %>% 
  distinct()
game_ids_add <- 
  team_game_ids_add %>% 
  select(game_id) %>% 
  arrange(game_id) %>%
  distinct() %>% 
  pull(game_id)


# game_ids_add <- 
#   team_game_logs_prior %>% 
#   select(game_id) %>% 
#   distinct() %>% 
#   anti_join(schmeh %>% 
#               select(game_id), by = "game_id") %>% 
#   distinct() %>% 
#   pull(game_id)

##Boxscore Advanced
boxscore_advanced_base_url <- "http://stats.nba.com/stats/boxscoreadvancedv2/?StartPeriod=1&EndPeriod=12&startRange=0&endRange=2147483647&rangeType=0&GameID="
boxscore_advanced_previous <- dbReadTable(my_db, "boxscore_advanced_raw")
boxscore_advanced_games_to_add <- 
  team_game_logs %>% 
  select(team_id, game_id) %>% 
  distinct() %>% 
  anti_join(boxscore_advanced_previous %>% 
              select(team_id, game_id) %>% 
              distinct(),
            by = c("game_id", "team_id")) %>% 
  select(game_id) %>% 
  distinct() %>% 
  pull(game_id)
if(length(boxscore_advanced_games_to_add) > 0){
  boxscore_advanced_urls <- paste0(boxscore_advanced_base_url, boxscore_advanced_games_to_add)
  boxscore_advanced_list <- list()
  for(i in seq_along(boxscore_advanced_urls)){
    print(i)
    boxscore_advanced_list[[i]] <- 
      boxscore_advanced_urls[i] %>% 
      get_data_from_url()
    Sys.sleep(0.1)
  }
  boxscore_advanced_df <-
    boxscore_advanced_list %>% 
    map_df(~bind_rows(.)) %>% 
    distinct()
  dbWriteTable(my_db, "boxscore_advanced_raw", boxscore_advanced_df, row.names = FALSE, append = TRUE)
}

##Boxscore Four Factors
boxscore_four_factors_base_url <- "http://stats.nba.com/stats/boxscorefourfactorsv2/?StartPeriod=1&EndPeriod=12&startRange=0&endRange=2147483647&rangeType=0&GameID="
boxscore_four_factors_previous <- dbReadTable(my_db, "boxscore_four_factors_raw")
boxscore_four_factors_games_to_add <- 
  team_game_logs %>% 
  select(team_id, game_id) %>% 
  distinct() %>% 
  anti_join(boxscore_four_factors_previous %>% 
              select(team_id, game_id) %>% 
              distinct(),
            by = c("game_id", "team_id")) %>% 
  select(game_id) %>% 
  distinct() %>% 
  pull(game_id)
if(length(boxscore_four_factors_games_to_add) > 0){
  boxscore_four_factors_urls <- paste0(boxscore_four_factors_base_url, boxscore_four_factors_games_to_add)
  boxscore_four_factors_list <- list()
  for(i in seq_along(boxscore_four_factors_urls)){
    print(i)
    boxscore_four_factors_list[[i]] <- 
      boxscore_four_factors_urls[i] %>% 
      get_data_from_url()
    Sys.sleep(0.1)
  }
  boxscore_four_factors_df <-
    boxscore_four_factors_list %>% 
    map_df(~bind_rows(.)) %>% 
    distinct()
  dbWriteTable(my_db, "boxscore_four_factors_raw", boxscore_four_factors_df, row.names = FALSE, append = TRUE)
}

##Box Score Miscellaneous
boxscore_misc_base_url <- "http://stats.nba.com/stats/boxscoremiscv2/?StartPeriod=1&EndPeriod=12&startRange=0&endRange=2147483647&rangeType=0&GameID="
boxscore_misc_previous <- dbReadTable(my_db, "boxscore_misc_raw")
boxscore_misc_games_to_add <- 
  team_game_logs %>% 
  select(team_id, game_id) %>% 
  distinct() %>% 
  anti_join(boxscore_misc_previous %>% 
              select(team_id, game_id) %>% 
              distinct(),
            by = c("game_id", "team_id")) %>% 
  select(game_id) %>% 
  distinct() %>% 
  pull(game_id)
if(length(boxscore_misc_games_to_add) > 0){
  boxscore_misc_urls <- paste0(boxscore_misc_base_url, boxscore_misc_games_to_add)
  boxscore_misc_list <- list()
  for(i in seq_along(boxscore_misc_urls)){
    print(i)
    boxscore_misc_list[[i]] <- 
      boxscore_misc_urls[i] %>% 
      get_data_from_url()
    Sys.sleep(0.1)
  }
  boxscore_misc_df <-
    boxscore_misc_list %>% 
    map_df(~bind_rows(.)) %>% 
    distinct()
  dbWriteTable(my_db, "boxscore_misc_raw", boxscore_misc_df, row.names = FALSE, append = TRUE)
}

##Box Score Player Tracking
boxscore_player_track_base_url <- "http://stats.nba.com/stats/boxscoreplayertrackv2/?GameID="
boxscore_player_track_previous <- dbReadTable(my_db, "boxscore_player_track_raw")
boxscore_player_track_games_to_add <- 
  team_game_logs %>% 
  select(team_id, game_id) %>% 
  distinct() %>% 
  anti_join(boxscore_player_track_previous %>% 
              select(team_id, game_id) %>% 
              distinct(),
            by = c("game_id", "team_id")) %>% 
  select(game_id) %>% 
  distinct() %>% 
  pull(game_id)
if(length(boxscore_player_track_games_to_add) > 0){
  boxscore_player_track_urls <- paste0(boxscore_player_track_base_url, boxscore_player_track_games_to_add)
  boxscore_player_track_list <- list()
  for(i in seq_along(boxscore_player_track_urls)){
    print(i)
    boxscore_player_track_list[[i]] <- 
      boxscore_player_track_urls[i] %>% 
      get_data_from_url()
    Sys.sleep(0.1)
  }
  boxscore_player_track_df <-
    boxscore_player_track_list %>% 
    map_df(~bind_rows(.)) %>% 
    distinct()
  dbWriteTable(my_db, "boxscore_player_track_raw", boxscore_player_track_df, row.names = FALSE, append = TRUE)
}

##Box Score Scoring
boxscore_scoring_base_url <- "http://stats.nba.com/stats/boxscorescoringv2/?StartPeriod=1&EndPeriod=12&startRange=0&endRange=2147483647&rangeType=0&GameID="
boxscore_scoring_previous <- dbReadTable(my_db, "boxscore_scoring_raw")
boxscore_scoring_games_to_add <- 
  team_game_logs %>% 
  select(team_id, game_id) %>% 
  distinct() %>% 
  anti_join(boxscore_scoring_previous %>% 
              select(team_id, game_id) %>% 
              distinct(),
            by = c("game_id", "team_id")) %>% 
  select(game_id) %>% 
  distinct() %>% 
  pull(game_id)
if(length(boxscore_scoring_games_to_add) > 0){
  boxscore_scoring_urls <- paste0(boxscore_scoring_base_url, boxscore_scoring_games_to_add)
  boxscore_scoring_list <- list()
  for(i in seq_along(boxscore_scoring_urls)){
    print(i)
    boxscore_scoring_list[[i]] <- 
      boxscore_scoring_urls[i] %>% 
      get_data_from_url()
    Sys.sleep(0.1)
  }
  boxscore_scoring_df <-
    boxscore_scoring_list %>% 
    map_df(~bind_rows(.)) %>% 
    distinct()
  dbWriteTable(my_db, "boxscore_scoring_raw", boxscore_scoring_df, row.names = FALSE, append = TRUE)
}

##Box Score Traditional
boxscore_traditional_base_url <- "http://stats.nba.com/stats/boxscoretraditionalv2/?StartPeriod=1&EndPeriod=12&startRange=0&endRange=2147483647&rangeType=0&GameID="
boxscore_traditional_previous <- dbReadTable(my_db, "boxscore_traditional_raw")
boxscore_traditional_games_to_add <- 
  team_game_logs %>% 
  select(team_id, game_id) %>% 
  distinct() %>% 
  anti_join(boxscore_traditional_previous %>% 
              select(team_id, game_id) %>% 
              distinct(),
            by = c("game_id", "team_id")) %>% 
  select(game_id) %>% 
  distinct() %>% 
  pull(game_id)
if(length(boxscore_traditional_games_to_add) > 0){
  boxscore_traditional_urls <- paste0(boxscore_traditional_base_url, boxscore_traditional_games_to_add)
  boxscore_traditional_list <- list()
  for(i in seq_along(boxscore_traditional_urls)){
    print(i)
    boxscore_traditional_list[[i]] <- 
      boxscore_traditional_urls[i] %>% 
      get_data_from_url()
    Sys.sleep(0.1)
  }
  boxscore_traditional_df <-
    boxscore_traditional_list %>% 
    map_df(~bind_rows(.)) %>% 
    distinct()
  dbWriteTable(my_db, "boxscore_traditional_raw", boxscore_traditional_df, row.names = FALSE, append = TRUE)
}

##Box Score Usage
boxscore_usage_base_url <- "http://stats.nba.com/stats/boxscoreusagev2/?StartPeriod=1&EndPeriod=12&startRange=0&endRange=2147483647&rangeType=0&GameID="
boxscore_usage_previous <- dbReadTable(my_db, "boxscore_usage_raw")
boxscore_usage_games_to_add <- 
  team_game_logs %>% 
  select(team_id, game_id) %>% 
  distinct() %>% 
  anti_join(boxscore_usage_previous %>% 
              select(team_id, game_id) %>% 
              distinct(),
            by = c("game_id", "team_id")) %>% 
  select(game_id) %>% 
  distinct() %>% 
  pull(game_id)
if(length(boxscore_usage_games_to_add) > 0){
  boxscore_usage_urls <- paste0(boxscore_usage_base_url, boxscore_usage_games_to_add)
  boxscore_usage_list <- list()
  for(i in seq_along(boxscore_usage_urls)){
    print(i)
    boxscore_usage_list[[i]] <- 
      boxscore_usage_urls[i] %>% 
      get_data_from_url()
    Sys.sleep(0.1)
  }
  boxscore_usage_df <-
    boxscore_usage_list %>% 
    map_df(~bind_rows(.)) %>% 
    distinct()
  dbWriteTable(my_db, "boxscore_usage_raw", boxscore_usage_df, row.names = FALSE, append = TRUE)
}


####Merge Tables####
##Read in All Player Tables
boxscore_tables <- 
  my_db %>% 
  dbListTables() %>% 
  str_subset("^boxscore") %>% 
  map(~dbReadTable(my_db, .) %>% 
        mutate_all(~na_if(., "")))

##Merge All Boxscore Tables
full_player_boxscore_data <- 
  boxscore_tables %>%
  reduce(left_join, by = c("player_id", "game_id", "team_id", 
                           "team_abbreviation", "team_city", "player_name",
                           "start_position", "comment", "min")) %>% 
  rename_at(
    vars(ends_with(".x")),
    ~str_replace(., "\\..$", "")) %>%
  select(-ends_with(".y"))

##Merge in Player and Team Tables
player_info_merge <- 
  player_info %>% 
  select(-c(player_name, team_abbreviation, college, country,
            draft_year, draft_round, draft_number, player_height)) %>% 
  rename_at(vars(c(pts, reb, ast, net_rating), ends_with("_pct")),
            ~paste(., "season", sep = "_"))

player_pt_shot_stats_merge <- 
  player_pt_shot_stats %>% 
  select(-c(player_name, player_last_team_id, player_last_team_abbreviation, age, gp, g, fga_frequency)) %>% 
  rename_at(vars(-player_id), 
            ~paste(., "season", sep = "_"))

team_game_logs_merge <- 
  team_game_logs %>% 
  select(-c(game_date, matchup, w, l, min)) %>% 
  rename_at(vars(-c(team_id, game_id)),
            ~paste(., "game_team", sep = "_"))

full_player_boxscore_data_final <- 
  full_player_boxscore_data %>% 
  left_join(player_info_merge, by = c("team_id", "player_id")) %>% 
  left_join(player_pt_shot_stats_merge, by = "player_id") %>% 
  left_join(team_game_logs_merge, by = c("team_id", "game_id"))

dbWriteTable(my_db, "full_player_boxscore_data", full_player_boxscore_data_final, row.names = FALSE, overwrite = TRUE)






