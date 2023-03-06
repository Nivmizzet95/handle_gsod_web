##### handle_gsod new ##########################################################

## setting the working directory is not necessarily needed if launched correctly
## for example from correct project etc.
# getwd()
# setwd("")



### packages needed for the function
# install.packages("httr")
library(httr)       # used to download through GET() function 
# install.packages("dplyr")
library(dplyr)      # mainly for pipeing %>% but also for filter() and mutate()
# install.packages("stringr")
library(stringr)    # for string handling functions like replace / remove
# install.packages("data.table")
library(data.table) # needed for custom multimerge function
# install.packages("padr")
library(padr)       # fill data set with NAs for missing days
# install.packages("DescTools")
library(DescTools)
# install.packages("progress")
library(progress)

handle_gsod_new <- function(action,                      ##### can now also be set to 'delete' and work with clean_up (see below) to delete data
                         time_interval = NULL,
                         location = NULL,
                         stations_to_choose_from = 25,
                         end_at_present = FALSE,
                         drop_most = TRUE,
                         add.Date = TRUE,             ##### necessary?
                         ##### not implemented
                         # add_station_name = TRUE,   ##### necessary?
                         ##### new options below
                         # quiet replaced with verbose('normal', 'detailed', 'quiet') more info below
                         # station.list is standard procedure now
                         update_station_list = FALSE, ##### load stationlist from disk if found in 'path' else download - if true, always download
                         path = "climate_data",       ##### sets the directory name for the downloaded files, now variable. original was 'chillRtempdirectory'
                         update_all = FALSE,          ##### tries to download everything, otherwise (FALSE) only downloads missing data
                         clean_up = NULL,             ##### in combination with 'action = delete', this can be set to 'all' to delete all weather data, or 'station' if only data from specific stations ('location') should be deleted
                         max_distance = 150,          ##### max station distance in km
                         min_overlap = 0,             ##### minimal overlap that the stations-recording-dates should have with input date (time_interval) [%] - setting this too high might lead to no stations or only far away ones
                         verbose = "normal"           ##### can be set to 'normal' for almost no feedback; or 'detailed' for more information; or anything else like 'quiet' for no feedback
){ 
  
  if (is.character(action)){
    
    if (action == "list_stations") {
      
      start_y <- time_interval[1]
      ifelse(length(time_interval)==2,
             end_y <- time_interval[2],
             end_y <- start_y)
      if (end_at_present==T) end_y <- format(Sys.Date(),"%Y")
      years <- c(start_y:end_y)
      amount_of_years <- length(years)
      
      if (!dir.exists(path)) dir.create(path)
      
      ## statlist download, load_in and pre-preparation
      if (!file.exists(paste0(path,"/station_list.csv"))|update_station_list==T) {
        GET("https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv",
            write_disk(paste0(path,"/station_list.csv"), overwrite = T)) %>%
          invisible()
        if(verbose=="detailed")cat("Downloaded newest version of the stationlist.\n  'update_station_list' can be set to 'FALSE'.")
      }else if(verbose=="detailed")cat("Stationlist was loaded from disk.\n  Set 'update_station_list' to 'TRUE' to update it (~3MB).")
      
      stat_list <- 
        read.csv(paste0(path,"/station_list.csv")) %>%
        rename(Long = contains("lo"),
               Lat = contains("la"),
               Elev = contains("ele")) %>% 
        mutate(chillR_code = paste(.$USAF, .$WBAN, sep = "")) %>% 
        filter(!is.na(Lat)|!is.na(Long)) %>% 
        filter(Lat != 0,Long != 0)
      
      ###### statlist gets cleaned (also when action == "download_weather") ####
      ###### needs to be done seperately since the location parameter is ####### 
      ###### diffrent - here its lat and long - in DW its chillRcodes ##########
      stat_list_filtered <-
        stat_list %>% 
        mutate(Distance = round(sp::spDistsN1(as.matrix(.[c("Long", "Lat")]),
                                              location, longlat = TRUE), 2)) %>% 
        arrange(Distance) %>% 
        filter(Distance <= max_distance) %>% 
        .[c(12,3,4,7,8,10,11,13)] 
      
      # preparation of desired start and end date
      start_date <- as.Date(paste0(start_y,"0101"),format="%Y%m%d")
      end_date <- as.Date(paste0(end_y,"1231"),format="%Y%m%d")
      # using a rowwise manipulation, using Overlap (DescTools) for daily 
      # difference and converting this in years / percentages
      stat_list_filtered <- 
        stat_list_filtered %>%
        rowwise() %>% 
        mutate(Overlap_years = round(((Overlap(c(start_date,end_date),
                                              c(as.Date(as.character(BEGIN),format="%Y%m%d"),as.Date(as.character(END),format="%Y%m%d")))
                                      + 1) / 365.25), 2),
               Perc_interval_covered = round(((Overlap(c(start_date,end_date),
                                                      c(as.Date(as.character(BEGIN),format="%Y%m%d"),as.Date(as.character(END),format="%Y%m%d")))
                                              + 1) / 365.25 / amount_of_years * 100), 0)) %>% 
        ungroup() %>% 
        filter(Perc_interval_covered>=min_overlap) %>% 
        head(stations_to_choose_from)
      
      return(stat_list_filtered)
    }
    
    if (action == "download_weather") { 
      
      if (!dir.exists(path)) dir.create(path)
      start_y <- time_interval[1]
      ifelse(length(time_interval)==2,
             end_y <- time_interval[2],
             end_y <- start_y)
      if (end_at_present==T) end_y <- format(Sys.Date(),"%Y")
      years <- c(start_y:end_y)
      amount_of_years <- length(years)
      
      ###### preparing download-links ##########################################
      raw_link <- "https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/"
      time_link <- rep(raw_link, times = amount_of_years) %>%
        paste0(years,"/")
      subsets_list <- split(rep(time_link, each = length(location)), location)
      
      ###### statlist gets loaded & cleaned (also when action == "list_station")
      ###### list of dataset links gets generated  #############################
      dataset_links <- list()
      for (i in 1:length(location)) {
        dataset_links[i] <- lapply(subsets_list[i], 
                                   function(x){paste0(x,names(subsets_list)[i],
                                                      ".csv")})
        stat_list <- read.csv(paste0(path,"/station_list.csv"))  %>%
          rename(Long = contains("lo"),
                 Lat = contains("la"),
                 Elev = contains("ele")) %>%
          mutate(chillR_code = paste(.$USAF, .$WBAN, sep = "")) %>% 
          filter(!is.na(Lat)|!is.na(Long))
        ###### getting the actual names of the stations instead of the chillRcodes  
        names(dataset_links)[i] <-
          filter(stat_list, chillR_code == names(subsets_list)[i])$STATION.NAME
      }
      
      ###### creating folders for each station #################################
      for (j in 1:length(location)) {
        if (!dir.exists(paste0(path, "/", location[j])))
          dir.create(paste0(path, "/", location[j]))
      }
      
      download <- function(dataset_links){
        for (i in 1:length(location)) {
          if (i==1) if(verbose=="detailed"|verbose=="normal")cat(paste0("Loading data for ", amount_of_years," years from station '", names(dataset_links)[i],"'\n"))
          if (i!=1) if(verbose=="normal")cat(paste0("\nLoading data for ", amount_of_years," years from station '", names(dataset_links)[i],"'\n"))
          if (i!=1) if(verbose=="detailed")cat(paste0("\n\nLoading data for ", amount_of_years," years from station '", names(dataset_links)[i],"'\n"))
          ##### lapply will go through every element (single links) in i-th sub-list of dataset_links and download it if needed
          if(verbose=="normal") pb <- txtProgressBar(min = 0, max = length(dataset_links[[i]]), initial = 0) # setting a progress bar
          lapply(dataset_links[[i]],
                 function(link){
                   if(verbose=="normal") setTxtProgressBar(pb,which(dataset_links[[i]]==link)) # counting the progressbar up
                   station_id <- str_extract(link, "\\d+.csv") %>% str_extract("\\d+")
                   year <- str_extract(link, "\\d+")
                   filenames <-
                     paste0(path,"/", station_id,"/",year,".csv")
                   ##### if you don't want to download every dataset again, this will only load missing datasets
                   ##### link will be empty if file already exist and update all is FALSE
                   if (update_all==F & file.exists(filenames)) link <- ""
                   if(curl::has_internet()){ ######################################################################################
                     if (!link=="") {
                       if (!http_error(link)) {
                         GET(link, write_disk(filenames, overwrite = T))#, progress())
                         if(verbose=='detailed') cat(paste0("\nDataset of year ",year, " loading from GSOD database."))
                       }else if(verbose=='detailed') cat(paste0("\nDataset of year ",year," for this station does not exist in the GSOD database."))
                     }else if(verbose=='detailed') cat(paste0("\nDataset of year ",year, " already present, loading from disk.(",filenames,")"))
                   }
                 }
          )
          invisible()
          if(verbose=="normal")close(pb)
        }
        if(verbose=='detailed') cat("\n\nExplanations:")
        if(verbose=='detailed') cat("\nLoaded in all specified data raw, if available.\n  Use the function on the dataset again (handle_gsod(dataset_name)) to dop columns and reformat to SI units (°C,liter/m²)")
        if(verbose=='detailed') cat("\nIf datasets couldn't be found, they are missing in the GSOD database.\n  Nevertheless, doublecheck if the correct 'location' and 'time_interval' were choosen.")
        if(verbose=='detailed') cat("\nIf station datasets shouldn't be loaded from disk set 'update_all' to 'TRUE'.\n  This updates/downloads all datasets from the GSOD database and takes a lot longer, if you already have the data partially on disk.")
      }
      
      download(dataset_links)

      multimerge <- function(){
        subfolders <- list.dirs(path = path, full.names = TRUE, recursive = FALSE) %>%
          .[str_extract(., "\\d+") %in% location]
        all_data <- list()
        all_data <- lapply(subfolders, function(x) {
          csv_files <- list.files(path = x, pattern = paste0(years,collapse = "|"), full.names = TRUE)
          if (length(csv_files) == 0){
            # cat(paste0("No data for station '", filter(stat_list, chillR_code == str_extract(x,"\\d+"))$STATION.NAME ,"'\nCheck the stationlist for the correct location input or a valid date range for this station.\n"))
            csv_data <-
              data.frame(
                STATION=NA,DATE=NA,LATITUDE=NA,LONGITUDE=NA,ELEVATION=NA,NAME=NA,
                TEMP=NA,TEMP_ATTRIBUTES=NA,DEWP=NA,DEWP_ATTRIBUTES=NA,SLP=NA,
                SLP_ATTRIBUTES=NA,STP=NA,STP_ATTRIBUTES=NA,VISIB=NA,
                VISIB_ATTRIBUTES=NA,WDSP=NA,WDSP_ATTRIBUTES=NA,MXSPD=NA,GUST=NA,
                MAX=NA,MAX_ATTRIBUTES=NA,MIN=NA,MIN_ATTRIBUTES=NA,PRCP=NA,
                PRCP_ATTRIBUTES=NA,SNDP=NA,FRSHTT=NA)
          }else{
            csv_data <- suppressWarnings(lapply(csv_files, read.csv))
            for (i in 1:length(csv_data)) {
              if (ncol(csv_data[[i]])==1) {
                csv_data[[i]] <-
                  data.frame(
                    STATION=NA,DATE=NA,LATITUDE=NA,LONGITUDE=NA,ELEVATION=NA,NAME=NA,
                    TEMP=NA,TEMP_ATTRIBUTES=NA,DEWP=NA,DEWP_ATTRIBUTES=NA,SLP=NA,
                    SLP_ATTRIBUTES=NA,STP=NA,STP_ATTRIBUTES=NA,VISIB=NA,
                    VISIB_ATTRIBUTES=NA,WDSP=NA,WDSP_ATTRIBUTES=NA,MXSPD=NA,GUST=NA,
                    MAX=NA,MAX_ATTRIBUTES=NA,MIN=NA,MIN_ATTRIBUTES=NA,PRCP=NA,
                    PRCP_ATTRIBUTES=NA,SNDP=NA,FRSHTT=NA)
              }
            }
          }
          subfolder_df <- do.call(rbind,csv_data)
          return(subfolder_df)
        })
        return(all_data)
      }
      
      all_climate_data <- multimerge()
      names(all_climate_data) <- names(dataset_links)
      return(all_climate_data)
      stoerr <- warnings()
    }
    
    if (action == "delete") {
      
      if (clean_up == "all"){
        cat(paste0("Are you sure you want to delete '",getwd(),"/",path,"'?",
                   "\nThis path contains ",length(list.dirs(path))-1," sub-folders, holding ",length(list.files("climate_data",recursive = T))-1," files in total.\n"))
        user_input <- readline("Press 'y' to confirm deletion (y/n).")
        if(user_input != 'y') stop(call. = F,'Exiting since you did not press y.')
        if (dir.exists(path)) {
          a <- unlink(path,recursive = T)
          unlink(path,recursive = T)
          if(a == 0) cat(paste0("Removal of '",getwd(),"/",path,"' complete."))
        }else cat(paste0("\nPath '",getwd(),"/",path,"' does not exist."))
      }
      
      if (clean_up == "station") {
        if (!exists("location")) cat("You need to specify the stations you want to delete in 'location'.")
        cat(paste0("Are you sure you want to delete the data for station(s) '",paste0(location,collapse = "', '"),"'?",
                   "\nPath(s) contain(s) ",length(list.dirs(paste0(path,"/",location,"/")))-length(location)," sub-folders, holding ",length(list.files(paste0(path,"/",location,"/")))," files in total.\n"))
        user_input <- readline("Press 'y' to confirm deletion (y/n).")
        if(user_input != 'y') stop(call. = F,'Exiting since you did not press y.')
        dirs <- paste0(path,"/",location,"/")
        if (all(dir.exists(dirs))) {
          a <- unlink(dirs,recursive = T)
          unlink(dirs,recursive = T)
          if(a == 0) cat(paste0("Removal of '",getwd(),"/",path,"' complete."))
        }else cat(paste0("\nPath(s)"),
                  paste0("\n'./",dirs[dir.exists(dirs)],"/'",collapse = ""),
                  "\nnot existing.")
      }
    }
  }
  
  if (is.list(action) | is.data.frame(action)){
    
    if (class(action) != "list") {
      time_interval <- 
        c(min(unique(format(as.Date(action$DATE), "%Y"))),
          max(unique(format(as.Date(action$DATE), "%Y"))))
      start <- as.Date(paste0(time_interval[1],"-01-01"))
      end <- as.Date(paste0(time_interval[2],"-12-31"))
      
      action$DATE <- as.Date(action$DATE)
      if (add.Date == T){
        temp_data_incomplete <- pad(action,
                                    interval = "day",
                                    start_val = start,
                                    end_val = end)
      }else temp_data_incomplete <- action
    }else {
      temp_data_incomplete <- list()
      for (i in 1:length(action)) {
        if (is.list(action[[i]])) {
          time_interval <- 
            c(min(unique(format(as.Date(action[[i]]$DATE), "%Y"))),
              max(unique(format(as.Date(action[[i]]$DATE), "%Y"))))
          start <- as.Date(paste0(time_interval[1],"-01-01"))
          end <- as.Date(paste0(time_interval[2],"-12-31"))
          
          action[[i]]$DATE <- as.Date(action[[i]]$DATE)
          if (add.Date == T){
            temp_data_incomplete[[i]] <- pad(action[[i]],
                                             interval = "day",
                                             start_val = start,
                                             end_val = end)
          }else temp_data_incomplete[[i]] <- action[[i]]
          
        }else temp_data_incomplete[[i]] <- action[[i]]
      }
    }
    
    clean_up <- function(data) {
      if (nrow(as.data.frame(data)[1]) == 28) {
        if (drop_most == T) {
          temp_data_complete <-
            data.frame(Date = NA,Year = NA,Month = NA,Day = NA,
                       Tmin = NA,Tmax = NA,Tmean = NA,Prec = NA)
        }else temp_data_complete <- temp_data_incomplete
      }else{
        if (drop_most == T) {
          temp_data_complete <-
            data.frame(
              Date = data$DATE,
              Year = format(data$DATE, "%Y"),
              Month = format(data$DATE, "%m"),
              Day = format(data$DATE, "%d"),
              Tmin = ifelse(data$MIN == 9999.9,NA,round( ((data$MIN - 32)*5/9), 3)), # fahrenheit to celsius
              Tmax = ifelse(data$MAX == 9999.9,NA,round( ((data$MAX - 32)*5/9), 3)), # fahrenheit to celsius
              Tmean = ifelse(data$TEMP == 9999.9,NA,round( ((data$TEMP - 32)*5/9), 3)), # fahrenheit to celsius
              Prec = ifelse(data$PRCP == 99.99,NA,round( (data$PRCP*25.4) ,3))  # inch to millimeter
            )
        }else temp_data_complete <- data.frame(
          STATION = data$STATION,
          DATE = as.Date(data$DATE),
          LATITUDE = data$LATITUDE,
          LONGITUDE = data$LONGITUDE,
          ELEVATION = data$ELEVATION,
          NAME = data$NAME,
          TEMP = ifelse(data$TEMP == 9999.9,NA,round(((data$TEMP - 32)*5/9),3)), # fahrenheit to celsius
          TEMP_ATTRIBUTES = data$TEMP_ATTRIBUTES,
          DEWP = ifelse(data$DEWP == 9999.9,NA,round(((data$DEWP - 32)*5/9),3)), # fahrenheit to celsius
          DEWP_ATTRIBUTES = data$DEWP_ATTRIBUTES,
          SLP = ifelse(data$SLP == 9999.9,NA,data$SLP), # millibar (= hPa; hektoPascal)
          SLP_ATTRIBUTES = data$SLP_ATTRIBUTES,
          STP = ifelse(data$STP == 9999.9,NA,data$STP), # millibar (= hPa; hektoPascal)
          STP_ATTRIBUTES = data$STP_ATTRIBUTES,
          VISIB = ifelse(data$VISIB == 999.9,NA,round(data$VISIB*1.609344,3)), # Miles to kilometer
          VISIB_ATTRIBUTES = data$VISIB_ATTRIBUTES,
          WDSP = ifelse(data$WDSP == 999.9,NA,round(data$WDSP/0.53996,3)), # knots to km/h
          WDSP_ATTRIBUTES = data$WDSP_ATTRIBUTES,
          MXSPD = ifelse(data$MXSPD == 999.9,NA,round(data$MXSPD/0.53996,3)), # knots to km/h
          GUST = ifelse(data$GUST == 999.9,NA,round(data$GUST/0.53996,3)),
          MAX = ifelse(data$MAX == 9999.9,NA,round((data$Max-32)*5/9,3)), # fahrenheit to celsius
          MAX_ATTRIBUTES = data$MAX_ATTRIBUTES,
          MIN = ifelse(data$MIN == 9999.9,NA,round(((data$MIN-32)*5/9),3)), # fahrenheit to celsius
          MIN_ATTRIBUTES = data$MIN_ATTRIBUTES,
          PRCP = ifelse(data$PRCP == 99.99,NA,round((data$PRCP*25.4),3)), # inch to millimeter
          PRCP_ATTRIBUTES = data$PRCP_ATTRIBUTES,
          SNDP = ifelse(data$SNDP == 99.99,NA,round((data$SNDP*25.4),3)), # inch to millimeter
          FRSHTT = data$FRSHTT
        )
      }
      temp_data_complete
    }
    
    if (class(action) != "list") {
      temp_data_complete <- clean_up(temp_data_incomplete)
    }else{
      temp_data_complete <- lapply(temp_data_incomplete, clean_up)
      names(temp_data_complete) <- names(action)
    }
    temp_data_complete
  }
  
}

# 
# ### original handle_gsod()
# library(chillR)
# 
# ## stationlist download
# 
# # Bonn
# lat <- 50.7341602
# long <- 7.0871843
# 
# stationlist_old <-
#   handle_gsod(action = "list_stations",
#               time_interval = c(1995,2000),
#               location = c(lat,long))
# 
# ## data download
# 
# test_data_old <-
#   handle_gsod(action = "download_weather",
#               time_interval = c(1995,2000),
#               location = stationlist_old$chillR_code[c(1,2)])
# 




### new function handle_gsod_new() - name pending
## stationlist download variations

# Bonn
lat <- 50.7341602
long <- 7.0871843

stationlist <-
  handle_gsod_new(action = "list_stations",
                  time_interval = c(1995,2000),
                  location = c(long,lat)
                  # ,
                  # path = "climate_data",            # standard option, can be set to something different
                  # update_station_list = F,          # standard option, tries to load from disk, if not possible it downloads - can be set to TRUE so it always downloads
                  # max_distance = 150,               # standard option, can be adjusted
                  # min_overlap = 0,                  # standard option, percentage from 0 - 100, might obscure relevant locations if to high 
                  # stations_to_choose_from = 25      # standard option, can be adjusted
                  )

## data download variations - downloads the raw files and binds them together

test_data <-
  handle_gsod_new(action = "download_weather",
                  time_interval = c(1995,2000),
                  location = stationlist$chillR_code[c(1,2)]
                  # ,
                  # end_at_present = F,             # standard option, can be set to true if time_interval has only one value
                  # path = "climate_data",          # standard option, can be set to something different
                  # verbose = "normal",             # standard option, minimal information 
                  # verbose = "detailed",           #                  detailed information
                  # verbose = "quiet",              #                  no information - does not need to be 'quiet', but different from 'normal' or 'detailed'
                  # update_all = F,                 # standard option, can be set to TRUE, if data should be downloaded again (updated)
                  )

## chillR formating

test_data_clean_minimal <- handle_gsod_new(test_data,
                                           # add.Date = T     # standard option, can be set to FALSE, but not advised if you want datasets without gaps
                                           )

test_data_clean <- handle_gsod_new(test_data,
                                   drop_most = F,             # standard option, can be set to false to keep columns and only convert to SI units
                                   # add.Date = T             # standard option, can be set to FALSE, but not advised if you want datasets without gaps
                                   )





## data deletion on disk for clean_up

# functions will ask for confirmation in th console - 'y' for yes to confirm deletion, anything else cancels the deletion

handle_gsod_new(action = "delete",
                clean_up = "all")

handle_gsod_new(action = "delete",
                clean_up = "station",
                location = stationlist$chillR_code[c(1,2)])









# ##### microbenchmark ###########################################################
# 
# library(microbenchmark)
# 
# # downloading stationlist 3 times
# mb <- microbenchmark(
#   "stationlist_old" = handle_gsod(action = "list_stations",
#                                   time_interval = c(1990,2000),
#                                   location = c(lat,long)),
#   "stationlist" = handle_gsod_new(action = "list_stations",
#                                location = c(long,lat),
#                                time_interval = c(1900,2000),
#                                update_station_list = T),
#   times = 3
# )
# mb
# 
# # downloading 5 years for 2 stations 3 times
# mb2 <- microbenchmark(
#   "data_old" =  handle_gsod(action = "download_weather",
#                             time_interval = c(1995,2000),
#                             location = stationlist_old$chillR_code[c(1,2)]),
#   "data" =  handle_gsod_new(action = "download_weather",
#                          time_interval = c(1995,2000),
#                          location = stationlist$chillR_code[c(1,2)],
#                          update_all = T),
#   times = 3
# )
# mb2
# 
# 
# # microbenchmark results
# > mb
# Unit: seconds
# expr            min       lq        mean     median    uq        max           neval
# stationlist_old 14.759117 15.074490 15.27142 15.389863 15.527575 15.665286     3
# stationlist     3.089891  3.379655  3.69647  3.669419  3.999759  4.330099      3
# > mb2
# Unit: seconds
# expr            min       lq        mean      median    uq        max           neval
# data_old        108.54369 110.08141 129.54036 111.61913 140.03870 168.45827     3
# data            10.13793  10.29189  10.52595  10.44584  10.71996  10.99408      3
# 
