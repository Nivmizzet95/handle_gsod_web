#' @title Download climate data from the Global Summary of the Day database (faster)
#'
#' @description
#' This function's main features are downloading and converting climate data from
#'    the Global Summary of the Day database (GSOD) from the National Climatic
#'    Data Centre (NCDC) of the National Oceanic and Atmospheric Administration
#'    (NOAA).
#'
#'    The standard workflow for this is:
#'
#'    1. Getting the list of stations that are close to a specified position
#'       using the `action = "list_stations"` in combination with the
#'       `location = c(longitude,latitude)` parameter. The parameter
#'       `time_interval = c(start_year,end_year)` can be used to see the
#'       coverage of each station.
#'    2. Downloading raw climate data from suitable stations by setting the
#'       parameter `action = "download_weather"` and selecting the stations
#'       `chillR_code` from the stationlist with
#'       `location = stationlist$chillR_code[n]`. In addition the parameter
#'       `time_interval` needs to be set to a desired timeinterval as in the
#'       previous step.
#'    3. Converting the raw data into a suitable `chillR` format by setting
#'       the `action` parameter equal to the raw data set.(`action = raw_data`)
#'
#'    There are many other parameters that can be set - see the Arguments
#'    section for more information.
#'
#' @param action Can be set to:
#'
#'    1. "`list_stations`" to download a list of stations in proximity of a
#'       location.
#'    2. "`download_weather`" to download climate data for a specific time
#'       interval from one or more stations listed in the downloaded stations
#'       list.
#'    3. "`delete`" to clean up the path used for downloading. Can be used with
#'       `clean_up = "all"` to delete the entire path, or with
#'       `clean_up = "station"` in combination with the parameter `location` set
#'        to the station `chillR_code` to be deleted, as used in
#'        `action = "download_weather"`.
#'    4. `raw_data_set`to convert the data into `chillR` format. This converts
#'       the units into SI units. For the raw data documentation visit
#'       https://www.ncei.noaa.gov/data/global-summary-of-the-day/doc/readme.txt
#' @param location Needs to be set to either a geographic location
#'    (`c(longitude,latitude)`) in conjunction with `action = "list_stations"`
#'    or `chillR_code(s)` extracted from the station list.
#' @param time_interval Normally a numeric vector with two elements, specifying
#'    the start and end date of the period of interest. Only required when
#'    running `action = list_stations` or `action = download_weather`.
#'    Can also be a numeric vector with only one element, if used with
#'    `end_at_present = TRUE`.
#' @param path Default value is "climate_data". Here the path for the downloads
#'    can be set manually if desired. The function will then concatenate this
#'    string with the current working directory path. Use `getwd()` and
#'    `setwd()` if you are not sure what your current working directory is.
#' @param end_at_present Default is `FALSE`. Can be set to `TRUE`, if only one
#'    element is provided in `time_interval`. In this case the second value will
#'    be exacted from the system time.
#' @param stations_to_choose_from Default is 25. This is limiting the amount of
#'    stations outputted while using `action = list_stations`.The
#'    `max_distance` parameter might be too low, if the output does contain less
#'    then 25 stations.
#' @param max_distance Default is 150 kilometer and freely adjustable. This is
#'    limiting the amount of stations outputted while using
#'    `action = list_stations`.
#' @param min_overlap Default is 0. This is used to create the `Overlap_years`
#'    and `Perc_interval_covered` columns in the station list file. Can be set
#'    higher to filter for stations that cover a certain amount of the desired
#'    `time_interval`. Setting this too high might result in a lot of available
#'    stations to be lost.
#' @param drop_most Default is `TRUE`. This is used in combination with
#'    `action = raw_data_set` to convert the raw downloaded data into `chillR`
#'    format. Can be set to `FALSE` to keep the overall structure of the raw
#'    data and only convert into SI units.
#' @param update_all Default is `FALSE`. This is used to speed up the function
#'    run time by only downloading data that is missing in the `path`
#'    directories. Can be set to `TRUE` if all the data is supposed to be
#'    re-downloaded.
#' @param update_station_list Default is `FALSE`. Similar behavior as
#'    `update_all`. The station list changes much less than the downloaded data,
#'    but it is still advised to update it from time to time.
#' @param add.Date Default is `TRUE`. This is used in combination with
#'    `action = raw_data_set` and will add missing dates to the `chillR`
#'    formated data set. These dates will have `NAs` in the data columns.
#' @param clean_up This is used in Combination with `action = delete`. Can be
#'    either `clean_up = "all"` to delete the entire download path set in
#'    `path`, or `clean_up = "station"` to delete specific station folders.
#'    `clean_up = "station"` needs to be used in combination with the parameter
#'    `location` set to the station(s) `chillR_codes`, as used in
#'    `action = "download_weather"`.
#' @param verbose Default is "`normal`" and can also be set to "`detailed`" or
#'    "`quiet`". Set to "`normal`" it will still give download progress updates, but
#'    keep the console comments to a minimum. setting it to "`detailed`" can
#'    helpful for debugging problems. "`quiet`" will give no console updates.
#'
#' @details
#' This function is an alternative to the original `handle_gsod()` function from
#'    the `chillR` package and strives to run faster and have some additional
#'    tweaks. The increased speed stems mainly from saving the data to the disk
#'    and also loading it from there if already present. It will only
#'    re-download ALL the data if the parameter `update_all` is set to `TRUE`.
#'    One major difference between this function and the original is that
#'    the downloaded data is initially kept in its raw form (as found in the
#'    GSOD database). This allows for own manipulation of the data, but keep in
#'    mind that the units are not yet converted to SI. Setting the `action`
#'    parameter to the raw data set and also setting `drop_most` to `FALSE`
#'    will keep the raw data structure but convert everything into SI units.
#'    Setting `drop_most` to `TRUE` will result in the `chillR` format.
#'
#'    The units are converted as following:
#'
#'    *  TEMP   - Mean temperature             (.1 Fahrenheit into Celsius)
#'    *  DEWP   - Mean dew point               (.1 Fahrenheit into Celsius)
#'    *  SLP    - Mean sea level pressure      (.1 mb kept as millibar or hektoPascal)
#'    *  STP    - Mean station pressure        (.1 mb kept as millibar or hektoPascal)
#'    *  VISIB  - Mean visibility              (.1 miles into kilometer)
#'    *  WDSP   - Mean wind speed              (.1 knots into meter per second)
#'    *  MXSPD  - Maximum sustained wind speed (.1 knots into meter per second)
#'    *  GUST   - Maximum wind gust            (.1 knots into meter per second)
#'    *  MAX    - Maximum temperature          (.1 Fahrenheit into Celsius)
#'    *  MIN    - Minimum temperature          (.1 Fahrenheit into Celsius)
#'    *  PRCP   - Precipitation amount         (.01 inches into millimeter)
#'    *  SNDP   - Snow depth                   (.1 inches into millimeter)
#'    *  FRSHTT - Indicator for occurrence of:
#'
#'        * Fog, Rain or Drizzle, Snow or Ice Pellets
#'        * Hail, Thunder, Tornado/Funnel Cloud
#'
#' @note
#' The GSOD database is described here: <https://www.ncei.noaa.gov/access/metadata/landing-page/bin/iso?id=gov.noaa.ncdc:C00516#Documentation>
#'
#'    Many databases have data quality flags, which may sometimes indicate that
#'    data isn't reliable. These are not considered by this function, but the
#'    relevant columns can still be found in the `raw data`. Quality control
#'    columns include "Attribute", please refer to the GSOD documentation above
#'    for a more detailed explanation.
#'
#' @author Adrian Fuelle, \email{afuelle@@outlook.de};
#'    Eike Luedeling, \email{eike@@eikeluedeling.com}
#' @importFrom magrittr %>%
#' @importFrom dplyr filter rename mutate arrange rowwise ungroup
#' @importFrom curl has_internet
#' @importFrom httr GET write_disk http_error
#' @importFrom DescTools Overlap
#' @importFrom padr pad
#' @importFrom stringr str_extract str_remove
#' @importFrom utils txtProgressBar
#' @export
#'
#' @examples
#' \dontrun{
#'
#' ## stationlist download
#' # Bonn
#' lat <- 50.7341602
#' long <- 7.0871843
#' stationlist <-
#'   handle_gsod_web(action = "list_stations",
#'                   time_interval = c(1995,2000),
#'                   location = c(long,lat) # ,
#'                   # path = "climate_data",              # standard option, can be set to something different
#'                   # update_station_list = F,            # standard option, tries to load from disk, if not possible it downloads - can be set to TRUE so it always downloads
#'                   # max_distance = 150,                 # standard option, can be adjusted
#'                   # min_overlap = 0,                    # standard option, percentage from 0 - 100, might obscure relevant locations if to high
#'                   # stations_to_choose_from = 25        # standard option, can be adjusted
#'                   )
#'
#'
#' ## data download
#' test_data <-
#'   handle_gsod_web(action = "download_weather",
#'                   time_interval = c(1995,2000),
#'                   location = stationlist$chillR_code[c(1,2)]#,
#'                   # end_at_present = F,                 # standard option, can be set to true if time_interval has only one value
#'                   # path = "climate_data",              # standard option, can be set to something different
#'                   # verbose = "normal",                 # standard option, minimal information
#'                   # verbose = "detailed",               #                  detailed information
#'                   # verbose = "quiet",                  #                  no information - does not need to be 'quiet', but different from 'normal' or 'detailed'
#'                   # update_all = F,                     # standard option, can be set to TRUE, if data should be downloaded again (updated)
#'                   )
#'
#'
#' ## chillR formating
#' test_data_clean <- handle_gsod_web(test_data,
#'                                    # drop_most = T,     # standard option, can be set to false to keep all columns and only convert to SI units
#'                                    # add.Date = T       # standard option, can be set to FALSE, but not advised if you want datasets without gaps
#'                                    )
#'
#'
#' ## data deletion on disk
#' # functions will ask for confirmation in th console - 'y' for yes to confirm deletion, anything else cancels the deletion
#' handle_gsod_web(action = "delete",
#'                 clean_up = "all")
#'
#' handle_gsod_web(action = "delete",
#'                 clean_up = "station",
#'                 location = stationlist$chillR_code[c(1,2)])
#' }
#'
handle_gsod_web <- function(action,
                            location = NULL,
                            time_interval = NULL,
                            path = "climate_data",
                            end_at_present = FALSE,
                            stations_to_choose_from = 25,
                            max_distance = 150,
                            min_overlap = 0,
                            drop_most = TRUE,
                            update_all = FALSE,
                            update_station_list = FALSE,
                            add.Date = TRUE,
                            clean_up = NULL,
                            verbose = "normal"){

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
      if (!file.exists(paste0(path,"/station_list.csv"))|update_station_list==T) {
        if(curl::has_internet()){
          httr::GET("https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv",
                    httr::write_disk(paste0(path,"/station_list.csv"), overwrite = T)) %>%
            invisible()
          if(verbose=="detailed")cat("Downloaded newest version of the stationlist.\n  'update_station_list' can be set to 'FALSE'.")
        }else if(verbose=="detailed"|verbose=="normal") cat("No internet connection detected.")
      }else if(verbose=="detailed")cat("Stationlist was loaded from disk.\n  Set 'update_station_list' to 'TRUE' to update it (~3MB).")
      stat_list <-
        read.csv(paste0(path,"/station_list.csv")) %>%
        dplyr::rename(Long = contains("lo"),
                      Lat = contains("la"),
                      Elev = contains("ele")) %>%
        dplyr::mutate(chillR_code = paste(.$USAF, .$WBAN, sep = "")) %>%
        dplyr::filter(!is.na(Lat)|!is.na(Long)) %>%
        dplyr::filter(Lat != 0,Long != 0)
      stat_list_filtered <-
        stat_list %>%
        dplyr::mutate(Distance = round(sp::spDistsN1(as.matrix(.[c("Long", "Lat")]),
                                                     location, longlat = TRUE), 2)) %>%
        dplyr::arrange(Distance) %>%
        dplyr::filter(Distance <= max_distance) %>%
        .[c(12,3,4,7,8,10,11,13)]
      start_date <- as.Date(paste0(start_y,"0101"),format="%Y%m%d")
      end_date <- as.Date(paste0(end_y,"1231"),format="%Y%m%d")
      stat_list_filtered <-
        stat_list_filtered %>%
        dplyr::rowwise() %>%
        dplyr::mutate(Overlap_years = round(((DescTools::Overlap(c(start_date,end_date),
                                                                 c(as.Date(as.character(BEGIN),format="%Y%m%d"),
                                                                   as.Date(as.character(END),format="%Y%m%d")))+1)/365.25),2),
                      Perc_interval_covered = round(((DescTools::Overlap(c(start_date,end_date),
                                                                         c(as.Date(as.character(BEGIN),format="%Y%m%d"),
                                                                           as.Date(as.character(END),format="%Y%m%d")))+1)/365.25/amount_of_years*100),0)) %>%
        dplyr::ungroup() %>%
        dplyr::filter(Perc_interval_covered>=min_overlap) %>%
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
      raw_link <- "https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/"
      time_link <- rep(raw_link, times = amount_of_years) %>%
        paste0(years,"/")
      location <- stringr::str_remove(location, "_")
      subsets_list <- split(rep(time_link, each = length(location)), location)
      dataset_links <- list()
      for (i in 1:length(location)) {
        dataset_links[i] <- lapply(subsets_list[i],
                                   function(x){paste0(x,names(subsets_list)[i],
                                                      ".csv")})
        stat_list <- read.csv(paste0(path,"/station_list.csv"))  %>%
          dplyr::rename(Long = contains("lo"),
                        Lat = contains("la"),
                        Elev = contains("ele")) %>%
          dplyr::mutate(chillR_code = paste(.$USAF, .$WBAN, sep = "")) %>%
          dplyr::filter(!is.na(Lat)|!is.na(Long))
        names(dataset_links)[i] <-
          dplyr::filter(stat_list, chillR_code == names(subsets_list)[i])$STATION.NAME
      }
      for (j in 1:length(location)) {
        if (!dir.exists(paste0(path, "/", location[j])))
          dir.create(paste0(path, "/", location[j]))
      }
      download <- function(dataset_links){
        for (i in 1:length(location)) {
          if (i==1) if(verbose=="detailed"|verbose=="normal")cat(paste0("Loading data for ", amount_of_years," years from station '", names(dataset_links)[i],"'\n"))
          if (i!=1) if(verbose=="normal")cat(paste0("\nLoading data for ", amount_of_years," years from station '", names(dataset_links)[i],"'\n"))
          if (i!=1) if(verbose=="detailed")cat(paste0("\n\nLoading data for ", amount_of_years," years from station '", names(dataset_links)[i],"'\n"))
          if(verbose=="normal") pb <- utils::txtProgressBar(min = 0, max = length(dataset_links[[i]]), initial = 0)
          lapply(dataset_links[[i]],
                 function(link){
                   if(verbose=="normal") utils::setTxtProgressBar(pb,which(dataset_links[[i]]==link))
                   station_id <- stringr::str_extract(link, "\\d+.csv") %>% stringr::str_extract("\\d+")
                   year <- stringr::str_extract(link, "\\d+")
                   filenames <-
                     paste0(path,"/", station_id,"/",year,".csv")
                   if (update_all==F & file.exists(filenames)) link <- ""
                   if(curl::has_internet()){
                     if (!link=="") {
                       if (!httr::http_error(link)) {
                         httr::GET(link, httr::write_disk(filenames, overwrite = T))
                         if(verbose=='detailed') cat(paste0("\nDataset of year ",year, " loading from GSOD database."))
                       }else if(verbose=='detailed') cat(paste0("\nDataset of year ",year," for this station does not exist in the GSOD database."))
                     }else if(verbose=='detailed') cat(paste0("\nDataset of year ",year, " already present, loading from disk.(",filenames,")"))
                   }else if(verbose=='detailed'|verbose=="normal") cat("\nNo internet connection found.")
                 }
          )
          invisible()
          if(verbose=="normal")close(pb)
        }
        if(verbose=='detailed') cat("\n\nExplanations:")
        if(verbose=='detailed') cat("\nLoaded in all specified data raw, if available.\n  Use the function on the dataset again (handle_gsod_web(dataset_name)) to drop columns and reformat to SI units (degree Celsius, meter per second ...)")
        if(verbose=='detailed') cat("\nIf datasets couldn't be found, they are missing in the GSOD database.\n  Nevertheless, doublecheck if the correct 'location' and 'time_interval' were choosen.")
        if(verbose=='detailed') cat("\nIf station datasets shouldn't be loaded from disk set 'update_all' to 'TRUE'.\n  This updates/downloads all datasets from the GSOD database and takes a lot longer, if you already have the data partially on disk.")
      }
      download(dataset_links)
      multimerge <- function(){
        subfolders <- list.dirs(path = path, full.names = TRUE, recursive = FALSE) %>%
          .[stringr::str_extract(., "\\d+") %in% location]
        all_data <- list()
        all_data <- lapply(subfolders, function(x) {
          csv_files <- list.files(path = x, pattern = paste0(years,collapse = "|"), full.names = TRUE)
          if (length(csv_files) == 0){
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
    }
    if (action == "delete") {
      location <- stringr::str_remove(location, "_")
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
        temp_data_incomplete <- padr::pad(action,
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
            temp_data_incomplete[[i]] <- padr::pad(action[[i]],
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
              Tmin = ifelse(data$MIN == 9999.9,NA,round( ((data$MIN - 32)*5/9), 3)),
              Tmax = ifelse(data$MAX == 9999.9,NA,round( ((data$MAX - 32)*5/9), 3)),
              Tmean = ifelse(data$TEMP == 9999.9,NA,round( ((data$TEMP - 32)*5/9), 3)),
              Prec = ifelse(data$PRCP == 99.99,NA,round( (data$PRCP*25.4) ,3))
            )
        }else temp_data_complete <- data.frame(
          STATION = data$STATION,
          DATE = as.Date(data$DATE),
          LATITUDE = data$LATITUDE,
          LONGITUDE = data$LONGITUDE,
          ELEVATION = data$ELEVATION,
          NAME = data$NAME,
          TEMP = ifelse(data$TEMP == 9999.9,NA,round(((data$TEMP - 32)*5/9),3)),
          TEMP_ATTRIBUTES = data$TEMP_ATTRIBUTES,
          DEWP = ifelse(data$DEWP == 9999.9,NA,round(((data$DEWP - 32)*5/9),3)),
          DEWP_ATTRIBUTES = data$DEWP_ATTRIBUTES,
          SLP = ifelse(data$SLP == 9999.9,NA,data$SLP),
          SLP_ATTRIBUTES = data$SLP_ATTRIBUTES,
          STP = ifelse(data$STP == 9999.9,NA,data$STP),
          STP_ATTRIBUTES = data$STP_ATTRIBUTES,
          VISIB = ifelse(data$VISIB == 999.9,NA,round((data$VISIB*1.609344)*1000,3)),
          VISIB_ATTRIBUTES = data$VISIB_ATTRIBUTES,
          WDSP = ifelse(data$WDSP == 999.9,NA,round(data$WDSP/1.9438,3)),
          WDSP_ATTRIBUTES = data$WDSP_ATTRIBUTES,
          MXSPD = ifelse(data$MXSPD == 999.9,NA,round(data$MXSPD/1.9438,3)),
          GUST = ifelse(data$GUST == 999.9,NA,round(data$GUST/1.9438,3)),
          MAX = ifelse(data$MAX == 9999.9,NA,round((data$Max-32)*5/9,3)),
          MAX_ATTRIBUTES = data$MAX_ATTRIBUTES,
          MIN = ifelse(data$MIN == 9999.9,NA,round(((data$MIN-32)*5/9),3)),
          MIN_ATTRIBUTES = data$MIN_ATTRIBUTES,
          PRCP = ifelse(data$PRCP == 99.99,NA,round((data$PRCP*25.4),3)),
          PRCP_ATTRIBUTES = data$PRCP_ATTRIBUTES,
          SNDP = ifelse(data$SNDP == 99.99,NA,round((data$SNDP*25.4),3)),
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
