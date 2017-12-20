###########################################################################################
######################################FUNCTIONS############################################
###########################################################################################

#' Get Merra Files from Server
#'
#' @description Function to download a single MERRA file with authentication from gesdisc. This is a helper function to make sParApply work
#' @param i index in list of filenames
#' @param fileNameIn list of filenames to download
#' @param fileNameOut list of filenames to save results to
#' @param user username for geo disc server
#' @param pass password for geo disc server
getFileMerraFinal<-function(i,fileNameIn,fileNameOut,user,pass){
  print(fileNameIn[i])
  print(fileNameOut[i])
  if(!file.exists(fileNameOut[i])){
    print(GET(fileNameIn[i],
              authenticate(user,pass),
              write_disk(fileNameOut[i],overwrite=TRUE)))
  }else{
    print("File exists already")
  }

}

#' Get MERRA Files for a given spatial box
#' @description Function to download data from the merra2 dataset
#' @param targetPath Path to save MERRA ncdf files to
#' @param lon1 Box coordinate
#' @param lat1 Box coordinate
#' @param lon2 Box coordinate
#' @param lat2 Box coordinate
#' @param period A vector with all dates to download (daily entries in the vector).
#' @param params The respective parameter to be downloaded
#' @param user,pass username and password for the downloadserver get one at: https://urs.earthdata.nasa.gov/home
#' @param runParallel if true, download uses available cores to parallelize downloads
#' @param outputOfParallelProc If different from "", output files are written to the specified directory
#' @return Does not return anything, but writes, as a side-effect, the downloaded files to disk.
getMERRADataBox<-function(targetPath="",lon1,lat1,lon2,lat2,period,params,user,pass,runParallel=TRUE,outputofParallelProc=""){

  targetPath<-addSlash(targetPath)

  dir.create(targetPath, recursive=TRUE,showWarnings = FALSE)


  dates<-period %>% format(format="%Y%m%d")
  year<-period %>% format(format="%Y") %>% as.numeric()
  month<-period %>% format(format="%m")

  variables<-sapply(params,paste,"%2C",sep="") %>%
    paste(collapse='') %>%
    substr(1,nchar(.)-3)

  boxes<-c(lat1,lon1,lat2,lon2) %>%
    sapply(paste,"%2C",sep="") %>%
    paste(collapse='') %>%
    substr(1,nchar(.)-3)

  fileNamesIn<-rep("",length(period))
  fileNamesOut<-rep("",length(period))

  print(paste("downloading:",lon1,lat1,lon2,lat2,params))

  for(i in 1:length(period)){


    fileName<-paste(targetPath,
                    "MERRA_",lon1,"_",lat1,"_",lon2,"_",lat2,"_",variables,"_",dates[i],".nc4",sep="")

    fileNamesOut[i]<-fileName


    fV<-100
    if(between(year[i],1992,2000)){
      fV<-200
    }
    if(between(year[i],2001,2010)){
      fV<-300
    }
    if(between(year[i],2011,2020)){
      fV<-400
    }


    if(params!="swgdn"){
    url<-paste("http://goldsmr4.gesdisc.eosdis.nasa.gov/daac-bin/OTF/HTTP_services.cgi?FILENAME=%2Fdata%2FMERRA2%2FM2I1NXASM.5.12.4%2F",
               year[i],"%2F",
               month[i],"%2FMERRA2_",fV,".inst1_2d_asm_Nx.",
               dates[i],".nc4&FORMAT=bmM0Lw&BBOX=",boxes,
               "&LABEL=MERRA2_",fV,".inst1_2d_asm_Nx.",dates[i],
               ".SUB.nc4&SHORTNAME=M2I1NXASM&SERVICE=SUBSET_MERRA2&VERSION=1.02&LAYERS=&VARIABLES=",variables,
               sep="")
    }else{

    url<-paste("http://goldsmr4.gesdisc.eosdis.nasa.gov/daac-bin/OTF/HTTP_services.cgi?FILENAME=%2Fdata%2FMERRA2%2FM2T1NXLFO.5.12.4%2F",
               year[i],"%2F",
               month[i],"%2FMERRA2_",fV,".tavg1_2d_lfo_Nx.",
               dates[i],".nc4&FORMAT=bmM0Lw&BBOX=",boxes,
               "&LABEL=MERRA2_",fV,".tavg1_2d_lfo_Nx.",dates[i],
               ".SUB.nc4&SHORTNAME=M2T1NXLFO&SERVICE=SUBSET_MERRA2&VERSION=1.02&LAYERS=&VARIABLES=",variables,
               sep="")

    print(url)

    }


    fileNamesIn[i]<-url

  }
  if(runParallel){

    no_cores <-8

    # Initiate cluster
    cl <- parallel::makeCluster(no_cores)
    parallel::clusterEvalQ(cl, library("httr"))
    if(outputofParallelProc!=""){
      parallel::clusterEvalQ(cl, sink(paste0(outputOfParallelProc, Sys.getpid(), ".txt")))
    }

    files<-data.frame(fileNamesIn,fileNamesOut)

    parallel::parSapply(cl,1:length(fileNamesIn),getFileMerraFinal,fileNamesIn, fileNamesOut,user,pass)
    parallel::stopCluster(cl)
  }else{
    sapply(1:length(fileNamesIn),getFileMerraFinal,fileNamesIn, fileNamesOut,user,pass)


  }


}



#' reads merra file from disk
#' interpolates the data from the neighbouring 4 points with respect to the point given in lon lat
#' @param ncname is the name of the MERRA file
#' @param pname is the name of the parameter to read
#' @param lon Longitude position to read from
#' @param lat Latitude position to read from
#' @return a tibble with the interpolated timeseries at the given point. Also, time is added
readParamMerra <- function(ncname,pname,lon,lat) {

  ncfile <- nc_open(ncname)

  #Longitude
  longitude <- ncvar_get(ncfile, "lon", verbose = F)
  nlon <- dim(longitude)

  #Latitude
  latitude <- ncvar_get(ncfile, "lat", verbose = F)
  nlat <- dim(latitude)

  #Time
  time <- ncvar_get(ncfile, "time")
  tunits <- ncatt_get(ncfile, "time", "units")
  ntime <- dim(time)

  #read the variable
  m.array <- ncvar_get(ncfile, pname)

  #Dataframe
  m.vec.long <- as.vector(m.array)
  m.mat <- matrix(m.vec.long, nrow = nlon * nlat, ncol = ntime)
  lonlat <- expand.grid(longitude,latitude)
  dista<-sqrt(abs((lonlat[,2]-lat)^2+(lonlat[,1]-lon)^2))
  dfm <- (data.frame(cbind(dista,lonlat, m.mat)))
  dfm <- dfm[ order(dfm[,1]), ]
  dfm$weight<-(max(dfm$dista)-dfm$dista)/sum(max(dfm$dista)-dfm$dista)
  fdg<-dfm$weight*dfm[,4:27]
  fdg<-apply(fdg,2,sum)
  nc_close(ncfile)
  return(fdg)
}


setOldClass("file")
setOldClass("data.frame")
setOldClass("tibble")
setOldClass("POSIXt")
#setOldClass("SpatialPoints")

#' Reference class for Binary gridded timeseries data
#'
#' @field file path to the binary files
#' @field endian Integer endian used on machine (small or big)
#' @field con The connection to the meta file, once opened
#' @field byteSize The size of the integer values in bytes
#' @field dimX X dimension of the coordinate system
#' @field dimY Y dimension of the coordinate system
#' @field timeTim time dimension of the dataset
#' @field lons,lats the longitude and latitude values in the dataset
#' @field time time values in the dataset, in seconds since 1980-01-01
#' @field grid the longitude/latitude grid
#' @field years The years in the dataset
#' @field counts the number of days per year
#' @field timePerFile The number of hours per day
#' @field sizePerYear The number of bytes saved per year per location


MERRABin<- setRefClass("MERRABin",
                       fields=list(file="character",

                                   con="file",
                                   byteSize="integer",
                                   dimX="integer",
                                   dimY="integer",
                                   timeDim="vector",
                                   lons="vector",
                                   lats="vector",
                                   time="vector",
                                   offset="integer",
                                   grid="data.frame",
                                   years="vector",
                                   counts="vector",
                                   timePerFile="integer",
                                   sizePerYear="vector",
                                   timeDate="POSIXt",
                                   spatialPoints="SpatialPoints",
                                   multI="integer"
                       ),

                       methods=list(

                         initialize=function(file){

                           file<-addSlash(file)

                           .self$con<-file(paste(file,"meta.bin",sep=""),open="rb")

                           .self$file<-file

                           #print("here1")

                           ####write meta information to file
                           ####size
                           .self$byteSize<-readBin(con,what="integer",size=8)
                           printl("byte size:",.self$byteSize)

                           ####dimx
                           .self$dimX<-readBin(con,what="integer",size=8)
                           printl("dim X:",.self$dimX)

                           #print("here3")

                           ####dimy
                           .self$dimY<-readBin(con,what="integer",size=8)
                           printl("dim Y:",.self$dimY)

                           #print("here4")

                           ####timedim
                           .self$timeDim<-readBin(con,what="integer",size=8)
                           printl("timeDim:",.self$timeDim)

                           ####timePerFile
                           .self$timePerFile<-readBin(con,what="integer",size=8)
                           printl("timePerFile:",.self$timePerFile)


                           ####lons
                           .self$lons<-readBinMult(con,"double",8,.self$dimX)
                           printl("Lon coordinates:",.self$lons)



                           ####lats
                           .self$lats<-readBinMult(con,"double",8,.self$dimY)
                           printl("Lat coordinates:",.self$lats)

                           ####time
                           .self$time<-readBinMult(con,"integer",8,.self$timeDim)
                           printl("Time values:",head(.self$time))

                           ####amount years
                           y<-readBin(con,"integer",size=8)
                           printl("Number of years:",y)
                           ####years
                           .self$years<-readBinMult(con,"integer",8,y)
                           printl("Years:",head(.self$years))

                           ####counts
                           .self$counts<-readBinMult(con,"integer",8,y)
                           printl("Days per year:",head(.self$counts))

                           ####multI
                           .self$multI<-readBin(con,"integer",8)
                           .self$multI<-.self$multI[1]

                           ####sizePerYear
                           .self$sizePerYear<-.self$timePerFile*.self$counts*.self$byteSize
                           #print(head(.self$sizePerYear))


                           .self$grid<-(expand.grid(.self$lats,.self$lons) %>% as_tibble(.))
                           names(.self$grid)<-c("Lat","Lon")
                           .self$grid<-dplyr::select(.self$grid,Lon,Lat)
                           #print("here9")
                           close(.self$con)

                           .self$timeDate<-as.POSIXct(.self$time,origin="1970-01-01 00:00 UTC",tz="UTC")

                           .self$spatialPoints<-sp::SpatialPoints(.self$grid,
                                                                  proj4string=CRS(as.character("+proj=longlat +ellps=WGS84")))





                         },

                         getDateTime=function(){

                           return(as.POSIXct(.self$time,
                                             origin=as.POSIXct("1970-01-01 00:00",tz="UTC")))

                         },


                         closeFile=function(){
                           close(.self$con)
                         }
                       ))

#' Reads binary encoded timeseries from disk
#'
#' @name MERRABin_readTS
#' @param posTS Position of timeseries. This is an internal representation, it is better to use getClosestTS
#' @return The complete timeseries as vector
NULL

MERRABin$methods(

  readTS=function(posTS){
    readTSYears(posTS,.self$years) %>% return()
  })


#' Finds the closest MERRA points for all given Lons/Lats
#'
#' @name MERRABin_getCorrespondingLonLat
#' @param dat dataframe which contains at least a column lon and a column lat
#' @return Returns dat with added column MLon and MLat, which indicates the associated MERRA Point
NULL

MERRABin$methods(

  getCorrespondingLonLat=function(dat){

    no_cores <-parallel::detectCores() - 1

    # Initiate cluster
    cl <- parallel::makeCluster(no_cores)
    parallel::clusterEvalQ(cl,library(magrittr))
    MerraLonsLats<-parallel::clusterMap(cl,
                                       .self$getMERRAPoint,
                                       dat$lon,
                                       dat$lat,
                                       SIMPLIFY=FALSE)
    parallel::stopCluster(cl)
    #MerraLonsLats<-mapply(.self$getMERRAPoint,
    #                      dat$lon,
    #                      dat$lat)
    m<-t(matrix(unlist(MerraLonsLats),
                nrow=2,
                ncol=nrow(dat)))

    dat %>% dplyr::mutate(MLon=m[,1],MLat=m[,2]) %>%
      return()
    })


#' Returns a timeseries with aggregated capacities per merra point
#' timeseries is extended to the timeseries covered by this MERRAbin object
#'
#' @name MERRABin_getAggregatedCapacitiesTimeSeries
#' @param dat dataframe which contains at least a column lon, a column lat, a column date and a column cap
#' @return Returns the same dateframe, but capacities are aggregated by merra point and the timeseries is extended to the timeseries of this MERRAbin object
NULL

MERRABin$methods(

  getAggregatedCapacitiesTimeSeries=function(dat){

    wind_dat_red_agg <- dat %>% group_by(MLon,MLat) %>% mutate(cumsum=cumsum(cap)) %>%
      ungroup() %>% mutate(datetime=as.POSIXct(paste(date,"00:00:00",sep=""),tz="UTC"))

    wind_dat_red_agg_red<-wind_dat_red_agg[!(duplicated(wind_dat_red_agg[,c(1,2,6)])),]

    d_seq<-mb$timeDate
    MLonLat<-unique(wind_dat_red_agg_red[,c(1,2)])
    MLonLat_<-MLonLat[rep(1:nrow(MLonLat),each=length(d_seq)),]
    td_fin<-tibble(MLon=MLonLat_$MLon,MLat=MLonLat_$MLat,datetime=rep(d_seq,nrow(MLonLat)),val=0)
    full_ts_expansion<-full_join(td_fin,wind_dat_red_agg_red)


    full_ts_expansion %>%
      mutate(cumsumInter=zoo::na.locf(cumsum, fromLast = TRUE)) %>% filter(year(datetime)>1999) %>%
      arrange(MLon,MLat,datetime) %>%  return()


  })


#' Reads binary encoded timeseries from disk forgiven years
#'
#' @name MERRABin_readTS
#' @param posTS Position of timeseries. This is an internal representation, it is better to use getClosestTS
#' @param years Years to read from disk
#' @return The complete timeseries as vector
NULL

MERRABin$methods(

  readTSYears=function(posTS,years){
    ts<-c()
    for(y in years){
      #  for(i in 2:3){
      #print(y)
      i<-which(.self$years==y)
      if(length(i)==0){
        print("y is not in dataset, aborting!")
        return()
      }
      f<-paste(.self$file,.self$years[i],".bin",sep="")
      #print(f)
      con_<-file(f,open="rb")
      pos<-.self$sizePerYear[i]*(posTS-1)

      seek(con_,pos)
      ts_<-readBinMult(con_,
                       what="integer",
                       size=.self$byteSize,
                       .self$sizePerYear[i]/.self$byteSize
      )/multI
      ts<-c(ts,ts_)
      #print(length(ts_))
      close(con_)
    }

    return(ts)
  })


#' Gets timeseries at lon/lat point
#' @description Gets the timeseries from the binary file which is closest to the given longitude/latitude
#'
#' @name MERRABin_getClosestTS
#' @param lon,lat Longitude and Latitude where the closest existing point is found
#' @return The complete timeseries as vector
NULL

MERRABin$methods(


  getClosestTS=function(lon,lat){
    getClosestTSYears(lon,lat,.self$years) %>% return()
  }

)

#' Gets timeseries at lon/lat point for given years
#' @description Gets the timeseries from the binary file which is closest to the given longitude/latitude
#'
#' @name MERRABin_getClosestTS
#' @param lon,lat Longitude and Latitude where the closest existing point is found
#' @param years Years to load from file
#' @return The complete timeseries as vector
NULL

MERRABin$methods(


  getClosestTSYears=function(lon,lat,years){
    t1<-Sys.time()
    #sel<-.self$grid %>% dplyr::filter(Lon<(lon+1)&Lon>(lon-1))
    #if(nrow(sel)==0){
    #  dists<-distm(.self$grid,
    #               c(lon, lat), fun = distHaversine)
    #  d<-.self$grid %>% slice(which(dists==min(dists)))

    #}else{
    #  dists<-distm(sel,
    #               c(lon, lat), fun = distHaversine)
    #  d<-sel %>% slice(which(dists==min(dists)))
    #}
    d<-.self$getMERRAPoint(lon,lat)
    #print(paste(d$Lon,d$Lat))
    tsInd<-which(.self$grid$Lon==d[1]&.self$grid$Lat==d[2])
    #print(tsInd)
    ts<-readTSYears(tsInd,years)
    t2<-Sys.time()
    #print(t2-t1)
    return(ts)
  }

)




#' gets the timeseries of timestamps
#' @description returns a timeseries of timestamps in UTC
#' @return The timestampe timeseries
NULL

MERRABin$methods(


  getTime=function(){
    .self$timeDate %>% return()
  }

)

#' gets the timeseries of timestamps with limitation to certain years
#' @description returns a timeseries of timestamps in UTC
#' @return The timestamp timeseries
NULL

MERRABin$methods(


  getTimeYears=function(years){

    u<-unique(year(.self$timeDate))
    if(!all(years %in% u)) {
      print("ERROR not all years found. The following years are in the current BIN object:")
      print(u)
      return()
    }

    return(.self$timeDate[year(.self$timeDate) %in% years])

  }

)

#' gets the set of timeseries which are related to the MERRA points
#' in the given shape file
#'
#' @param shapefile The shapefile to read
#' @param years The years to read
#' @return A tibble of timeseries
NULL

MERRABin$methods(


  getShape=function(shape,years=c()){
    print("This may take some time...")
    if(length(years)==0){
      years<-.self$years
    }


    sp_proj<-spTransform(mb$spatialPoints,projection(SE_border))
    res<-sp_proj[shape,]

    dat<-unlist(mapply(.self$getClosestTSYears,res$Lon,res$Lat,list(years),SIMPLIFY = FALSE))
    time<-.self$getTimeYears(years)
    dat<-as_tibble(data.frame(time=time,val=dat,Lon=rep(res$Lon,each=length(time)),Lat=rep(res$Lat,each=length(time))))
    return(dat)

  }

)





MERRABin$methods(


#' Returns the closest MERRA Point
#' @description returns the coordinates of the MERRA point which is closest to the given point
#' @param lon Longitude of point to assess
#' @param lat Latitude of point to assess
#'
#' @return A vector where the first element is the longitude and the second element is the latitude of the closest MERRA point
#' @export
#'
#' @examples
  getMERRAPoint=function(lon,lat){
    sel<-.self$grid %>% dplyr::filter(Lon<(lon+0.26)&
                                      Lon>(lon-0.26)&
                                      Lat<(lat+0.26)&
                                      Lat>(lat-0.26)
                                      )

    if(nrow(sel)==0){
      print("Point not inside grid. This may take some time!")
      dists<-distm(.self$grid,
                   c(lon, lat), fun = distHaversine)
      d<-.self$grid %>% dplyr::slice(which(dists==min(dists)))


    }else{

      if(nrow(sel)>1){
        dists<-distm(sel,
                     c(lon, lat), fun = distHaversine)
        d<-sel %>% dplyr::slice(which(dists==min(dists)))
        }else{
      d<-sel
        }
    }
    return(c(d$Lon,d$Lat))


  }

)



#' Prints to element to console
#'
#' @description Prints two elements to the console, concatenating them
#' @param a Element A
#' @param b Element B
printl<-function(a,b){
  print(paste(a,b))
}



#' Reads multiple bytes from a binary file
#' @param con Binary connection to file
#' @param what Type of variable to read (e.g. Integer, Double, etc.)
#' @param size Size of variable in bytes
#' @param elements Number of elements
#' @param endian Endian type of number (Big or Small)
#' @return The values read in from the binary connection
readBinMult<-function(con,what,size,elements){

  c<-readBin(con=con,what=what,n=elements,size=size)
  return(c)

}



#' Adds Slash to path name
#'
#' @description adds a slash to the end of a path name
#'
#' @param targetPath Path name
#'
#' @return A path name with trailing slash
addSlash<-function(targetPath){

  lastChar<-substr(targetPath, nchar(targetPath),nchar(targetPath))

  if(lastChar!="/"&&lastChar!="\\"){

    targetPath<-paste(targetPath,"/",sep="")

  }

  return(targetPath)

}





#' conversion of merra files to binary files
#'
#' @description converts merra files to a fast readable binary format
#' @param in_path Path of merra files
#' @param out_path path where output binary files should be located
#' @param date_seq sequence of dates which should be converted
#' @param param Parameter in MERRA file that should be converted
#' @param multI multiplicator for variable (to be used if numbers after comma are relevant)
#' @param silent If true, no detailed output on handling of files is printed to console
convMerraToBin<-function(in_path,out_path,date_seq,param,multI,silent=TRUE){

  print(paste("Starting conversion of MERRA to Binary Files"))
  print(in_path)
  print(out_path)
  print(param)


  in_path<-addSlash(in_path)
  out_path<-addSlash(out_path)


  dir.create(out_path, showWarnings = FALSE)


  t_start <- Sys.time()

  con<-file(paste(out_path,"meta.bin",sep=""),open="wb")

  listOfFiles<-list.files(path=in_path,pattern="MERRA")
  listOfFiles<-paste(in_path,listOfFiles,sep="")

  datesInFiles<-substr(listOfFiles,nchar(listOfFiles[1])-11,nchar(listOfFiles[1])-4)

  strDatesSeq<-paste(year(date_seq),
                     sprintf("%02i",month(date_seq)),
                     sprintf("%02i",day(date_seq)),sep="")

  diff<-setdiff(strDatesSeq,datesInFiles)

  if(length(diff)>0){
    print("ERRROR: The following dates could not be found in the MERRA files:")
    print(diff)
    print("ABORTING")
    return()
  }

  nc_file<-nc_open(listOfFiles[1])
  temp<-ncvar_get(nc_file,param)
  dimX<-dim(temp)[1]
  dimY<-dim(temp)[2]
  timePerFile<-dim(temp)[3]



  lons<-ncvar_get(nc_file,"lon")
  lats<-ncvar_get(nc_file,"lat")
  time<-ncvar_get(nc_file,"time")

  timeDim<-length(listOfFiles)*timePerFile

  nc_close(nc_file)

  length<-timeDim

  #size
  size<-2
  writeBin(as.integer(size),con,8)

  #dimx
  writeBin(as.integer(dimX),con,8)

  #dimy
  writeBin(as.integer(dimY),con,8)

  #timeDim
  writeBin(as.integer(timeDim),con,8);

  #timePerFile
  writeBin(as.integer(timePerFile),con,8);

  #lons
  writeBin(as.vector(lons),con,8);

  #lats
  writeBin(as.vector(lats),con,8);

  #time
  date_seq_h<-seq(date_seq[1],date_seq[length(date_seq)]+3600*23,by="h")

  writeBin(as.integer(date_seq_h),con,8)


  #amount years
  y_l<-length(unique(year(date_seq)))

  ####amount years
  writeBin(as.integer(y_l),con,8)
  print(y_l)

  ####years
  years<-unique(year(date_seq))
  writeBin(as.vector(as.integer(years)),con,8)

  ####counts
  td<-tibble(dat=date_seq,year=year(date_seq)) %>% group_by(year) %>% summarize(n=n())
  writeBin(as.vector(td$n),con,8)

  ####multI
  writeBin(as.integer(multI),con,8)

  close(con)


  ####write single years
  minY<-min(year(date_seq))
  maxY<-max(year(date_seq))
  mapply(writeSingleYearFile,
         minY:maxY,
         list(listOfFiles),
         list(date_seq),
         list(param),
         list(timePerFile),
         list(dimX),
         list(dimY),
         list(out_path),
         list(multI))



}

#' Helper function to convert one year of MERRA data to a binary file
#'
#' @description This is a helper function used to converted one year of MERRA data to a binary file
#' @param year Year to convert
#' @param listOfFiles list of MERRA files to convert
#' @param date_seq vector with sequence of all dates to be converted (including the given year)
#' @param param Parameter in MERRA file that should be converted
#' @param timePerFile number of hours per file
#' @param dimX X dimension of spatial grid
#' @param dimY Y dimension of spatial grid
#' @param out_path Path to write file to
#' @param multVar multiplicator for variable (to be used if numbers after comma are relevant)
#' @param silent If true, no detailed output on handling of files is printed to console
writeSingleYearFile<-function(year,listOfFiles,date_seq,param,timePerFile,dimX,dimY,out_path,multVar,silent=TRUE) {

  count<-sum(year(date_seq)==year)

  print(paste("----------Working on year...",year,"----------"))


  if(!file.exists(paste(out_path,year,".bin",sep=""))) {

    length=count*timePerFile

    cc<-0
    listOfFilesRed <- listOfFiles[year(date_seq)==year]
    out<-as.integer(rep(0,count*timePerFile*dimX*dimY))

    locpos<-(0:(dimX*dimY-1))*length
    cc<-0
    reorder1<-((cc*timePerFile):(cc*timePerFile+timePerFile-1))+1
    tt<-as.vector(mapply(sum_,locpos,list(reorder1),SIMPLIFY=TRUE))

    for(k in c(1:length(listOfFilesRed))){

      t_start_=Sys.time()
      if(!silent){
        print(paste("working on ",listOfFilesRed[k]))
      }
      nc_file<-nc_open(listOfFilesRed[k])
      dat<-aperm(ncvar_get(nc_file,param)*multVar,c(3,2,1))
      dat<-as.vector(dat)
      dat[is.na(dat)]<-0
      nc_close(nc_file)

      out[tt+cc*timePerFile]<-as.integer(dat)
      cc<-cc+1

      if(!silent){
        print(paste(listOfFilesRed[k]," took ", (Sys.time()-t_start_)))
      }

    }

    con = file(paste(out_path,year,".bin",sep=""),open="wb")

    writeBinLong(out, con,2)

    close(con)



  }

}



#' Writes a very long vector to a binary file
#'
#' @description The R function writeBin is limited by 2^31-1 bytes. This function uses writeBin
#' iteratively to be able to write longer files
#' @param bytes The files to write
#' @param connection The connection to write to
#' @param size The element size in bytes
#'
#' @return
#' @export
#'
#' @examples
writeBinLong<-function(bytes,connection,size){

  maxL<-2^31-1

  divisions<-ceiling(length(bytes)*size/maxL)

  maxL<-floor((2^31-1)/size)

  for(i in 1:divisions){
    end<-min(i*maxL,length(bytes))
    writeBin(bytes[((i-1)*maxL+1):end],connection,size)
  }

}

#' Helper function to sum up to numbers
#'
#' @description Adds two numbers
#' @param a,b numbers to add
#' @return the sum of the two numbers

sum_<-function(a,b){
  return(a+b)
}

#' Creates a MERRA Test Data Set
#'
#' @description Creates the test-data set also available as data in the package
#' @param dir Directory to save the test data set
#' @param date_seq length of test data set
#' @return Does not return anything. Writes, as side-effect, two files in the given directory: test_data.feater and MERRA_Test.Rdata, which contain the same information
createTestDataSet<-function(dir,date_seq){



  setwd(dir)
  getMERRADataBox(-180,-90,180,90,
                  date_seq,
                  "T2M",
                  "RE_EXTREME",
                  "Re_extreme666!",
                  FALSE)
  start<-Sys.time()
  filelist<-list.files(path=".",pattern = "MERRA")

  df<-data.frame(matrix(nrow=(length(date_seq)*24),ncol=9))

  lon_pos<-c(1,1,1,1,1,1,2,2,3,3)
  lat_pos<-c(1,2,3,4,5,6,1,2,1,2)


  if(!file.exists("testdata.feather")){

    cnt<-1
    for(file in filelist){

      f<-file
      print(f)

      nc_file<-nc_open(f)
      dat<-ncvar_get(nc_file,"T2M")
      nc_close(nc_file)

      rows<-cnt:(cnt+23)

      for(j in 1:10){
        df[rows, j]<-dat[lon_pos[j]  ,lat_pos[j]  ,]
      }

      cnt<-cnt+24
    }

    par(mfrow=c(1,1))
    matplot(df-273.5,type="l")

    xdim<-dim(dat)[1]
    ydim<-dim(dat)[2]
    tot_pos<-(lon_pos-1)*ydim+lat_pos

    names(df)<-tot_pos
    test_data_fin<-as_tibble(df) %>% gather(pos,val) %>% mutate(pos=as.numeric(pos))

    write_feather(test_data_fin,path="testdata.feather")
    MERRA_Test<-test_data_fin
    save(MERRA_Test,file="MERRA_Test.RData")
    end<-Sys.time()
    print(end-start)
  }



  test_data<-read_feather("testdata.feather")

  return(test_data)

}

#' Title
#'
#' @param lon
#' @param lat
#' @param u10m
#' @param u50m
#' @param v10m
#' @param v50m
#' @param disph
#' @param speeds_in
#' @param power_out
#' @param nmbTurbines
#'
#' @return
#' @export
#'
#' @examples
windOutput<-function(lon,lat,u10m,u50m,v10m,v50m,disph,speeds_in,power_out,nmbTurbines){
  print(paste("Calculating wind power output at ",lon,lat))
  u10m<-u10m$getClosestTS(lon,lat)
  u50m<-u50m$getClosestTS(lon,lat)
  v10m<-v10m$getClosestTS(lon,lat)
  v50m<-v50m$getClosestTS(lon,lat)
  disph<-disph$getClosestTS(lon,lat)


  v10<-sqrt(u10m^2+v10m^2)
  v50<-sqrt(u50m^2+v50m^2)

  wind_interpolated<-power_law_interpolation(v10,v50,10,50,disph,100)
  hist(wind_interpolated)
  powercalc(speeds_in,power_out,wind_interpolated,nmbTurbines) %>%
    return()


}


#' Title
#'
#' @param v1
#' @param v2
#' @param h1
#' @param h2
#' @param dH
#' @param h
#'
#' @return
#' @export
#'
#' @examples
power_law_interpolation<-function(v1,v2,h1,h2,dH,h){
  h2<-h2+dH
  h1<-h1+dH
  a <- (log(v2)-log(v1))/(log(h2)-log(h1))
  vz<-v2*(h/h2)^a

  return(vz)
}

#' Title
#'
#' @param wIn
#' @param pOut
#' @param windSpeeds
#' @param numberTurbines
#'
#' @return
#' @export
#'
#' @examples
powercalc<-function(wIn,pOut,windSpeeds,numberTurbines){
  func <- approxfun(wIn, pOut)
  TApl1 <- sapply(windSpeeds, FUN = "func")
  TApwr1 <- numberTurbines*TApl1
  TApwr1[is.na(TApwr1)]<-0
  return(TApwr1)
}


#' Title
#'
#' @param fileOPSD
#'
#' @return
#' @export
#'
#' @examples
getOPSDTS<-function(fileOPSD){
  if(!file.exists(fileOPSD)){

    dir.create(dirname(fileOPSD), showWarnings = FALSE)
    download.file("https://data.open-power-system-data.org/time_series/2016-07-14/timeseries60min_stacked.csv",
                destfile=fileOPSD)
  }

  tab_<-readr::read_delim(fileOPSD,delim=",")

  return(tab_)

}

#' Title
#'
#' @param fileOPSD
#'
#' @return
#' @export
#'
#' @examples
getOPSDRenPowerPlantsDE<-function(fileOPSD){

  if(!file.exists(fileOPSD)){

    dir.create(dirname(fileOPSD), showWarnings = FALSE)
    download.file("https://data.open-power-system-data.org/renewable_power_plants/2016-10-21/renewable_power_plants_DE.csv",
                  destfile=fileOPSD)
  }

  tab_<-readr::read_delim(fileOPSD,delim=",")

  return(tab_)



}

#' Title
#'
#' @param tab_
#' @param variable_
#' @param region_
#' @param attribute_
#'
#' @return
#' @export
#'
#' @examples
getOPSDParam<-function(tab_,variable_,region_,attribute_){

  tab_ %>% dplyr::filter(variable %in% variable_ &
                         region %in% region_ &
                         attribute %in% attribute_) %>% return()

}

#' Title
#'
#' @param tab_
#'
#' @return
#' @export
#'
#' @examples
getOPSDvariables<-function(tab_){
  return(unique(tab_$variable))
}

#' Title
#'
#' @param tab_
#'
#' @return
#' @export
#'
#' @examples
getOPSDregion<-function(tab_){
  return(unique(tab_$region))
}

#' Title
#'
#' @param tab_
#'
#' @return
#' @export
#'
#' @examples
getOPSDattribute<-function(tab_){
  return(unique(tab_$attribute))
}




