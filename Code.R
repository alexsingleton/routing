#?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?#
#DATA PREP
#?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?#

#Get and install R packages
install.packages("RMySQL", dependencies = TRUE)
install.packages("gdata", dependencies = TRUE)
install.packages("rjson", dependencies = TRUE)
install.packages("RCurl", dependencies = TRUE)
install.packages("geosphere", dependencies = TRUE)
install.packages("FNN", dependencies = TRUE)
install.packages("rgdal", dependencies = TRUE)
install.packages("doBy", dependencies = TRUE)
install.packages("sqldf", dependencies = TRUE)
install.packages("RPostgreSQL", dependencies = TRUE)
install.packages("stringr", dependencies = TRUE)
install.packages("doBy", dependencies = TRUE)# Packages for summaryBy

library(doBy)
library("RMySQL")
library("gdata")#reading Excel
library("rjson")
library(RCurl)
library("geosphere")#lat lon distances
library("FNN")
library("rgdal")
library(doBy)
library(sqldf)
library("RPostgreSQL")
library(stringr)


#Get Education Data 
con <- dbConnect(MySQL(), user="root", password="agra123", dbname="education_raw", host="localhost")
rs = dbSendQuery(con,"select PupilMatchingRefAnonymous_SPR11, EthnicGroupMajor_SPR11, Gender_SPR11, Postcode_SPR11,ModeOfTravel_SPR11, LAEstab_SPR11,URN_SPR11, NCyearActual_SPR11 from Spring_Census_2011")
ed_data = fetch(rs, n = -1)


ed_data$Postcode_SPR11   <- gsub(" ","", ed_data$Postcode_SPR11, fixed=TRUE)

#Get NSPD

temp <- tempfile()
download.file("http://parlvid.mysociety.org:81/os/ONSPD_FEB_2012_UK_O.zip",temp) 
NSPD <- read.csv(unz(temp, "ONSPD_FEB_2012_UK_O.csv"),head = FALSE) 
unlink(temp)

NSPD <- NSPD[,c("V1","V38","V10","V11")]
NSPD$V1   <- gsub(" ","", NSPD$V1, fixed=TRUE)
colnames(NSPD) <- c("Postcode","LSOA","E","N")


#Get ACORN
con <- dbConnect(MySQL(), user="root", password="agra123", dbname="geodemographics", host="localhost")
rs = dbSendQuery(con,"SELECT newpcd,acorncategory,acorngroup,acorntype FROM acorn")
ACORN = fetch(rs, n = -1)

#Get IMD
IMD <- read.xls('http://www.communities.gov.uk/documents/statistics/xls/1871524.xls', sheet=2,verbose=FALSE)
IMD$decile <- cut(IMD$IMD.SCORE,quantile(IMD$IMD.SCORE,(0:10)/10),labels=FALSE,include.lowest=TRUE)
IMD <- IMD[,c(1,6:8)]
colnames(IMD) <- c('LSOACODE','IMD_2010_S','IMD_2010_R','IMD_2010_D')


#Join NSPD, IMD and ACORN to Education Data

ed_data <- merge(ed_data, ACORN, by.x='Postcode_SPR11', by.y='newpcd', all.x=TRUE)
ed_data <- merge(ed_data, NSPD, by.x='Postcode_SPR11', by.y='Postcode', all.x=TRUE)
ed_data <- merge(ed_data, IMD, by.x='LSOA', by.y='LSOACODE', all.x=TRUE)



##Import Edubase##
dir <- "/Volumes/Macintosh HD 2/Dropbox/Projects/School_Carbon/Final Version/Edubase_Data/"
input <- "extract.csv"

comb <- paste(dir,input, sep = '')
edubase <- read.csv(comb, header = TRUE, sep = ",", quote="\"", dec=".", colClasses = "character")

edubase2 <- subset(edubase, select = c(URN,EstablishmentName,LA..code.,Postcode,TypeOfEstablishment..name.,PhaseOfEducation..name.))

#Append Edubase to schools
ed_data <- merge(ed_data, edubase2, by.x='URN_SPR11', by.y='URN', all.x=TRUE)
ed_data$Postcode   <- gsub(" ","", ed_data$Postcode, fixed=TRUE)


#Get the postcode lat long
dir <- "/Volumes/Macintosh HD 2/Dropbox/Projects/School_Carbon/Final Version/"
input <- "Nov_11_lat_long.txt"

comb <- paste(dir,input, sep = '')

latlng <- read.csv(comb, header = TRUE, sep = ",", quote="\"", dec=".")

latlng <- subset(latlng,select = c(PCD,Lat,Long_))
latlng$PCD <- gsub("\\s","",latlng$PCD)

#Join lat long onto pupils and schools

#Append lat long to pupils home
names(latlng) <- c('PCD','Pupil_Lat','Pupil_Lon')
ed_data <- merge(ed_data, latlng, by.x='Postcode_SPR11', by.y='PCD', all.x=TRUE)

names(latlng) <- c('PCD','School_Lat','School_Lon')
ed_data <- merge(ed_data, latlng, by.x='Postcode', by.y='PCD', all.x=TRUE)

rm(list=setdiff(ls(), c("ed_data","i","final","OD")))


#Remove records where postcode is in error / georef is not available.

all_LA <- all_LA[!is.na(all_LA$Postcode),]
all_LA <- all_LA[!is.na(all_LA$PCD),]
all_LA <- all_LA[!is.na(all_LA$Pupil_Lat),]
all_LA <- all_LA[!is.na(all_LA$Pupil_Lon),]
all_LA <- all_LA[!is.na(all_LA$School_Lat),]
all_LA <- all_LA[!is.na(all_LA$School_Lon),]


#Remove Some Junk

all_LA$LSOA <- NULL

#Remove blank and BDR and Other modes

all_LA <- subset(all_LA, !(Mode %in% c("","BDR","OTH")))



#?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?#
#Road Network Distances 
#?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?#

#### Create OD

OD_split <- split(ed_data,ed_data$LA..code.)

all_LA <- NULL

for(j in 1:length(names(OD_split))) { 
  
  LA <- names(OD_split)[j]
  nam <- paste("LA_",LA, sep="")
    
  OD <- subset(ed_data,LA..code. == paste(LA))
  
  final <- NULL
  fileloc <- "/Users/alex/quickest-all.txt"
  
  #Type Codes - R=Routino; SL=strightline SP = Same Postcode MV = missing values
  
  for (i in 1:nrow(OD))  {
    
    UID <- OD[i,]$PupilMatchingRefAnonymous_SPR11
    Mode <-OD[i,]$ModeOfTravel_SPR11
    Pupil_Lat <-OD[i,]$Pupil_Lat
    Pupil_Lon <-OD[i,]$Pupil_Lon
    School_Lat <-OD[i,]$School_Lat
    School_Lon <-OD[i,]$School_Lon
    Postcode <-OD[i,]$Postcode
    PCD <-OD[i,]$Postcode_SPR11
    URN_SPR11 <-OD[i,]$URN_SPR11
    LSOA<-OD[i,]$LSOA
    EthnicGroupMajor_SPR11 <-OD[i,]$EthnicGroupMajor_SPR11
    Gender_SPR11 <-OD[i,]$Gender_SPR11
    LAEstab_SPR11 <-OD[i,]$LAEstab_SPR11
    NCyearActual_SPR11 <-OD[i,]$NCyearActual_SPR11
    acorncategory <-OD[i,]$acorncategory
    acorngroup <-OD[i,]$acorngroup
    acorntype <-OD[i,]$acorntype
    EstablishmentName <-OD[i,]$EstablishmentName
    LA..code.<-OD[i,]$LA..code.
    TypeOfEstablishment..name. <-OD[i,]$TypeOfEstablishment..name.
    IMD_2010_S <-OD[i,]$IMD_2010_S
    IMD_2010_D <-OD[i,]$IMD_2010_D
    IMD_2010_R <-OD[i,]$IMD_2010_R
    PhaseOfEducation..name. <-OD[i,]$PhaseOfEducation..name.
    
    if (is.na(Pupil_Lat)||is.na(Pupil_Lon)||is.na(School_Lat)||is.na(School_Lat)) {
      
      type <- "MV"
      
      distance <- NA
      
      tmp <- c(UID, Mode, Pupil_Lat, Pupil_Lon, School_Lat, School_Lon, Postcode, PCD, URN_SPR11, LSOA, EthnicGroupMajor_SPR11, Gender_SPR11, LAEstab_SPR11, NCyearActual_SPR11, acorncategory, acorngroup, acorntype, EstablishmentName, LA..code., TypeOfEstablishment..name., IMD_2010_D, IMD_2010_S, IMD_2010_R, PhaseOfEducation..name., distance,type)
      
      final <- rbind(final,tmp)
      
      print(i) 
      
      rm(list=setdiff(ls(), c("ed_data","i","final","OD","fileloc","nam","j","OD_split","all_LA")))
      
      
    } else {
      
      if (Postcode != PCD){
        
        #Loop to assign the transport variabe
        if (Mode %in% c('Walk','WLK')) {
          
          transport <- "foot"
          
        } else {
          
          transport <- "motorcar"
        }
        
        router <- paste("router --transport=",transport," --prefix=gb --quickest  --lon1=",Pupil_Lon," --lat1=",Pupil_Lat," --lon2=",School_Lon,"   --lat2=",School_Lat,"  --output-text-all --quiet --profiles=/Users/alex/routino-2.2/xml/routino-profiles.xml --dir=/Users/alex/routino-2.2/",sep='')
        
        
        system(router, wait=TRUE) # Send the routing command
        
        suppressWarnings(try_read <- try(read.delim(fileloc, header = FALSE, sep = "\t", skip = 4)))
        
        if ((file.exists(fileloc)) && (!inherits(try_read, 'try-error'))){
          
          
          routeresults <- read.delim(fileloc, header = FALSE, sep = "\t", skip = 4)
          distance <- max(routeresults$V7)
          
          type <- "R"
          
          tmp <- c(UID, Mode, Pupil_Lat, Pupil_Lon, School_Lat, School_Lon, Postcode, PCD, URN_SPR11, LSOA, EthnicGroupMajor_SPR11, Gender_SPR11, LAEstab_SPR11, NCyearActual_SPR11, acorncategory, acorngroup, acorntype, EstablishmentName, LA..code., TypeOfEstablishment..name., IMD_2010_D, IMD_2010_S, IMD_2010_R, PhaseOfEducation..name., distance,type)
          
          
          final <- rbind(final,tmp)
          
          system(" rm /Users/alex/quickest-all.txt", wait=TRUE)
          
          
          rm(list=setdiff(ls(), c("ed_data","i","final","OD","fileloc","nam","j","OD_split","all_LA")))
          
          
          print(i)
          
          
        } else { #do straight line distance
          
          
          p1 <- c(-Pupil_Lon,Pupil_Lat) #pupil
          p2 <- c(-School_Lon,School_Lat) #pupil
          
          distance <-  distHaversine(p1,p2) / 1000
          
          
          type <- "SL"
          
          tmp <- c(UID, Mode, Pupil_Lat, Pupil_Lon, School_Lat, School_Lon, Postcode, PCD, URN_SPR11, LSOA, EthnicGroupMajor_SPR11, Gender_SPR11, LAEstab_SPR11, NCyearActual_SPR11, acorncategory, acorngroup, acorntype, EstablishmentName, LA..code., TypeOfEstablishment..name., IMD_2010_D, IMD_2010_S, IMD_2010_R, PhaseOfEducation..name., distance,type)
          
          
          final <- rbind(final,tmp)
          
          
          print(i) 
          
          rm(list=setdiff(ls(), c("ed_data","i","final","OD","fileloc","nam","j","OD_split","all_LA")))
          
        }
        
      } else {
        
        distance <- 0
        
        type <- "SP"
        
        tmp <- c(UID, Mode, Pupil_Lat, Pupil_Lon, School_Lat, School_Lon, Postcode, PCD, URN_SPR11, LSOA, EthnicGroupMajor_SPR11, Gender_SPR11, LAEstab_SPR11, NCyearActual_SPR11, acorncategory, acorngroup, acorntype, EstablishmentName, LA..code., TypeOfEstablishment..name., IMD_2010_D, IMD_2010_S, IMD_2010_R, PhaseOfEducation..name., distance, type)
        
        
        final <- rbind(final,tmp)
        
        rm(list=setdiff(ls(), c("ed_data","i","final","OD","fileloc","nam","j","OD_split","all_LA")))
        
        
        print(i)
        
      }
      
    }
    
    
  }
  
  colnames(final) <- c('UID', 'Mode', 'Pupil_Lat', 'Pupil_Lon', 'School_Lat', 'School_Lon', 'Postcode', 'PCD', 'URN_SPR11', 'LSOA', 'EthnicGroupMajor_SPR11', 'Gender_SPR11', 'LAEstab_SPR11', 'NCyearActual_SPR11', 'acorncategory', 'acorngroup', 'acorntype', 'EstablishmentName', 'LA..code.', 'TypeOfEstablishment..name.', 'IMD_2010_D', 'IMD_2010_S', 'IMD_2010_R', 'PhaseOfEducation..name.', 'distance', 'type')
  
  assign(paste(nam),final)
  
  write.csv(final,file=paste("/Volumes/Macintosh HD 2/Dropbox/Projects/School_Carbon/Final Version/",nam,".csv",sep=''))
  
  all_LA <-rbind(all_LA,get(nam))
  
}


rm(list=setdiff(ls(), c("ed_data","OD_split")))


write.csv(all_LA,file="/Volumes/Macintosh HD 2/Dropbox/Projects/School_Carbon/Final Version/all_LA_With_Distances.csv")




#?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?#
#Non Road Network Distances 
#?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?#


#load("/Users/alex/Dropbox/Projects/School_Carbon/Final/Railway_Analysis.RData")


#Set working directory
#setwd("/Volumes/Macintosh HD 2/Dropbox/Projects/School_Carbon/Final/")#work
setwd("/Users/alex/Dropbox/Projects/School_Carbon/Final")#laptop


#Read in previous results
all_LA <- read.csv(file="all_LA_With_Distances.csv")

#Create ID
id<- as.vector(seq(1:nrow(all_LA)))
all_LA<-cbind(all_LA,id)




#Download ONS Postcode Directory (ONSPD) from MySociety
temp <- tempfile()
download.file("http://parlvid.mysociety.org:81/os/ONSPD_FEB_2012_UK_O.zip",temp) 
NSPD <- read.csv(unz(temp, "ONSPD_FEB_2012_UK_O.csv"),head = FALSE) 
unlink(temp)

#Cut NSPD down and apply to all_LA...

NSPD <- subset(NSPD, !is.na(V10) & !is.na(V11), select=c("V1","V10","V11","V36","V38"))
NSPD$V1   <- gsub(" ","", NSPD$V1, fixed=TRUE)

colnames(NSPD) <- c("Postcode","pupil_East","pupil_North","CASWARD","LSOA")
all_LA <- merge(all_LA, NSPD, by.x='PCD', by.y='Postcode', all.x=TRUE)

NSPD2 <- subset(NSPD,select=c("Postcode","pupil_East","pupil_North"))

colnames(NSPD2) <- c("Postcode","school_East","school_North")
all_LA <- merge(all_LA, NSPD2, by.x='Postcode', by.y='Postcode', all.x=TRUE)

NSPD2 <- NULL


#Calculate straight line

all_LA$sl_distance <- sqrt((all_LA$pupil_East - all_LA$school_East)^2 + (all_LA$pupil_North - all_LA$school_North)^2) / 1000


rail <- all_LA[(!is.na(all_LA$pupil_North) & !is.na(all_LA$pupil_East) & !is.na(all_LA$school_North) & !is.na(all_LA$school_East) & all_LA$Mode=='TRN'),]
tram <- all_LA[(!is.na(all_LA$pupil_North) & !is.na(all_LA$pupil_East) & !is.na(all_LA$school_North) & !is.na(all_LA$school_East) & all_LA$Mode=='MTL'),]
tube <- all_LA[(!is.na(all_LA$pupil_North) & !is.na(all_LA$pupil_East) & !is.na(all_LA$school_North) & !is.na(all_LA$school_East) & all_LA$Mode=='LUL'),]




###########################################
## TTTTTTT RRRRRR    AAA   MM    MM  SSSSS  
#    TTT   RR   RR  AAAAA  MMM  MMM SS      
#    TTT   RRRRRR  AA   AA MM MM MM  SSSSS  
#    TTT   RR  RR  AAAAAAA MM    MM      SS 
#    TTT   RR   RR AA   AA MM    MM  SSSSS  
##########################################




##########
#TRAMS >>> Pupils
##########


#Create pupil / Tram location objects

pupil_tram_xy <- as.matrix(tram[,c("pupil_East","pupil_North")])

tram_locations <- readOGR("Rail", "all_tram_light_rail_stations")

tram_tram_xy <- as.matrix(cbind(tram_locations@data$x,tram_locations@data$y))


#Nearest Neighbour - Find closest tram stop to a pupil home
nn = get.knnx(tram_tram_xy,pupil_tram_xy,1) #Nearest Neighbour
colnames(tram_locations@data) <- c("network","PUPIL_TRAM_ID","tram_x_pupil","tram_y_pupil")
tram_match <- cbind(tram,tram_locations@data[nn$nn.index,]) #Append Nearest Neighbour
tram_match <- cbind(tram_match,(nn$nn.dist/1000)) #Append Distance to Nearest Neighbour
colnames(tram_match)[40] <- "distance_station"


#Check the data for outliers caused by an error in mode choice

#Calculate Quartiles and IQR
Quartiles_tram <- summaryBy(distance_station ~ network, data = tram_match, 
                            FUN = function(x) { c(IQR = IQR(x,na.rm=TRUE, type= 7),m = quantile(x,na.rm=TRUE)) } )
colnames(Quartiles_tram) <- c("network","IQR","","Q1","","Q3","")
Quartiles_tram <- subset(Quartiles_tram,  select=c(network, IQR, Q1, Q3))

tram_match <- merge(tram_match,Quartiles_tram, by= "network", all.x=TRUE)

attach(tram_match)
tram_match$outlier_tram <- ifelse((distance_station < (Q1 - (1.5 * IQR)) | distance_station > (Q3 + (1.5 * IQR))),1,0)                    
detach(tram_match)

tram_match$IQR <- NULL
tram_match$Q1 <- NULL
tram_match$Q3 <- NULL








##########
#TRAMS >>> Schools
##########


#Get a table of unique schools and their locations from the tram_match data
options(sqldf.driver = "SQLite")

schools <- sqldf("select LAEstab_SPR11, network, school_East, school_North from tram_match group by LAEstab_SPR11, school_East, school_North ")
school_tram_xy <- as.matrix(schools[,c("school_East","school_North")])


#Nearest Neighbour - Find closest tram stop to a school
nn = get.knnx(tram_tram_xy,school_tram_xy,1) #Nearest Neighbour
colnames(tram_locations@data) <- c("network","SCHOOL_TRAM_ID","tram_x_school","tram_y_school")
tram_school_match <- cbind(schools,tram_locations@data[nn$nn.index,]) #Append Nearest Neighbour
tram_school_match <- subset(tram_school_match,select=c("LAEstab_SPR11","network","SCHOOL_TRAM_ID","tram_x_school","tram_y_school"))
tram_school_match <- cbind(tram_school_match,(nn$nn.dist/1000)) #Append Distance to Nearest Neighbour

colnames(tram_school_match)[6] <- "distance_station"

#Calculate Quartiles and IQR
Quartiles_SL_to_school_tram <- summaryBy(distance_station ~ network, data = tram_school_match, 
                            FUN = function(x) { c(IQR = IQR(x,na.rm=TRUE, type= 7),m = quantile(x,na.rm=TRUE)) } )

colnames(Quartiles_SL_to_school_tram) <- c("network","school_IQR","","school_Q1","","school_Q3","")
Quartiles_SL_to_school_tram <- subset(Quartiles_SL_to_school_tram,  select=c(network, school_IQR, school_Q1, school_Q3))

#Check each of the schools

tram_school_match <- merge(tram_school_match,Quartiles_SL_to_school_tram, by= "network", all.x=TRUE)


attach(tram_school_match)
tram_school_match$outlier_school_tram <- ifelse((distance_station < (school_Q1 - (1.5 * school_IQR)) | distance_station > (school_Q3 + (1.5 * school_IQR))),1,0)                    
detach(tram_school_match)

tram_school_match <- subset(tram_school_match,select=c('LAEstab_SPR11','outlier_school_tram','SCHOOL_TRAM_ID'))


#Join schools back onto tram_match & add an outlier flag to the pupil record if the school is considered too far from a station to be viable
tram_match <- merge(tram_match, tram_school_match, by="LAEstab_SPR11", all.x=TRUE)

tram_match$outlier_tram <- ifelse(tram_match$outlier_school_tram == 1,1,tram_match$outlier_tram)                    


#Add outlier flag if the journeys cross netwok (usually found in london - i.e. docklands / croydon)
tram_match$pupil_net <- str_split_fixed(tram_match$PUPIL_TRAM_ID, "_",2)[,1]
tram_match$school_net <- str_split_fixed(tram_match$SCHOOL_TRAM_ID, "_",2)[,1]

tram_match$cross_net <- ifelse(tram_match$pupil_net != tram_match$school_net,1,0)                    
tram_match$outlier_tram <- ifelse(tram_match$pupil_net != tram_match$school_net,1,tram_match$outlier_tram)                    







#Remove records detected as outliers and add these to a new error dataset - also remove added variables and reorder to match all_LA
error_tram <- tram_match[tram_match$outlier_tram == 1,]
error_tram <- subset(error_tram,select=colnames(all_LA))
tram_match <- tram_match[tram_match$outlier_tram == 0,]



#identify tram journeys where the distance to the tram stop is greater than the distance to the school

tram_match$outlier_tram[tram_match$sl_distance < tram_match$distance_station] <- 1

tram_error_tmp <- subset(tram_match, outlier_tram ==1)
tram_error_tmp<- subset(tram_error_tmp,select=colnames(all_LA))
error_tram <- rbind(error_tram,tram_error_tmp)

tram_match <- subset(tram_match, outlier_tram ==0)


#Cleanup
keep(NSPD,all_LA,rail,tram,tram_match,error_tram,tube,sure=TRUE)


#################################
#  RRRRRR    AAA   IIIII LL      
#  RR   RR  AAAAA   III  LL      
#  RRRRRR  AA   AA  III  LL      
#  RR  RR  AAAAAAA  III  LL      
#  RR   RR AA   AA IIIII LLLLLLL 
#################################

##########
#RAIL >>> Pupils
##########


#Create pupil / Tram location objects

pupil_rail_xy <- as.matrix(rail[,c("pupil_East","pupil_North")])

rail_locations <- readOGR("Rail", "station_point")

rail_rail_xy <- as.matrix(cbind(rail_locations@data$x,rail_locations@data$y))

#Manual intervention to prevent an issue with an equidistant point
pupil_rail_xy[27269,1] <- pupil_rail_xy[27269,1] +1

#Nearest Neighbour - Find closest tram stop to a pupil home
nn = get.knnx(rail_rail_xy,pupil_rail_xy,1) #Nearest Neighbour
colnames(rail_locations@data) <- c("","","","rail_x_pupil","rail_y_pupil","PUPIL_RAIL_ID")
rail_locations@data <- rail_locations@data[,c("PUPIL_RAIL_ID","rail_x_pupil","rail_y_pupil")]
rail_match <- cbind(rail,rail_locations@data[nn$nn.index,]) #Append Nearest Neighbour
rail_match <- cbind(rail_match,(nn$nn.dist/1000)) #Append Distance to Nearest Neighbour
colnames(rail_match)[39] <- "distance_station"





##########
#Rail >>> Schools
##########


#Get a table of unique schools and their locations from the tram_match data
schools <- sqldf("select LAEstab_SPR11, school_East, school_North from rail_match group by LAEstab_SPR11, school_East, school_North ")
school_rail_xy <- as.matrix(schools[,c("school_East","school_North")])


#Nearest Neighbour - Find closest tram stop to a school
nn = get.knnx(rail_rail_xy,school_rail_xy,1) #Nearest Neighbour
colnames(rail_locations@data) <- c("SCHOOL_RAIL_ID","rail_x_school","rail_y_school")
rail_school_match <- cbind(schools,rail_locations@data[nn$nn.index,]) #Append Nearest Neighbour
rail_school_match <- subset(rail_school_match,select=c("LAEstab_SPR11","SCHOOL_RAIL_ID"))
                                                       


#Join schools back onto tram_match
rail_match <- merge(rail_match, rail_school_match, by="LAEstab_SPR11", all.x=TRUE)




#remove those tram journeys where the distance to the railway station is greater than the distance to the school

rail_match$outlier_rail <- ifelse(rail_match$sl_distance < rail_match$distance_station,1,0)



#Remove records detected as outliers and add these to a new error dataset - also remove added variables and reorder to match all_LA
error_rail <- rail_match[rail_match$outlier_rail == 1,]
error_rail <- subset(error_rail,select=colnames(all_LA))
rail_match <- rail_match[rail_match$outlier_rail == 0,]




#Cleanup
keep(NSPD,all_LA,rail,tram,tram_match,error_tram,tube,rail_match,error_rail,sure=TRUE)





#  TTTTTTT UU   UU BBBBB   EEEEEEE 
#    TTT   UU   UU BB   B  EE      
#    TTT   UU   UU BBBBBB  EEEEE   
#    TTT   UU   UU BB   BB EE      
#    TTT    UUUUU  BBBBBB  EEEEEEE 
#




##########
#TUBE >>> Pupils
##########


#Create pupil / Tube location objects

pupil_tube_xy <- as.matrix(tube[,c("pupil_East","pupil_North")])

tube_locations <- readOGR("Rail", "tube_stations")

tube_tube_xy <- as.matrix(cbind(tube_locations@data$x,tube_locations@data$y))



#Nearest Neighbour - Find closest tube stop to a pupil home
nn = get.knnx(tube_tube_xy,pupil_tube_xy,1) #Nearest Neighbour
colnames(tube_locations@data) <- c("tube_x_pupil","tube_y_pupil","PUPIL_TUBE_ID")
tube_locations@data <- tube_locations@data[,c("PUPIL_TUBE_ID","tube_x_pupil","tube_y_pupil")]
tube_match <- cbind(tube,tube_locations@data[nn$nn.index,]) #Append Nearest Neighbour
tube_match <- cbind(tube_match,(nn$nn.dist/1000)) #Append Distance to Nearest Neighbour
colnames(tube_match)[39] <- "distance_station"




#Check the data for outliers caused by an error in mode choice

#Calculate Quartiles and IQR
tube_match$all <- "all"
Quartiles_tube <- summaryBy(distance_station~all, data = tube_match, 
                            FUN = function(x) { c(IQR = IQR(x,na.rm=TRUE, type= 7),m = quantile(x,na.rm=TRUE)) } )

colnames(Quartiles_tube) <- c("network","IQR","","Q1","","Q3","")
Quartiles_tube <- subset(Quartiles_tube,  select=c(network, IQR, Q1, Q3))


Q1 <- as.numeric(Quartiles_tube["Q1"])
Q3 <- as.numeric(Quartiles_tube["Q3"])
IQR <- as.numeric(Quartiles_tube["IQR"])

attach(tube_match)
tube_match$outlier_tube <- ifelse((distance_station < (Q1 - (1.5 * IQR)) | distance_station > (Q3 + (1.5 * IQR))),1,0)                    
detach(tube_match)







##########
#Tube >>> Schools
##########


#Get a table of unique schools and their locations from the tube_match data
detach("package:RPostgreSQL")
schools <- sqldf("select LAEstab_SPR11, school_East, school_North from tube_match group by LAEstab_SPR11, school_East, school_North ")
school_tube_xy <- as.matrix(schools[,c("school_East","school_North")])


#Nearest Neighbour - Find closest tram stop to a school
nn = get.knnx(tube_tube_xy,school_tube_xy,1) #Nearest Neighbour
colnames(tube_locations@data) <- c("SCHOOL_TUBE_ID","tube_x_school","tube_y_school")
tube_school_match <- cbind(schools,tube_locations@data[nn$nn.index,]) #Append Nearest Neighbour
tube_school_match <- subset(tube_school_match,select=c("LAEstab_SPR11","SCHOOL_TUBE_ID","tube_x_school","tube_y_school"))
tube_school_match <- cbind(tube_school_match,(nn$nn.dist/1000)) #Append Distance to Nearest Neighbour

colnames(tube_school_match)[5] <- "distance_station"

tube_school_match$network <- "tube"


#Calculate Quartiles and IQR
Quartiles_SL_to_school_tube <- summaryBy(distance_station ~ network, data = tube_school_match, 
                                         FUN = function(x) { c(IQR = IQR(x,na.rm=TRUE, type= 7),m = quantile(x,na.rm=TRUE)) } )

colnames(Quartiles_SL_to_school_tube) <- c("network","school_IQR","","school_Q1","","school_Q3","")
Quartiles_SL_to_school_tube <- subset(Quartiles_SL_to_school_tube,  select=c(network, school_IQR, school_Q1, school_Q3))

Q1 <- as.numeric(Quartiles_SL_to_school_tube$school_Q1)
Q3 <- as.numeric(Quartiles_SL_to_school_tube$school_Q3)
IQR <- as.numeric(Quartiles_SL_to_school_tube$school_IQR)


attach(tube_school_match)
tube_school_match$outlier_school_tube <- ifelse((distance_station < (Q1 - (1.5 * IQR)) | distance_station > (Q3 + (1.5 * IQR))),1,0)                    
detach(tube_school_match)



tube_school_match <- subset(tube_school_match,select=c('LAEstab_SPR11','outlier_school_tube','SCHOOL_TUBE_ID'))



#Join schools back onto tram_match & add an outlier flag to the pupil record if the school is considered too far from a station to be viable
tube_match <- merge(tube_match, tube_school_match, by="LAEstab_SPR11", all.x=TRUE)

tube_match$outlier_tube <- ifelse(tube_match$outlier_school_tube == 1,1,tube_match$outlier_tube)                    



#Remove records detected as outliers and add these to a new error dataset - also remove added variables and reorder to match all_LA
error_tube <- tube_match[tube_match$outlier_tube == 1,]
error_tube <- subset(error_tube,select=colnames(all_LA))
tube_match <- tube_match[tube_match$outlier_tube == 0,]



#remove those tube journeys where the distance to the tube station is greater than the distance to the school

tube_match$outlier_tube[tube_match$sl_distance < tube_match$distance_station] <- 1

tube_error_tmp <- subset(tube_match, outlier_tube ==1)
tube_error_tmp<- subset(tube_error_tmp,select=colnames(all_LA))
error_tube <- rbind(error_tube,tube_error_tmp)

tube_match <- subset(tube_match, outlier_tube ==0)






#Cleanup
keep(NSPD,all_LA,rail,tram,tram_match,error_tram,tube,rail_match,error_rail,tube_match,error_tube,sure=TRUE)


#################################################
# Calculate network distances for TUBE

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv,  dbname="school_carbon", user="postgres", password="") 

tube_out <- NULL

for (i in 1:nrow(tube_match)) {
  
  pupil_tube_id <- tube_match[i,'PUPIL_TUBE_ID']
  school_tube_id <- tube_match[i,'SCHOOL_TUBE_ID']
  row_id <- tube_match[i,'id']
  
  o_q <-  paste("SELECT tube_id.id FROM public.tube_id where tube_id.\"UID\" = '",pupil_tube_id,"';",sep="")
  d_q <-  paste("SELECT tube_id.id FROM public.tube_id where tube_id.\"UID\" = '",school_tube_id,"';",sep="")
  
  origin <- dbGetQuery(con, o_q)
  destination <- dbGetQuery(con, d_q)
  
  
  query <- paste("select * from shortest_path('select gid as id, start_id::int4 as source,end_id::int4 as target,shape_leng::float8 as cost from tube_network',",origin,",",destination,", false, false);", sep='')
  temp <- dbGetQuery(con, query)
  temp_d <- sum(temp$cost)
  
  temp_out <- cbind(row_id,temp_d)
  
  tube_out <- rbind(tube_out,temp_out)
  
  remove(list= c('pupil_tube_id','school_tube_id','o_q','d_q','origin','destination','query','temp','temp_d','temp_out'))
  
}

tube_out <- as.data.frame(tube_out)
colnames(tube_out) <- c("row_id","Net_dist")
tube_match <- sqldf("select tube_match.*,tube_out.Net_dist from tube_match left join tube_out where tube_match.id = tube_out.row_id")
remove(tube_out)
#############################





#################################################
# Calculate network distances for RAIL

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, host="localhost", dbname="school_carbon", user="postgres", password="") 

rail_out <- NULL


for (i in 1:nrow(rail_match)) {
  
  pupil_rail_id <- rail_match[i,'PUPIL_RAIL_ID']
  school_rail_id <- rail_match[i,'SCHOOL_RAIL_ID']
  row_id <- rail_match[i,'id']
  
  o_q <-  paste("SELECT railway_id.id FROM public.railway_id where railway_id.\"UID\" = '",pupil_rail_id,"';",sep="")
  d_q <-  paste("SELECT railway_id.id FROM public.railway_id where railway_id.\"UID\" = '",school_rail_id,"';",sep="")
  
  origin <- dbGetQuery(con, o_q)
  destination <- dbGetQuery(con, d_q)
  
  
  query <- paste("select * from shortest_path('select gid as id, start_id::int4 as source,end_id::int4 as target,shape_leng::float8 as cost from railway_network',",origin,",",destination,", false, false);", sep='')
  temp <- dbGetQuery(con, query)
  temp_d <- sum(temp$cost)
  
  temp_out <- cbind(row_id,temp_d)
  
  rail_out <- rbind(rail_out,temp_out)
  
  remove(list= c('pupil_rail_id','school_rail_id','o_q','d_q','origin','destination','query','temp','temp_d','temp_out'))
  
}
detach("package:RPostgreSQL")

rail_out <- as.data.frame(rail_out)
colnames(rail_out) <- c("row_id","Net_dist")

rail_match<- sqldf("select rail_match.*,rail_out.Net_dist from rail_match left join rail_out where rail_match.id = rail_out.row_id")
remove(rail_out)
#############################





#################################################
# Calculate network distances for TRAM
library("RPostgreSQL")

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, host="localhost", dbname="school_carbon", user="postgres", password="") 

tram_out <- NULL


for (i in 1:nrow(tram_match)) {
  
  pupil_tram_id <- tram_match[i,'PUPIL_TRAM_ID']
  school_tram_id <- tram_match[i,'SCHOOL_TRAM_ID']
  row_id <- tram_match[i,'id']
  network <- tram_match[i,'pupil_net']#both pupil_net and school_net should be the same as differences are excluded
  
  
  o_q <-  paste("SELECT ",network,"_id.id FROM public.",network,"_id where ",network,"_id.\"UID\" = '",pupil_tram_id,"';",sep="")
  d_q <-  paste("SELECT ",network,"_id.id FROM public.",network,"_id where ",network,"_id.\"UID\" = '",school_tram_id,"';",sep="")
  
  origin <- dbGetQuery(con, o_q)
  destination <- dbGetQuery(con, d_q)
  
  
  query <- paste("select * from shortest_path('select gid as id, start_id::int4 as source,end_id::int4 as target,shape_leng::float8 as cost from ",network,"_network',",origin,",",destination,", false, false);", sep='')
  temp <- dbGetQuery(con, query)
  temp_d <- sum(temp$cost)
  
  temp_out <- cbind(row_id,temp_d)
  
  tram_out <- rbind(tram_out,temp_out)
  
  remove(list= c('pupil_tram_id','school_tram_id','o_q','d_q','origin','destination','query','temp','temp_d','temp_out','network'))
  print(i)  
}
detach("package:RPostgreSQL")

tram_out <- as.data.frame(tram_out)
colnames(tram_out) <- c("row_id","Net_dist")

tram_match<- sqldf("select tram_match.*,tram_out.Net_dist from tram_match left join tram_out where tram_match.id = tram_out.row_id")
remove(tram_out)
#############################




#Create cutdown dataset for merge back onto the original data
#Error
error_rail_cutdown <- cbind(subset(error_rail, select = c('id')),"E")
error_tube_cutdown <- cbind(subset(error_tube, select = c('id')),"E")
error_tram_cutdown <- cbind(subset(error_tram, select = c('id')),"E")
error_all_cutdown <- rbind(error_rail_cutdown,error_tube_cutdown,error_tram_cutdown)
error_all_cutdown <- cbind(error_all_cutdown,NA)
colnames(error_all_cutdown) <- c('id','type','net_distance')
error_all_cutdown$network_type <- "error"

#Non Error
rail_match_cutdown <- cbind(subset(rail_match, select = c('id','Net_dist')),"OK")
tube_match_cutdown <- cbind(subset(tube_match, select = c('id','Net_dist')),"OK")
tram_match_cutdown <- cbind(subset(tram_match, select = c('id','Net_dist','pupil_net')),"OK")
rail_match_cutdown$pupil_net <- "rail"
tube_match_cutdown$pupil_net <- "tube"

all_match_cutdown <- rbind(rail_match_cutdown,tube_match_cutdown,tram_match_cutdown)

colnames(all_match_cutdown) <- c('id','net_distance','type','network_type')

all_match_cutdown <- subset(all_match_cutdown,select=c('id', 'net_distance',	'type',	'network_type'))
error_all_cutdown <- subset(error_all_cutdown,select=c('id', 'net_distance',  'type',	'network_type'))


final_net_distances <- rbind(all_match_cutdown,error_all_cutdown)
colnames(final_net_distances) <- c('id','net_distance','e_or_ok','network_type')



#Remove routino distances where these are for non road transport

all_LA$distance[all_LA$Mode %in% c("TRN","LUL","MTL")] <- NA



#Remove routino distances where SL was calculated - we assume these are errors
all_LA$distance[all_LA$type =="SL"] <- NA



#Big merge!!!


rownames(final_net_distances) <- NULL
all_LA2 <- merge(all_LA, final_net_distances, by='id', all.x=TRUE)


all_LA2$final_distance <- ifelse(all_LA2$Mode %in% c('TRN','MTL','LUL'), all_LA2$net_distance, all_LA2$distance)
all_LA2$final_type <- ifelse(all_LA2$Mode %in% c('TRN','MTL','LUL'), all_LA2$network_type, as.character(all_LA2$type))

all_LA2$LSOA.x <- NULL


#Add an error column

#Remove list - #no east or north NEN; No routino route returned ERO; road RD; rail RA; error rail - e.g. station dist RA;   

all_LA2$excludes[all_LA2$final_type == "MV" | (is.na(all_LA2$pupil_East) | is.na(all_LA2$pupil_North) | is.na(all_LA2$school_East) | is.na(all_LA2$school_North))] <- "NEN"

all_LA2$excludes[all_LA2$final_type %in% c("R","SP")] <- "RD"

all_LA2$excludes[all_LA2$final_type =="SL"] <- "ERO"

all_LA2$excludes[all_LA2$final_type =="error"] <- "ERA"

all_LA2$excludes[all_LA2$final_type %in% c("blackpool","croydon","docklands","manchester","midland","newcastle","nottingham","rail","sheffield","tube")] <- "RA"

  
  
#Write out the distances to PostgresDB

library("RPostgreSQL")

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, host="localhost", dbname="school_carbon", user="postgres", password="") 

dbWriteTable(con, 'Spring_Census_2011_WD', all_LA2)

dbDisconnect(con)





#?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?#
#Outlier detection
#?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?#




#Set working directory
#setwd("/Volumes/Macintosh HD 2/Dropbox/Projects/School_Carbon/Final/")#work
setwd("/Users/alex/Dropbox/Projects/School_Carbon/Final")#laptop






#Get data from DB
library(RPostgreSQL)


drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, host="localhost", dbname="school_carbon", user="postgres", password="") 


query <- "
SELECT 
\"Spring_Census_2011_WD\".\"id\",
\"Spring_Census_2011_WD\".\"Mode\", 
  \"Spring_Census_2011_WD\".\"URN_SPR11\",
  \"Spring_Census_2011_WD\".\"LAEstab_SPR11\",
  \"Spring_Census_2011_WD\".\"LA..code.\",
  \"Spring_Census_2011_WD\".\"NCyearActual_SPR11\",
  \"Spring_Census_2011_WD\".\"PCD\", 
  \"Spring_Census_2011_WD\".\"EthnicGroupMajor_SPR11\", 
  \"Spring_Census_2011_WD\".acorntype, 
  \"Spring_Census_2011_WD\".\"TypeOfEstablishment..name.\", 
  \"Spring_Census_2011_WD\".\"IMD_2010_D\", 
  \"Spring_Census_2011_WD\".\"IMD_2010_R\", 
  \"Spring_Census_2011_WD\".\"CASWARD\",
  \"Spring_Census_2011_WD\".\"LSOA\",
  \"Spring_Census_2011_WD\".\"PhaseOfEducation..name.\", 
  \"Spring_Census_2011_WD\".final_distance, 
  \"Spring_Census_2011_WD\".final_type,
  \"Spring_Census_2011_WD\".network_type,
  \"Spring_Census_2011_WD\".excludes,
  \"Spring_Census_2011_WD\".sl_distance
FROM 
public.\"Spring_Census_2011_WD\"
WHERE 
\"Spring_Census_2011_WD\".final_distance IS NOT NULL  AND 
  \"Spring_Census_2011_WD\".\"Mode\" IS NOT NULL ;
"


ed_data <- dbGetQuery(con, query)




##########################
#########Data Prep########
##########################

#Recode Mode of Travel into courser groupings

attach(ed_data)

ed_data$Mode2[Mode == "PSB" | Mode == "DSB" | Mode == "BNK"] <- "BUS"
ed_data$Mode2[Mode == "TRN" | Mode == "LUL" | Mode == "MTL"] <- "TRA"
ed_data$Mode2[Mode == "WLK" | Mode == "CYC" ] <- "NON"
ed_data$Mode2[Mode == "CRS" | Mode == "CAR" | Mode == "TXI"] <- "CAR"
ed_data$Mode2[Mode == "BDR" ] <- "BDR"
ed_data$Mode2[Mode == "OTH" ] <- "OTH"

detach(ed_data)


#Create a lookup for outliers

ed_data$Qlookup <- paste(ed_data$LA..code., ed_data$NCyearActual_SPR11, ed_data$Mode2, sep = "")




##########################
###Cleaning Procedures####
##########################


# Identify Outliers based on Travel Distance



#Calculate Quartiles and IQR
Quartiles <- summaryBy(final_distance ~ LA..code. + NCyearActual_SPR11  + Mode2, data = ed_data, 
                       FUN = function(x) { c(IQR = IQR(x,na.rm=TRUE, type= 7),m = quantile(x,na.rm=TRUE)) } )
colnames(Quartiles) <- c("LA..code.","NCyearActual_SPR11","Mode2","IQR","","Q1","","Q3","")
Quartiles <- subset(Quartiles,  select=c(LA..code., NCyearActual_SPR11, Mode2,  IQR, Q1, Q3))
Quartiles$Qlookup <- paste(Quartiles$LA..code., Quartiles$NCyearActual_SPR11, Quartiles$Mode2, sep = "")
Quartiles <- subset(Quartiles,  select=c(Qlookup,  IQR, Q1, Q3))


#Merge Results back onto data frome x

ed_data <- merge(ed_data,Quartiles, by= "Qlookup", all.x=TRUE)


#Outlier calc
attach(ed_data)

ed_data$outlier <- ifelse((final_distance < (Q1 - (1.5 * IQR)) | final_distance > (Q3 + (1.5 * IQR))),1,0)                    

detach(ed_data)



# 
# summary(ed_data$final_distance[ed_data$outlier==0 & !ed_data$TypeOfEstablishment..name. %in% c('Community Special School',' Foundation Special School','Non-Maintained Special School')])
# table(ed_data$outlier)
# 
# 
# table(ed_data$TypeOfEstablishment..name.)

################Trim out remaining junk##########


ed_data$IQR <- NULL
ed_data$Q1 <- NULL
ed_data$Q3 <- NULL




####Write the results of outlier detection back to DB#######

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, host="localhost", dbname="school_carbon", user="postgres", password="") 

dbWriteTable(con, 'Spring_Census_2011_WD_NOutlier', ed_data)

dbDisconnect(con)


###################################
###Create CO2 Estimates for Cars###
###################################


#Get Cars Data


drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, host="localhost", dbname="school_carbon", user="postgres", password="") 

query <- "

SELECT 
\"Spring_Census_2011_WD_NOutlier\".\"id\",
\"Spring_Census_2011_WD_NOutlier\".\"URN_SPR11\", 
  \"Spring_Census_2011_WD_NOutlier\".\"LAEstab_SPR11\", 
  \"Spring_Census_2011_WD_NOutlier\".\"LA..code.\", 
  \"Spring_Census_2011_WD_NOutlier\".\"NCyearActual_SPR11\", 
  \"Spring_Census_2011_WD_NOutlier\".\"LSOA\", 
  \"Spring_Census_2011_WD_NOutlier\".\"CASWARD\",
  \"Spring_Census_2011_WD_NOutlier\".\"EthnicGroupMajor_SPR11\", 
  \"Spring_Census_2011_WD_NOutlier\".acorntype, 
  \"Spring_Census_2011_WD_NOutlier\".\"TypeOfEstablishment..name.\", 
  \"Spring_Census_2011_WD_NOutlier\".\"IMD_2010_D\", 
  \"Spring_Census_2011_WD_NOutlier\".\"IMD_2010_R\", 
  \"Spring_Census_2011_WD_NOutlier\".\"PhaseOfEducation..name.\", 
  \"Spring_Census_2011_WD_NOutlier\".sl_distance, 
  \"Spring_Census_2011_WD_NOutlier\".final_distance,
  \"Spring_Census_2011_WD_NOutlier\".\"Mode\", 
  \"Spring_Census_2011_WD_NOutlier\".outlier
FROM 
public.\"Spring_Census_2011_WD_NOutlier\"
WHERE 
\"Spring_Census_2011_WD_NOutlier\".outlier = 0 AND 
  \"Spring_Census_2011_WD_NOutlier\".\"Mode2\" = 'CAR';
"

cars <- dbGetQuery(con, query)



#Append CO2 Data


LSOA_CO2 <-  read.csv("CO2 Data/120314-Cars by LSOA 2011Q2-Singleton-DL.csv") # grm / KM

cars <- merge(cars, LSOA_CO2, by.x='LSOA', by.y='LSOA', all.x=TRUE)

#calculate CO2 model for cars

cars$co2g_route <- ifelse(cars$Mode == "CAR", cars$final_distance * cars$AvgCO2,(ifelse(cars$Mode == "CRS",( cars$final_distance * cars$AvgCO2)/2,(ifelse(cars$Mode == "TXI",cars$final_distance * 150.3,NA)))))



# Car shared are assumed as two pupils ; Taxi Emissions detailed P24: http://www.defra.gov.uk/publications/files/pb13625-emission-factor-methodology-paper-110905.pdf


###################################
###Create CO2 Estimates for Bus###
###################################


#Get Bus Data
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, host="localhost", dbname="school_carbon", user="postgres", password="") 

query <- "

SELECT 
\"Spring_Census_2011_WD_NOutlier\".\"id\",
\"Spring_Census_2011_WD_NOutlier\".\"URN_SPR11\", 
  \"Spring_Census_2011_WD_NOutlier\".\"LAEstab_SPR11\", 
  \"Spring_Census_2011_WD_NOutlier\".\"LA..code.\", 
  \"Spring_Census_2011_WD_NOutlier\".\"NCyearActual_SPR11\", 
  \"Spring_Census_2011_WD_NOutlier\".\"LSOA\", 
  \"Spring_Census_2011_WD_NOutlier\".\"CASWARD\",
  \"Spring_Census_2011_WD_NOutlier\".\"EthnicGroupMajor_SPR11\", 
  \"Spring_Census_2011_WD_NOutlier\".acorntype, 
  \"Spring_Census_2011_WD_NOutlier\".\"TypeOfEstablishment..name.\", 
  \"Spring_Census_2011_WD_NOutlier\".\"IMD_2010_D\", 
  \"Spring_Census_2011_WD_NOutlier\".\"IMD_2010_R\", 
  \"Spring_Census_2011_WD_NOutlier\".\"PhaseOfEducation..name.\", 
  \"Spring_Census_2011_WD_NOutlier\".sl_distance, 
  \"Spring_Census_2011_WD_NOutlier\".final_distance, 
  \"Spring_Census_2011_WD_NOutlier\".\"Mode\", 
  \"Spring_Census_2011_WD_NOutlier\".outlier
FROM 
public.\"Spring_Census_2011_WD_NOutlier\"
WHERE 
\"Spring_Census_2011_WD_NOutlier\".outlier = 0 AND 
  \"Spring_Census_2011_WD_NOutlier\".\"Mode2\" = 'BUS';
"

bus <- dbGetQuery(con, query)




#Identify those trips made within London

bus$London <- ifelse(substr(bus$LAEstab_SPR11,1,3) %in% c('201', '202', '203', '204', '205', '206', '207', '208', '209', '210', '211', '212', '213', '301', '302', '303', '304', '305', '306', '307', '308', '309', '310', '311', '312', '313', '314', '315', '316', '317', '318', '319', '320'),1 ,0)



# Average occupancy of a bus is 11
#http://assets.dft.gov.uk/statistics/releases/light-rail-tram-statistics-2011-12/lrt-2011-12.pdf
# however, differences for London: http://www.defra.gov.uk/publications/files/pb13625-emission-factor-methodology-paper-110905.pdf (P31)
# In London - 16.7 ; and non london 6.3 ; also - coach is 16.2. Thus, the gCO2 are:
#Non London Bus - 184.3 ; London Bus - 85.7 ; Coach (which we will assume is a dedicated school bus) 30;

#ANational Travel Survey: 2010 http://www.dft.gov.uk/statistics/releases/national-travel-survey-2010/ states that 85% of households in Great Britain lived within a 6 minute walk of a bus stop. Average walking speed in children is around 70 meters / minute (http://onlinelibrary.wiley.com/doi/10.1002/jor.1100060208/abstract) - thus, a average walk to the bus stop would be around 420m



#calculate CO2 model for bus
bus$co2g_route <- ifelse(((bus$Mode == "PSB" | bus$Mode == "BNK") & (bus$London == 1)),bus$final_distance * 85.7,ifelse(((bus$Mode == "PSB" | bus$Mode == "BNK") & (bus$London == 0)),bus$final_distance * 184.3,ifelse(bus$Mode == "DSB",bus$final_distance * 30,NA)))

bus$London <- NULL






###################################
###Create CO2 Estimates for Rail### some infor - http://assets.dft.gov.uk/statistics/releases/light-rail-tram-statistics-2011-12/lrt-2011-12.pdf
###################################

#Estimates from http://www.defra.gov.uk/publications/files/pb13625-emission-factor-methodology-paper-110905.pdf P35
# Grm CO2 / person KM
#DLR - 68.3 ; Birmingham - 70.5 ; Newcastle - 103.0 ; Croydon - 44.3 ; Manchester  39.5 ; Nottingham (no data - so use national average) 71 ; Sheffield 96.8 ; 

#National rail - 53.4 ; 
#London Underground - 73.1;


drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, host="localhost", dbname="school_carbon", user="postgres", password="") 

query <- "

SELECT 
\"Spring_Census_2011_WD_NOutlier\".\"id\",
\"Spring_Census_2011_WD_NOutlier\".\"URN_SPR11\", 
  \"Spring_Census_2011_WD_NOutlier\".\"LAEstab_SPR11\", 
  \"Spring_Census_2011_WD_NOutlier\".\"LA..code.\", 
  \"Spring_Census_2011_WD_NOutlier\".\"NCyearActual_SPR11\", 
  \"Spring_Census_2011_WD_NOutlier\".\"LSOA\", 
  \"Spring_Census_2011_WD_NOutlier\".\"CASWARD\",
  \"Spring_Census_2011_WD_NOutlier\".\"EthnicGroupMajor_SPR11\", 
  \"Spring_Census_2011_WD_NOutlier\".acorntype, 
  \"Spring_Census_2011_WD_NOutlier\".\"TypeOfEstablishment..name.\", 
  \"Spring_Census_2011_WD_NOutlier\".\"IMD_2010_D\", 
  \"Spring_Census_2011_WD_NOutlier\".\"IMD_2010_R\", 
  \"Spring_Census_2011_WD_NOutlier\".\"PhaseOfEducation..name.\", 
  \"Spring_Census_2011_WD_NOutlier\".sl_distance, 
  \"Spring_Census_2011_WD_NOutlier\".final_distance, 
  \"Spring_Census_2011_WD_NOutlier\".\"Mode\", 
  \"Spring_Census_2011_WD_NOutlier\".\"network_type\",
  \"Spring_Census_2011_WD_NOutlier\".outlier
FROM 
public.\"Spring_Census_2011_WD_NOutlier\"
WHERE 
\"Spring_Census_2011_WD_NOutlier\".outlier = 0 AND 
  \"Spring_Census_2011_WD_NOutlier\".\"Mode2\" = 'TRA';
"

rail <- dbGetQuery(con, query)




# 
# rail$LA <- substr(rail$LAEstab_SPR11,1,3)
# 
# rail$London <- ifelse(rail$LA %in% c('201', '202', '203', '204', '205', '206', '207', '208', '209', '210', '211', '212', '213', '301', '302', '303', '304', '305', '306', '307', '308', '309', '310', '311', '312', '313', '314', '315', '316', '317', '318', '319', '320'),1 ,0)

#light rail
#DLR - 68.3 ; Birmingham - 70.5 ; Newcastle - 103.0 ; Croydon - 44.3 ; Manchester  39.5 ; Nottingham (no data - so use national average) 71 ; Sheffield 96.8 ; 

#National rail - 53.4 ; 
#London Underground - 73.1;

rail$CO2g_passenger_km <- NA

#Change values for Train
rail$CO2g_passenger_km <- ifelse(rail$Mode == "TRN",53.4,rail$CO2g_passenger_km) 

#Change values for underground
rail$CO2g_passenger_km <- ifelse(rail$Mode == "LUL",73.1,rail$CO2g_passenger_km) 

#Trams

rail$CO2g_passenger_km <- ifelse(rail$Mode == "MTL" & rail$network_type == "blackpool",71,rail$CO2g_passenger_km) 
rail$CO2g_passenger_km <- ifelse(rail$Mode == "MTL" & rail$network_type == "croydon",44.3,rail$CO2g_passenger_km)
rail$CO2g_passenger_km <- ifelse(rail$Mode == "MTL" & rail$network_type == "docklands",68.3,rail$CO2g_passenger_km)
rail$CO2g_passenger_km <- ifelse(rail$Mode == "MTL" & rail$network_type == "manchester",39.5,rail$CO2g_passenger_km)
rail$CO2g_passenger_km <- ifelse(rail$Mode == "MTL" & rail$network_type == "midland",70.5,rail$CO2g_passenger_km)
rail$CO2g_passenger_km <- ifelse(rail$Mode == "MTL" & rail$network_type == "newcastle",103,rail$CO2g_passenger_km)
rail$CO2g_passenger_km <- ifelse(rail$Mode == "MTL" & rail$network_type == "nottingham",71,rail$CO2g_passenger_km)
rail$CO2g_passenger_km <- ifelse(rail$Mode == "MTL" & rail$network_type == "sheffield",96.8,rail$CO2g_passenger_km)

#CO2 Calculation
rail$co2g_route <- rail$final_distance * rail$CO2g_passenger_km





###########################################
###Create CO2 Estimates for Walk / Cycle###
###########################################
#http://www.sciencedirect.com/science/article/pii/S0301421501000611 - 11.4 (walking) ; 8.3 (cycling)

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, host="localhost", dbname="school_carbon", user="postgres", password="") 

query <- "

SELECT 
\"Spring_Census_2011_WD_NOutlier\".\"id\",
\"Spring_Census_2011_WD_NOutlier\".\"URN_SPR11\", 
  \"Spring_Census_2011_WD_NOutlier\".\"LAEstab_SPR11\", 
  \"Spring_Census_2011_WD_NOutlier\".\"LA..code.\", 
  \"Spring_Census_2011_WD_NOutlier\".\"NCyearActual_SPR11\", 
  \"Spring_Census_2011_WD_NOutlier\".\"LSOA\", 
  \"Spring_Census_2011_WD_NOutlier\".\"CASWARD\",
  \"Spring_Census_2011_WD_NOutlier\".\"EthnicGroupMajor_SPR11\", 
  \"Spring_Census_2011_WD_NOutlier\".acorntype, 
  \"Spring_Census_2011_WD_NOutlier\".\"TypeOfEstablishment..name.\", 
  \"Spring_Census_2011_WD_NOutlier\".\"IMD_2010_D\", 
  \"Spring_Census_2011_WD_NOutlier\".\"IMD_2010_R\", 
  \"Spring_Census_2011_WD_NOutlier\".\"PhaseOfEducation..name.\", 
  \"Spring_Census_2011_WD_NOutlier\".sl_distance, 
  \"Spring_Census_2011_WD_NOutlier\".final_distance, 
  \"Spring_Census_2011_WD_NOutlier\".\"Mode\", 
  \"Spring_Census_2011_WD_NOutlier\".outlier
FROM 
public.\"Spring_Census_2011_WD_NOutlier\"
WHERE 
\"Spring_Census_2011_WD_NOutlier\".outlier = 0 AND 
  \"Spring_Census_2011_WD_NOutlier\".\"Mode2\" = 'NON';
"

walk <- dbGetQuery(con, query)





walk$co2g_route <- ifelse(walk$Mode == "WLK", walk$final_distance * 11.4, ifelse(walk$Mode == "CYC",walk$final_distance * 8.3,NA))






###################################################
########Cut down files and output to DB#############
###################################################




bus <- subset(bus, select = c(id,co2g_route))
rail <- subset(rail, select = c(id,co2g_route))
cars <- subset(cars, select = c(id,co2g_route))
walk <- subset(walk, select = c(id,co2g_route))

output <- rbind (bus,rail,cars,walk)


#Get the full data from DB 

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, host="localhost", dbname="school_carbon", user="postgres", password="") 

query <- "

SELECT *
FROM 
public.\"Spring_Census_2011_WD_NOutlier\";
"

Spring_Census_2011_WD_NOutlier <- dbGetQuery(con, query)



#Create final output to DB



Spring_Census_2011_WD_NOutlier_CO2 <- merge(Spring_Census_2011_WD_NOutlier, output, by = "id", all.x=TRUE)

Spring_Census_2011_WD_NOutlier_CO2$row.names  <- NULL






#

library("RPostgreSQL")

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, host="localhost", dbname="school_carbon", user="postgres", password="") 

dbWriteTable(con, 'Spring_Census_2011_WD_NOutlier_CO2', Spring_Census_2011_WD_NOutlier_CO2)

dbDisconnect(con)



#?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?#
#DESCRIPTIVE ANALYSIS
#?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?##?#



library(gmodels)
library(rgdal)
library(gdata)
library(maptools)
library(GISTools)

#Get Data
library("RPostgreSQL")
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, host="localhost", dbname="school_carbon", user="postgres", password="") 
Spring_Census_2011_WD_NOutlier_CO2 <- dbGetQuery(con, "SELECT * FROM public.\"Spring_Census_2011_WD_NOutlier_CO2\";")

#Create ID
id<- as.vector(seq(1:nrow(Spring_Census_2011_WD_NOutlier_CO2)))
Spring_Census_2011_WD_NOutlier_CO2<-cbind(Spring_Census_2011_WD_NOutlier_CO2,id)


#Remove records where there are outliers 
Spring_Census_2011_WD_NOutlier_CO2$row.names <- NULL
Spring_Census_2011_WD_NOutlier_CO2 <- subset(Spring_Census_2011_WD_NOutlier_CO2, Spring_Census_2011_WD_NOutlier_CO2$outlier == 0)

#Remove records which are BDR or OTH or NULL
Spring_Census_2011_WD_NOutlier_CO2 <- subset(Spring_Census_2011_WD_NOutlier_CO2, !Spring_Census_2011_WD_NOutlier_CO2$Mode %in% c("OTH","BDR"))
Spring_Census_2011_WD_NOutlier_CO2 <- subset(Spring_Census_2011_WD_NOutlier_CO2, !is.na(Spring_Census_2011_WD_NOutlier_CO2$Mode2)
)

#table(Spring_Census_2011_WD_NOutlier_CO2$outlier_binary,is.na(Spring_Census_2011_WD_NOutlier_CO2$final_distance2))


##############################
#Double CO2 results - account both to and from school;
##############################

Spring_Census_2011_WD_NOutlier_CO2$co2g_route_X2 <- Spring_Census_2011_WD_NOutlier_CO2$co2g_route*2



########
#Distance charts
#######

#Import Distances#
Distances <- read.csv("/Compare_Distances.csv")
colnames(Distances) <- c('row','KS4_PupilMatchingRefAnonymous','Google','Bing','Routino_Dist','Straight_Dist','Routino')

pdf(file='/Users/alex/Dropbox/Publications/Journals/Area/Distances.pdf')
plotmatrix(with(Distances, data.frame(Google, Bing, Routino)))
dev.off()


########
#External Validation - straight line aspatial model
#######


attach(Spring_Census_2011_WD_NOutlier_CO2)
Spring_Census_2011_WD_NOutlier_CO2$CO2_Non_Geo[Mode == "PSB" | Mode == "DSB" | Mode == "BNK"] <- 184.3
Spring_Census_2011_WD_NOutlier_CO2$CO2_Non_Geo[Mode == "TRN"] <- 53.4
Spring_Census_2011_WD_NOutlier_CO2$CO2_Non_Geo[Mode == "WLK"] <- 11.4
Spring_Census_2011_WD_NOutlier_CO2$CO2_Non_Geo[Mode == "LUL"] <- 73.1
Spring_Census_2011_WD_NOutlier_CO2$CO2_Non_Geo[Mode == "MTL"] <- 71
Spring_Census_2011_WD_NOutlier_CO2$CO2_Non_Geo[Mode == "CAR"] <- 207.6
Spring_Census_2011_WD_NOutlier_CO2$CO2_Non_Geo[Mode == "CRS" ] <- 207.6 * 0.5
Spring_Census_2011_WD_NOutlier_CO2$CO2_Non_Geo[Mode == "CYC" ] <- 8.3
Spring_Census_2011_WD_NOutlier_CO2$CO2_Non_Geo[Mode == "TXI" ] <- 150.3
detach(Spring_Census_2011_WD_NOutlier_CO2)

Spring_Census_2011_WD_NOutlier_CO2$co2g_route_non_geo <- Spring_Census_2011_WD_NOutlier_CO2$CO2_Non_Geo * Spring_Census_2011_WD_NOutlier_CO2$sl_distance * 2

#Get LA
temp <- tempfile(fileext='.zip')
download.file("http://dl.dropbox.com/u/881843/shapefiles/Districts_LA_UA/dist_bor_ua_2.zip",temp)
dist_bor_ua <- readOGR(unzip(temp), "dist_bor_ua_2")
unlink(temp)

#Get GOR
temp <- tempfile(fileext='.zip')
download.file("http://dl.dropbox.com/u/881843/shapefiles/GOR/England_gor_2001_clipped.zip",temp)
gor<- readOGR(unzip(temp), "England_gor_2001_clipped")
unlink(temp)
gor <- generalize.polys(gor, 100)


#Get LSOA Lookup
temp <- tempfile(fileext='.zip')
download.file("http://www.neighbourhood.statistics.gov.uk/HTMLDocs/images/OA_LSOA_MSOA_LAD_LU_newcodes_text_file_tcm97-102752.zip",temp)
LSOA_LA <- read.csv(unzip(temp))
unlink(temp)
LSOA_LA <- subset(LSOA_LA, select = c(LSOACD,LADCD))
LSOA_LA <- unique(LSOA_LA)
Spring_Census_2011_WD_NOutlier_CO2 <- merge(Spring_Census_2011_WD_NOutlier_CO2,LSOA_LA, by.x="LSOA",by.y="LSOACD",all.x=TRUE)


# #Create median and sum CO2 aggregation
options(sqldf.driver = "SQLite")
library(sqldf)
CO2_Local_Authority <- sqldf("select LADCD, count(*) as totpupils,
                             (sum(co2g_route_X2)/1000) as tot_CO2kg_km,
                             median(co2g_route_X2) as med_CO2g_km,
                            (sum(co2g_route_non_geo)/1000) as tot_CO2kg_km_NG,
                             median(co2g_route_non_geo) as med_CO2g_km_NG
                             from Spring_Census_2011_WD_NOutlier_CO2 group by LADCD")



CO2_Local_Authority$Diff <-  CO2_Local_Authority$tot_CO2kg_km_NG - CO2_Local_Authority$tot_CO2kg_km




#Merge onto spatial data frame
dist_bor_ua@data <- merge(dist_bor_ua,CO2_Local_Authority, by.x="LABEL",by.y="LADCD", all.x=TRUE)

#Map setup - differences
breaks <- classIntervals(dist_bor_ua@data$Diff,n=4,style="fisher")
shades <- shading(breaks$brks, cols = brewer.pal(6, "GnBu"))


pdf(file='/Users/alex/Dropbox/Publications/Journals/Area/Model_difference_map.pdf')

# Draw a map for sum
choropleth(dist_bor_ua, dist_bor_ua@data$Diff, shades,border = NA)
choro.legend(0, 542652.7, shades,fmt = "%3.0f", title = "Sum CO2kg",border=NA,bty="n")
plot(gor, border="#666666",bg=NA, add=TRUE)
dev.off()






#################################################################
#Compare mode choice and distance travelled
#################################################################


#Create bins
attach(Spring_Census_2011_WD_NOutlier_CO2)

Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 0 & final_distance <= 0.5] <- "a:0-0.5km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 0.5 & final_distance <= 1] <- "b:0.5-1km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 1 & final_distance <= 1.5] <- "c:1-1.5km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 1.5 & final_distance <= 2] <- "d:1.5-2km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 2 & final_distance <= 2.5] <- "e:2-2.5km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 2.5 & final_distance <= 3] <- "f:2.5-3km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 3 & final_distance <= 3.5] <- "g:3-3.5km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 3.5 & final_distance <= 4] <- "h:3.5-4km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 4 & final_distance <= 4.5] <- "i:4-4.5km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 4.5 & final_distance <= 5] <- "j:4.5-5km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 5 & final_distance <= 5.5] <- "k:5-5.5km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 5.5 & final_distance <= 6] <- "l:5.5-6km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 6 & final_distance <= 6.5] <- "m:6-6.5km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 6.5 & final_distance <= 7] <- "n:6.5-7km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 7 & final_distance <= 7.5] <- "o:7-7.5km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 7.5 & final_distance <= 8] <- "p:7.5-8km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 8 & final_distance <= 8.5] <- "q:8-8.5km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 8.5 & final_distance <= 9] <- "r:8.5-9km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 9 & final_distance <= 9.5] <- "s:9-9.5km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 9.5 & final_distance <= 10] <- "t:9.5-10km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 10 & final_distance <= 10.5] <- "u:10-10.5km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 10.5 & final_distance <= 11] <- "v:10.5-11km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 11 & final_distance <= 11.5] <- "w:11-11.5km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 11.5 & final_distance <= 12] <- "x:11.5-12km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 12 & final_distance <= 12.5] <- "y:12-12.5km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 12.5 & final_distance <= 13] <- "z:12.5-13km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 13 & final_distance <= 13.5] <- "za:13-13.5km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 13.5 & final_distance <= 14] <- "zb:13.5-14km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 14 & final_distance <= 14.5] <- "zc:14-14.5km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 14.5 & final_distance <= 15] <- "zd:14.5-15km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 15 & final_distance <= 15.5] <- "ze:15-15.5km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 15.5 & final_distance <= 16] <- "zf:15.5-16km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 16 & final_distance <= 16.5] <- "zg:16-16.5km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 16.5 & final_distance <= 17] <- "zh:16.5-17km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 17 & final_distance <= 17.5] <- "zi:17-17.5km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 17.5 & final_distance <= 18] <- "zk:17.5-18km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 18 & final_distance <= 18.5] <- "zl:18-18.5km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 18.5 & final_distance <= 19] <- "zm:18.5-19km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 19 & final_distance <= 19.5] <- "zn:19-19.5km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 19.5 & final_distance <= 20] <- "zo:19.5-20km"
Spring_Census_2011_WD_NOutlier_CO2$distance_bin[final_distance > 20 ] <- "zp:> 20km"

detach(Spring_Census_2011_WD_NOutlier_CO2)


# 
# #Label generator
# for (i in (seq(0,20,by=0.5))) {
#   
#   print(paste("Spring_Census_2011_WD_NOutlier_CO2_V2$distance_bin[final_distance2 > ",i," & final_distance2 <= ",i+0.5,"] <- \"",i,"-",i+0.5,"km","\"",sep=''))
#   
# }




#Geodemographics, IMD distance & CO2


#CO2 & school type


#Map of CO2


library(gmodels)
library(ggplot2)


#################
#Mode Choice Plot
#################
table <- CrossTable(Spring_Census_2011_WD_NOutlier_CO2$distance_bin,Spring_Census_2011_WD_NOutlier_CO2$Mode2,prop.c=FALSE,prop.r=TRUE, prop.t=FALSE,prop.chisq=FALSE,digits=2)
tmp <- as.data.frame(table$prop.row)
colnames(tmp) <- c('Distance','Mode','Frequency')
labs <- c('0-0.5km','0.5-1km','1-1.5km','1.5-2km','2-2.5km','2.5-3km','3-3.5km','3.5-4km','4-4.5km','4.5-5km','5-5.5km','5.5-6km','6-6.5km','6.5-7km','7-7.5km','7.5-8km','8-8.5km','8.5-9km','9-9.5km','9.5-10km','10-10.5km','10.5-11km','11-11.5km','11.5-12km','12-12.5km','12.5-13km','13-13.5km','13.5-14km','14-14.5km','14.5-15km','15-15.5km','15.5-16km','16-16.5km','16.5-17km','17-17.5km','17.5-18km','18-18.5km','18.5-19km','19-19.5km','19.5-20km','> 20km')
#all years + mode
ggplot(data=tmp, aes(x=Distance,y=Frequency, group=Mode, colour=Mode)) + geom_line() + xlab("Distance") + ylab("Percentage") + scale_x_discrete(labels=labs) + opts(axis.text.x= theme_text(angle=90))
#cars / walk by year group

output <- NULL

for (i in 1:13){
yr <- subset(Spring_Census_2011_WD_NOutlier_CO2, NCyearActual_SPR11 == i)
table <- CrossTable(yr$distance_bin,yr$Mode2,prop.c=FALSE,prop.r=TRUE, prop.t=FALSE,prop.chisq=FALSE,digits=2)
table2 <- as.data.frame(table$prop.row)
colnames(table2) <- c('Distance','Mode','Pct')
table2$yr <- i

output <- rbind(output,table2)

remove(yr,table,table2)

  print(i)
}

output2 <- subset(output, Mode != "TRA")
output2 <- subset(output2,!Distance %in% c('u:10-10.5km',  'v:10.5-11km',  'w:11-11.5km',	'x:11.5-12km',	'y:12-12.5km',	'z:12.5-13km',	'za:13-13.5km',	'zb:13.5-14km',	'zc:14-14.5km',	'zd:14.5-15km',	'ze:15-15.5km',	'zf:15.5-16km',	'zg:16-16.5km',	'zh:16.5-17km',	'zi:17-17.5km',	'zk:17.5-18km',	'zl:18-18.5km',	'zm:18.5-19km',	'zn:19-19.5km',	'zo:19.5-20km',	'zp:> 20km'))

ggplot(data=output2, aes(x=Distance,y=Pct, group=Mode, colour=Mode)) + geom_line()  + facet_grid(yr~.,scales="free_y") + xlab("Distance") + ylab("Percentage") + scale_x_discrete(labels=labs) + opts(axis.text.x= theme_text(angle=90))






#################################
#National CO2 map
################################

install.packages('GISTools',depend=T)
library(GISTools)

install.packages('classInt',depend=T)
library(classInt)

#Get LSOA Lookup
temp <- tempfile(fileext='.zip')
download.file("http://www.neighbourhood.statistics.gov.uk/HTMLDocs/images/OA_LSOA_MSOA_LAD_LU_newcodes_text_file_tcm97-102752.zip",temp)
LSOA_LA <- read.csv(unzip(temp))
unlink(temp)
LSOA_LA <- subset(LSOA_LA, select = c(LSOACD,LADCD))
LSOA_LA <- unique(LSOA_LA)
Spring_Census_2011_WD_NOutlier_CO2 <- merge(Spring_Census_2011_WD_NOutlier_CO2,LSOA_LA, by.x="LSOA",by.y="LSOACD",all.x=TRUE)

#Get LA
temp <- tempfile(fileext='.zip')
download.file("http://dl.dropbox.com/u/881843/shapefiles/Districts_LA_UA/dist_bor_ua_2.zip",temp)
dist_bor_ua <- readOGR(unzip(temp), "dist_bor_ua_2")
unlink(temp)

#Get GOR
temp <- tempfile(fileext='.zip')
download.file("http://dl.dropbox.com/u/881843/shapefiles/GOR/England_gor_2001_clipped.zip",temp)
gor<- readOGR(unzip(temp), "England_gor_2001_clipped")
unlink(temp)
gor <- generalize.polys(gor, 100)

# #Create median and sum CO2 aggregation

library(sqldf)
CO2_Local_Authority <- sqldf("select LADCD, count(*) as totpupils,(sum(co2g_route_X2)/1000) as tot_CO2kg_km, median(co2g_route_X2) as med_CO2g_km from Spring_Census_2011_WD_NOutlier_CO2 group by LADCD")





#Merge onto spatial data frame
dist_bor_ua@data <- merge(dist_bor_ua,CO2_Local_Authority, by.x="LABEL",by.y="LADCD", all.x=TRUE)

#Map setup - average
breaks <- classIntervals(dist_bor_ua@data$med_CO2g_km,n=4,style="fisher")
shades <- shading(breaks$brks, cols = brewer.pal(6, "GnBu"))

pdf(file='/Users/alex/Dropbox/Publications/Journals/Area/Model_Results_median.pdf')

# Draw a map for average
choropleth(dist_bor_ua, dist_bor_ua@data$med_CO2g_km, shades,border = NA)
choro.legend(18000, 542652.7, shades, fmt = "%3.0f", title = "Avg CO2g",border=NA,bty="n",x.intersp = 1.5)
plot(gor, border="#666666",bg=NA, add=TRUE)
dev.off()



#Map setup - sum
breaks <- classIntervals(dist_bor_ua@data$tot_CO2kg_km,n=4,style="fisher")
shades <- shading(breaks$brks, cols = brewer.pal(6, "GnBu"))

pdf(file='/Users/alex/Dropbox/Publications/Journals/Area/Model_Results_SUM.pdf')

# Draw a map for sum
choropleth(dist_bor_ua, dist_bor_ua@data$tot_CO2kg_km, shades,border = NA)
choro.legend(18000, 542652.7, shades, fmt = "%3.0f", title = "Sum CO2kg",border=NA,bty="n",x.intersp = 1.5)
plot(gor, border="#666666",bg=NA, add=TRUE)
dev.off()


#################################################
#Calculate CO2
sum(CO2_Local_Authority$tot_CO2kg_km)/1000 #CO2 tonnes per day
sum(CO2_Local_Authority$tot_CO2kg_km)/1000 * 5 #CO2 tonnes per week
sum(CO2_Local_Authority$tot_CO2kg_km)/1000 * 5 * 38 #CO2 tonnes per year
#################################################



##################
#CREATE LOCAL MAP - Kent
###################

#download kent
temp <- tempfile(fileext='.zip')
download.file("http://dl.dropbox.com/u/881843/shapefiles/CASWARD/kent.zip",temp)
unzip(temp)
kent<- readOGR("/Users/alex/kent", "kent")
#gor<- readOGR(unzip(temp), "England_gor_2001_clipped")
unlink(temp)

#Create median CO2 aggregation
CO2_CASWARD <- aggregate(Spring_Census_2011_WD_NOutlier_CO2$co2g_route~Spring_Census_2011_WD_NOutlier_CO2$CASWARD, FUN="median")
colnames(CO2_CASWARD) <- c("CASWARD","CO2g_km")

kent@data <- merge(kent@data,CO2_CASWARD, by.x="ons_label",by.y="CASWARD",all.x=TRUE)

breaks <- classIntervals(kent@data$CO2g_km,n=4,style="fisher")
shades <- shading(breaks$brks, cols = brewer.pal(6, "GnBu"))

pdf(file='/Users/alex/Dropbox/Publications/Journals/Area/Model_Results_local.pdf')
choropleth(kent, kent@data$CO2g_km, shades,border = NA)
plot(dist_bor_ua, border="#666666",bg=NA, add=TRUE)
text(getSpPPolygonsLabptSlots(dist_bor_ua), labels=dist_bor_ua@data$NAME, cex=0.4,font=2)
choro.legend(609388, 133000, shades, fmt = "%3.0f", title = "Avg CO2g",border=NA,bty="n",x.intersp = 1.5,y.intersp = 1.2, cex=0.6)

dev.off()




##################
#CREATE LOCAL MAP - Liverpool
###################


#download liverpool LSOA
temp <- tempfile(fileext='.zip')
download.file("http://dl.dropbox.com/u/881843/shapefiles/LSOA/Liverpool.zip",temp)
unzip(temp,overwrite = TRUE)
liverpool<- readOGR("/Users/alex/", "liverpool")
unlink(temp)

#download liverpool CASWARD
temp <- tempfile(fileext='.zip')
download.file("http://dl.dropbox.com/u/881843/shapefiles/CASWARD/Liverpool.zip",temp)
unzip(temp,overwrite = TRUE)
liverpool_ward<- readOGR("/Users/alex/", "liverpool")
unlink(temp)


#download liverpool roads
temp <- tempfile(fileext='.zip')
download.file("http://dl.dropbox.com/u/881843/shapefiles/OS_Vectormap_District/Liverpool.zip",temp)
unzip(temp,overwrite = TRUE)
liverpool_road <- readOGR("/Users/alex/", "liverpool")
unlink(temp)

liverpool_road_busy <- subset(liverpool_road,liverpool_road@data$CLASSIFICA %in% c("A Road","B Road","Motorway"))

library(classInt)

#Create sum CO2 aggregation at LSOA
CO2_LSOA <- aggregate(Spring_Census_2011_WD_NOutlier_CO2$co2g_route_X2~Spring_Census_2011_WD_NOutlier_CO2$LSOA, FUN="sum")
colnames(CO2_LSOA) <- c("LSOA","CO2g")

#Create median CO2 aggregation at School
CO2_school <- aggregate(Spring_Census_2011_WD_NOutlier_CO2$co2g_route_X2~Spring_Census_2011_WD_NOutlier_CO2$LAEstab_SPR11, FUN="median")
colnames(CO2_school) <- c("School_ID","CO2g")



#Read Edubase - select open secondary schools in liverpool
edubase <- read.csv("/Volumes/Macintosh HD 2/Dropbox/Data/Edubase/extract.csv")
edubase$code <- do.call(paste, c(edubase[c("LA..code.", "EstablishmentNumber")], sep = ""))
edubase <- subset(edubase, LA..code. == 341 & PhaseOfEducation..name. == "Secondary" & EstablishmentStatus..name. == "Open",select = c("EstablishmentName","Easting","Northing","code"))

#Merge LSOA
liverpool@data <- merge(liverpool@data,CO2_LSOA, by.x="zonecode",by.y="LSOA",all.x=TRUE)

#Merge Schools
Liverpool_Schools <- merge(edubase,CO2_school, by.x="code", by.y="School_ID", all.x=TRUE)
Liverpool_Schools_dup <- Liverpool_Schools


#Create spatial points dataframe for schools
coordinates(Liverpool_Schools)<- ~Easting+Northing
proj4string(Liverpool_Schools)=CRS("+init=epsg:27700")







min <- min(Liverpool_Schools$CO2g) 
max<- max(Liverpool_Schools$CO2g)
bound <- (max-min)/4

b1 <- bound
b2 <- bound * 2
b3 <- bound * 4

Liverpool_Schools@data$C_Bin <- ifelse(Liverpool_Schools$CO2g < b1, 1,ifelse(Liverpool_Schools$CO2g >=b1 & Liverpool_Schools$CO2g < b2, 2,ifelse(Liverpool_Schools$CO2g >=b2 & Liverpool_Schools$CO2g < b3, 3,ifelse(Liverpool_Schools$CO2g >= b3, 4,NA))))
Liverpool_Schools@data$C_Bin <- Liverpool_Schools@data$C_Bin * 1.5

pdf(file='/Users/alex/Dropbox/Publications/Journals/Area/Model_Results_local_Liverpool.pdf')
#Base map
breaks <- classIntervals(liverpool@data$CO2g,n=4,style="fisher")
shades <- shading(breaks$brks, cols = brewer.pal(6, "GnBu"))
choropleth(liverpool, liverpool@data$CO2g, shades,border = NA)

plot(liverpool_road,lwd=0.2,add=TRUE)

plot(liverpool_ward, border="#666666",bg=NA, add=TRUE)


#Points
colour <- rgb(69,139,0,alpha =200,maxColorValue=255)
plot(Liverpool_Schools, add=TRUE, cex = Liverpool_Schools@data$C_Bin, pch=16, col=colour)

#Legend Plot : Symbols (Circles)
y <- 381531
Northing <- c(y,y+780,y+2020,y+3700)
Easting <- 330674
CO2 <- c(1,2,3,4) *1.5
 
legend_plot <- data.frame(Easting,Northing,CO2)

coordinates(legend_plot)<- ~Easting+Northing
proj4string(legend_plot)=CRS("+init=epsg:27700")

plot(legend_plot, add=TRUE, cex = legend_plot@data$CO2, pch=16, col=colour)


#Legend Plot : Labels (Circles)
Easting <- 332199
Bin <-c(paste("<",round(b1, digits = 0)),
        paste(">=",round(b1, digits = 0) ,"and <",round(b2, digits = 0)),
        paste(">=",round(b2, digits = 0) ,"and <",round(b3, digits = 0)),
        paste(">",round(b3, digits = 0)))

text(Easting,Northing,labels=Bin, cex=0.6,font=2,adj=c(0,1))



#Legend Plot : Symbols (Squares)

y <- 387822
Northing <- c(y,y+800,y+1600,y+2400,y+3200,y+4000)
Easting <- 329658

legend_plot2 <- data.frame(Easting,Northing)

coordinates(legend_plot2)<- ~Easting+Northing
proj4string(legend_plot2)=CRS("+init=epsg:27700")

plot(legend_plot2, add=TRUE, pch=15, cex=1.3,col=shades$cols)

#Legend Plot : Labels (Squares)

shades$breaks <- shades$breaks /1000 #Rescale breaks into KG

  Bin <-c(
      paste("<",round(shades$breaks[1],digits = 0)),
      paste(">=",round(shades$breaks[1],digits = 0),"<",round(shades$breaks[2],digits = 0)),
      paste(">=",round(shades$breaks[2],digits = 0),"<",round(shades$breaks[3],digits = 0)),
      paste(">=",round(shades$breaks[3],digits = 0),"<",round(shades$breaks[4],digits = 0)),
      paste(">=",round(shades$breaks[4],digits = 0),"<",round(shades$breaks[5],digits = 0)),
      paste(">=",round(shades$breaks[5],digits = 0)))
Easting<- 330420
text(Easting,Northing,labels=Bin, cex=0.5,font=2,adj=c(0,1))


#Plot titles
text(330929,392778,labels="Sum Co2 kg",cex=0.9)
text(330929,386678,labels="Avg Co2g",cex=0.9)


dev.off()








########################################################
########scattler plot of population and total CO2########
########################################################

#Create Quintiles

ApplyQuintiles <- function(x) {
  cut(x, breaks=c(quantile(dist_bor_ua@data$totpupils, probs = seq(0, 1, by = 0.20))), 
      labels=c("Q1","Q2","Q3","Q4","Q5"), include.lowest=TRUE)
}

dist_bor_ua@data$quant_pop <- sapply(dist_bor_ua@data$totpupils, ApplyQuintiles)

ggplot(dist_bor_ua@data, aes(x=tot_CO2kg_km, y=med_CO2g_km, color=quant_pop)) + geom_point(shape=1)









