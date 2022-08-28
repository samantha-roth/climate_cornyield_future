#prepare future growing season tmin, tmax, and pr

rm(list=ls())
setwd("/storage/work/svr5482")

load("Climate_CornYield-master/ANSI")

yrnum<- 30
countynum<- 1878
gslen<- 185 #in days

for(k in 1:2){
  
  #create a dataframe to hold the growing season daily maximum temperatures
  tmax_gs_temps<- matrix(NA, nrow= countynum, ncol= gslen*yrnum)
  tmin_gs_temps<- matrix(NA, nrow= countynum, ncol= gslen*yrnum)
  #get the total precipitation for each county for each year during the growing season
  totpr_GS<- rep(NA, countynum*yrnum)
  
  if (k==1){
    load("Countywise/Metdata/macaprojdataframe/GSstarttoend_2020_2049")
    load("Countywise/SourceData/MACAv2-METDATA_proj/linearshift_proj/tmax_2020_2049")
    load("Countywise/SourceData/MACAv2-METDATA_proj/linearshift_proj/tmin_2020_2049")
    load("Countywise/SourceData/MACAv2-METDATA_proj/linearshift_proj/pr_2020_2049")
    
    tmax<- shiftedMettmax_2020_2049
    tmin<- shiftedMettmin_2020_2049
    pr<- shiftedMetpr_2020_2049
    
    yr1=2020
    rm(shiftedMetpr_2020_2049,shiftedMettmax_2020_2049,shiftedMettmin_2020_2049)
  }
  if (k==2){
    load("Countywise/Metdata/macaprojdataframe/GSstarttoend_2070_2099")
    load("Countywise/SourceData/MACAv2-METDATA_proj/linearshift_proj/tmax_2070_2099")
    load("Countywise/SourceData/MACAv2-METDATA_proj/linearshift_proj/tmin_2070_2099")
    load("Countywise/SourceData/MACAv2-METDATA_proj/linearshift_proj/pr_2070_2099")
    
    tmax<- shiftedMettmax_2070_2099
    tmin<- shiftedMettmin_2070_2099
    pr<- shiftedMetpr_2070_2099
    
    yr1=2070
    rm(shiftedMetpr_2070_2099,shiftedMettmax_2070_2099,shiftedMettmin_2070_2099)
  }
  
  #create a vector that keeps track of the year each day is in
  yeardays<- NA
  for (j in 1:30){
    if(!((j-1)%%4)) yeardays<- c(yeardays, rep((yr1-1+j),366))
    if((j-1)%%4) yeardays<- c(yeardays, rep((yr1-1+j),365))
  }
  yeardays<- yeardays[-1]
  
  #create a dataframe for the daily max and daily min temps during the growing season
  #in each year in each county
  for(i in 1:countynum){
    for(j in 1:yrnum){
      year<- which(yeardays==yr1-1+j)
      tmx<- tmax[i,year]
      tmn<- tmin[i,year]
      tmx_GS<- tmx[c(GS_start[i,j]:GS_end[i,j])]
      tmn_GS<- tmn[c(GS_start[i,j]:GS_end[i,j])]
      tmax_gs_temps[i,(1+(j-1)*gslen):(gslen+(j-1)*gslen)]<- tmx_GS
      tmin_gs_temps[i,(1+(j-1)*gslen):(gslen+(j-1)*gslen)]<- tmn_GS
    }
  }
  
  for(i in 1:countynum){
    for(j in 1:yrnum){
      year<- which(yeardays==yr1-1+j)
      prec<- pr[i,year]
      totpr_GS[(i-1)*yrnum+j]<- sum(prec[c(GS_start[i,j]:GS_end[i,j])])
    }
  }
  
  #save growing season daily max and min temps
  if(k==1){
    save(tmax_gs_temps, file= "Countywise/SourceData/MACAv2-METDATA_proj/linearshift_proj/tmax_gs_temps_2020_2049")
    save(tmin_gs_temps, file= "Countywise/SourceData/MACAv2-METDATA_proj/linearshift_proj/tmin_gs_temps_2020_2049")
    save(totpr_GS, file= "Countywise/SourceData/MACAv2-METDATA_proj/linearshift_proj/totpr_GS_2020_2049")
  }
  if(k==2){
    save(tmax_gs_temps, file= "Countywise/SourceData/MACAv2-METDATA_proj/linearshift_proj/tmax_gs_temps_2070_2099")
    save(tmin_gs_temps, file= "Countywise/SourceData/MACAv2-METDATA_proj/linearshift_proj/tmin_gs_temps_2070_2099")
    save(totpr_GS, file= "Countywise/SourceData/MACAv2-METDATA_proj/linearshift_proj/totpr_GS_2070_2099")
  }
  
}


#for(k in 1:2){
#  if(k==1){
#    load("Countywise/SourceData/MACAv2-METDATA_proj/linearshift_proj/tmax_gs_temps_2020_2049")
#    load("Countywise/SourceData/MACAv2-METDATA_proj/linearshift_proj/tmin_gs_temps_2020_2049")
#  }
#  if(k==2){
#    load("Countywise/SourceData/MACAv2-METDATA_proj/linearshift_proj/tmax_gs_temps_2070_2099")
#    load("Countywise/SourceData/MACAv2-METDATA_proj/linearshift_proj/tmin_gs_temps_2070_2099")
#  }
  
#  for(i in 1:countynum){
#    print(length(which(is.na(tmax_gs_temps[i,]))))
#    print(length(which(is.na(tmin_gs_temps[i,]))))
#  }
#}


#can start from below
rm(list=ls())

library(foreach)
library(doParallel)

#setup parallel backend to use many processors
cores=detectCores()
cl <- parallel::makeCluster(cores[1]-1) # -1 not to overload system
registerDoParallel(cl)


foreach(k = 1:2)%dopar%{
  
  yrnum<- 30
  countynum<- 1878
  gslen<- 185 #in days
  
  if(k==1){
    load("Countywise/SourceData/MACAv2-METDATA_proj/linearshift_proj/tmax_gs_temps_2020_2049")
    load("Countywise/SourceData/MACAv2-METDATA_proj/linearshift_proj/tmin_gs_temps_2020_2049")
  }
  if(k==2){
    load("Countywise/SourceData/MACAv2-METDATA_proj/linearshift_proj/tmax_gs_temps_2070_2099")
    load("Countywise/SourceData/MACAv2-METDATA_proj/linearshift_proj/tmin_gs_temps_2070_2099")
  }
  
  
  #Find the max max temp and min min temp for all the growing seasons, set interval
  low<- floor(min(tmin_gs_temps))
  high<- ceiling(max(tmax_gs_temps))
  T_interval=c(low, high)
  
  #temp_dist_GS<- matrix(NA, nrow= 1, ncol= (high-low))
  
  #Calculate the amount of time spent in each 1 C interval
  #source("/gpfs/group/kzk10/default/private/svr5482/Climate_CornYield-me/T_distribution.R") #function to be used
  source("/storage/work/svr5482/Climate_CornYield-me/data_prep/T_distribution.R") #function to be used
  
  temp_dist_GS<- matrix(NA, nrow= countynum*yrnum, ncol= (high-low))
  for(i in 1:countynum){
    pt<- Sys.time()
    for(j in 1:yrnum){
      Tmax<- tmax_gs_temps[i,(1+(j-1)*gslen):(gslen+(j-1)*gslen)]
      Tmin<- tmin_gs_temps[i,(1+(j-1)*gslen):(gslen+(j-1)*gslen)]
      #temp_dist[i,(1+(j-1)*77):(77+(j-1)*77)]<- T_distribution(Tmax= Tmax, Tmin= Tmin, T_interval= T_interval)
      td<- T_distribution(Tmax= Tmax, Tmin= Tmin, T_interval= T_interval)
      #temp_dist_GS<- rbind(temp_dist_GS, td)
      temp_dist_GS[(i-1)*yrnum+j,]<- td
    }
    pt2<- Sys.time()
    pt2-pt
  }
  #temp_dist_GS<- temp_dist_GS[-1,]
  
  #save growing season daily max and min temps
  if(k==1){
    save(temp_dist_GS, file= "Countywise/SourceData/MACAv2-METDATA_proj/linearshift_proj/temp_dist_GS_2020_2049")
  }
  if(k==2){
    save(temp_dist_GS, file= "Countywise/SourceData/MACAv2-METDATA_proj/linearshift_proj/temp_dist_GS_2070_2099")
  }

}


stopCluster(cl)
