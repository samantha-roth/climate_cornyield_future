#create df using the future projections

rm(list=ls())

rm(list=ls())
setwd("/storage/work/svr5482")

library(foreach)
library(doParallel)

load("/storage/work/svr5482/Climate_CornYield-master/CountyANSI")
load("/storage/work/svr5482/Climate_CornYield-master/StateANSI")
load("/storage/work/svr5482/Climate_CornYield-master/ANSI")

load("/storage/work/svr5482/Climate_CornYield-me/SourceData/METDATA/df_tempdist_totpr")
ufips<- unique(df$fips); rm(df)

yrnum<- 30
obscountynum<- length(ufips)
gslen<- 185 #in days

ANSI<- ANSI[which(ANSI%in%ufips)]

df2<- data.frame(StateANSI=rep(StateANSI[which(ANSI%in%ufips)],each=yrnum),
                 countyANSI=rep(CountyANSI[which(ANSI%in%ufips)],each=yrnum), 
                 fips=rep(ANSI[which(ANSI%in%ufips)],each=yrnum),
                 year=rep(c(1:yrnum),obscountynum))

#setup parallel backend to use many processors
cores=detectCores()
cl <- parallel::makeCluster(cores[1]-1) # -1 not to overload system
registerDoParallel(cl)


foreach(k = 1:2)%dopar%{
  #save growing season daily max and min temps
  if(k==1){
    load("Countywise/SourceData/MACAv2-METDATA_proj/linearshift_proj/temp_dist_GS_2020_2049")
    load("Countywise/SourceData/MACAv2-METDATA_proj/linearshift_proj/totpr_GS_2020_2049")
    yr1= 2020
  }
  if(k==2){
    load("Countywise/SourceData/MACAv2-METDATA_proj/linearshift_proj/temp_dist_GS_2070_2099")
    load("Countywise/SourceData/MACAv2-METDATA_proj/linearshift_proj/totpr_GS_2070_2099")
    yr1= 2070
  }
  
  totpr_GS2<- totpr_GS^2
  
  df<- cbind(df2, totpr_GS, totpr_GS2, temp_dist_GS)
  
  df$year=df$year+yr1-1
  
  #rename the temperature distribution columns to correspond to the upper temperature of 
  #each 1degree C temperature intervals i.e. the amount of time spent in (-29,-28) is called -28
  #mininum temp: -29, maximum temp: 48
  
  tempnames<- seq(low,high-1, by=1)
  
  tempInd1<- which(colnames(df)=="totpr_GS2")+1
  colnames(df)[tempInd1:ncol(df)]<- as.character(tempnames)
  
  PrGSInd<- which(colnames(df)=="totpr_GS")
  PrGS2Ind<- which(colnames(df)=="totpr_GS2")
  colnames(df)[PrGSInd:PrGS2Ind]<- c("Pr_GS", "Pr_GS2")
  
  #find unique state ansi values
  states<- unique(df$StateANSI)
  
  stateyears<- as.data.frame(matrix(0, nrow= nrow(df), ncol= length(states)))
  for(i in 1:ncol(stateyears)) colnames(stateyears)[i]<- paste("state",states[i],"yr",sep="")
  
  for(i in 1:ncol(stateyears)){
    stateyears[which(df$StateANSI==states[i]),i]<- df$year[which(df$StateANSI==states[i])]-yr1+1
  }
  
  stateyears_sq<- stateyears^2
  for(i in 1:ncol(stateyears_sq)) colnames(stateyears_sq)[i]<- paste("state",states[i],"yr_sq",sep="")
  
  #columns 86-133 are stateyears and stateyears_sq
  df<- cbind(df, stateyears, stateyears_sq)
  
  #create the portion of the dataframe containing indicators for the fips codes
  uniquefips<- unique(df$fips)
  ufips<- as.character(uniquefips)
  
  fipscode<- matrix(0, nrow= nrow(df), ncol= length(ufips))
  fipscode<- as.data.frame(fipscode)
  
  for(i in 1:length(ufips)) colnames(fipscode)[i]<- paste("fips",uniquefips[i],sep="")
  for(i in 1:length(ufips)) fipscode[which(df$fips==uniquefips[i]),i]<- 1
  
  df<- cbind(df, fipscode)
  #save(df, file= "/storage/work/svr5482/Climate_CornYield-me/SourceData/METDATA/df_tempdist_totpr")
  
  #save growing season daily max and min temps
  if(k==1){
    save(df, file= "Countywise/SourceData/MACAv2-METDATA_proj/linearshift_proj/df_tempdist_totpr_2020_2049")
  }
  if(k==2){
    save(df, file= "Countywise/SourceData/MACAv2-METDATA_proj/linearshift_proj/df_tempdist_totpr_2070_2099")
  }

}
