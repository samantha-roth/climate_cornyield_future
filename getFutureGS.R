#calculate growing season
rm(list=ls())
setwd("/storage/work/svr5482/Countywise")

ma <- function(arr, n){  #moving avg function
  res = rep(NA,length(arr)-n+1)
  for(i in n:length(arr)){
    res[i] = mean(arr[(i-n+1):i])
  }
  res
}

#can also start from loading after saving 
load("SourceData/MACAv2-METDATA_proj/linearshift_proj/tmax_2020_2049")
load("SourceData/MACAv2-METDATA_proj/linearshift_proj/tmin_2020_2049")
#load("SourceData/MACAv2-METDATA_proj/linearshift_proj/pr_2020_2049")
load("SourceData/MACAv2-METDATA_proj/linearshift_proj/tmax_2070_2099")
load("SourceData/MACAv2-METDATA_proj/linearshift_proj/tmin_2070_2099")
#load("SourceData/MACAv2-METDATA_proj/linearshift_proj/pr_2070_2099")

totdays<-30*365+8
countynum<-1878

#Then calculate VPD,EDD,GDD
for (k in 1:2){
  if (k==1){
    yr<-"2020_2049"
    yr1<- 2020
  }
  if (k==2){
    yr<-"2070_2099"
    yr1<-2070
  }

  Tmean<-(get(paste("shiftedMettmax_",yr,sep=""))+get(paste("shiftedMettmin_",yr,sep="")))/2
  
  Tthres<-10
  GS_start<-matrix(NA,nrow=countynum,ncol=30)
  GS_end<-matrix(NA,nrow=countynum,ncol=30)
  m1=1
  m2=1
  for (i in 1:countynum){
    m1=1
    m2=1
    for (j in 1:30){
      n=365
      if ((yr1+j-1)%%4==0){ 
        n=366
      }
      m2<-m1+n-1
      Tmeantoget<-Tmean[i,c(m1:m2)]
      TMA<-ma(Tmeantoget,21)
      GS_start[i,j]<-which(TMA >= Tthres)[1]
      GS_end[i,j]<-GS_start[i,j]+184
      m1=m2+1
    }
  }
  
  if (k==1){
    save(GS_start,GS_end,file="Metdata/macaprojdataframe/GSstarttoend_2020_2049")
  }
  if (k==2){
    save(GS_start,GS_end,file="Metdata/macaprojdataframe/GSstarttoend_2070_2099")
  }
}
