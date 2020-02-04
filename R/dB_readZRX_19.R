library("readr")
library("dplyr")
library("magrittr")
library("purrr")
library("stringr")
library("tidyr")
library("lubridate")
library("tibble")

file1 <- "C:/Users/MRossi/Documents/03_Data/03_InSitu/02_Province/Rossi_N_T_08_09.zrx"
file2 <- "C:/Users/MRossi/Documents/03_Data/03_InSitu/02_Province/Rossi_N_T_16_17.zrx"

db0809 <- dB_readZRX19(file1)
db1617 <- dB_readZRX19(file2)

bnd<-unnest(rbind(db0809,db1617))
saveRDS(bnd,file = "C:/Users/MRossi/Documents/03_Data/03_InSitu/02_Province/Rossi_N_T_bound.rds")

dB_readZRX19 <- function(file, header="", unnest="a", delete.NA=T,verbose=F) {
  
  
  # Create the Header File with the important header Files
  if(header=="") header<-c("ZRXPVERSION","ZRXPCREATOR","SANR","SNAME","CNR","CNAME","SFGEBIET","CUNIT","REXCHANGE","LAYOUT")
  header <- tibble::enframe(header, name = NULL,value = "Meta")
  
  # Open Connection to the File
  if(verbose==T) print("Read File")
  dummy   <- readLines(con=file, n = -1)
  if(verbose==T) print(paste0("File Read with",length(dummy),"Lines"))
  
  
  # Data/Header Columns
  if(verbose==T) print("Search for Header and Data Columns Columns") 
  
  # All Metadata finishes with #LAYOUT and starts with #ZRXPVERSION
  metastart<- which(substr(dummy,1,7)=="#ZRXPVE")
  metaend  <- which(substr(dummy,1,7)=="#LAYOUT")
  
  df<-cbind(metastart,metaend) %>% 
    as_tibble %>% 
    mutate(metalength=metaend-metastart)
  
  # List of Data with some exceptions in case there is no data or in case we are at the very end of the table
  data<-list()
  for (i in 1:(nrow(df))){
    
    t1 <- df$metaend[i]+1
    t2 <- if(i<nrow(df)) df$metastart[i+1] else t2<-length(dummy)
    
    if (t1>=t2) {
      
      t1  <- NA
      res <- NA
      
    } else {res<-(t2-1)}
    
    data[[i]]<-c(t1,res)
  }
  
  # Add the Data sections at the respective part of the table
  df<-df %>% mutate(datastart=sapply(data,"[[",1))
  df<-df %>% mutate(dataend=sapply(data,"[[",2))
  
  if(verbose==T) nas<-length(which(is.na(df$datastart)))
  if(verbose==T) print(paste("Found",nrow(df),"Entries -",nas, "NA Entries"))
  
  # Process the various headers from metastart (start) to metaend(end) based on the header 
  # (h) defined at the beginning of the function
  
  if(verbose==T) print("Process Header") 
  df.meta<-df %>% 
    mutate(Header=map2(metastart,metaend,function(start,end,h=header){
      
      index<-c(start:end)
      headerString<-dummy[index] %>% 
        paste0(collapse="") %>% 
        str_replace_all("#","") %>% 
        strsplit(.,"\\|\\*\\|") %>% 
        unlist
      
        tib<- h %>% 
          mutate(Value=map_chr(Meta,function(x){
            
            info<-headerString[grep(x,headerString)]
            res<-str_replace(info,x,"")
            return(res)
            
          })) %>% spread(Meta,Value)
        
        return(tib)
      }))
  
  if(unnest=="h" | unnest=="a")  df.meta<-unnest(df.meta,Header)
  
  # Add the Data Values and format the respective tables
  if(verbose==T) print("Process Data") 
  df.data<- df.meta %>% 
    mutate(Data=map2(datastart,dataend,function(start,end){
      
      if(is.na(start)) return(NA)
      
      index <- c(start:end)
      data  <- dummy[index]
      bind  <- str_split(data," ",simplify = T)
      bind.tib<- bind %>% 
        as_tibble %>%
        mutate(TimeStamp=as_datetime(V1)) %>% 
        mutate(Value=as.numeric(V2)) %>% 
        mutate(Status=as.numeric(V3)) %>% 
        select(-c(V1,V2,V3))
      
      return(bind.tib)
      
    }))
  
  # Last changes about the formatting of thetable depending on the function ards
  if(verbose==T) print("Format Table") 
  if(delete.NA==T) df.data<-filter(df.data,!is.na(datastart))
  df.final<-df.data %>% select(-c(metastart,metaend,metalength,datastart,dataend))
  if(unnest=="a")  df.final<-unnest(df.final)
  
  # Return
  return(df.final)
  
}
