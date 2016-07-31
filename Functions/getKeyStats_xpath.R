#######################################################################
##Author:       Alex Frye
##Created:      06/19/2016
##Citation:     Base logic for getKeyStats_xpath found here: 
##                http://www.r-bloggers.com/pull-yahoo-finance-key-statistics-instantaneously-using-xml-and-xpath-in-r/
##Description:  Define function getKeyStats_xpath to be consumed downstream.
##              Attribute lookups only 
##                INPUTS:   symbol          = "Finder" or Ticker symbol 
##                          URLSuffix       = Modular URL text to be appended dynamically for each page 
##                          Attibute1Name   = Attribute1 Name (e.g. @class)
##                          Attibute1Value  = Attribute1 Value (e.g. 'yfnc_datamodlabel1')
##                          Attibute2Name   = Attribute2 Name (e.g. @class) 
##                          Attibute2Value  = Attribute2 Value (e.g. 'yfnc_datamodlabel1')
##
##                OUTPUTS:  TickerList      = Character Vector of ticker symbols
##                          df              = Data.Frame containing Yahoo Fund data for Ticker Symbols Requested

##This function will produce a data.frame df output variable containing
#######################################################################
# install.packages('XML')
# install.packages('plyr')
# install.packages('formattable')
# install.packages('doParallel')
# install.packages('foreach')
# install.packages('quantmod') 

library(XML)
library(plyr)
library(formattable)
library(doParallel)
library(foreach)
library(quantmod) 

symbol<-"FOCPX"

getKeyStats_xpath <- function(symbol="Finder") {
  ############################################################################################  
    ## Finder symbol is to grab all Mutual Fund ticker symbols!  
  if (symbol == "Finder") {
      #Initialize TickerList character vector 
    TickerList  <- vector(mode = "character", length = 1)

      #Loop through Letters A:Z from eoddata.com to identify full list of funds available
    lettercnt <- 1
    for (lettercnt in 1:26) {
      html_text <- htmlParse(paste0("http://www.eoddata.com/stocklist/USMF/",letters[lettercnt],".htm"),encoding = "UTF-8")
      nodes     <- getNodeSet(html_text, "//table[@class='quotes']//td/a")
     
            #strip Ticker Value from nodes and append to TickerList vector
          if (length(nodes) > 0) {
            TickerList <- cbind(TickerList,sapply(nodes, xmlValue))
            TickerList <- TickerList[TickerList != '']
          }
      lettercnt<-lettercnt+1
    }
    
      #Remove Duplicate Tickers in list if applicable
    dups <- which(duplicated(TickerList))
    if (length(dups)>0) {
      TickerList<-TickerList[-dups]
    }
    
      #Do not want to receive data for these Tickers captured
    RemoveTickers<-which(TickerList %in%  c("COMP",  "DJI",   "SP500", "DAX",   "FTSE",  "NI225", "CAC40", "GLD",   "BDI",   "HSI"))
    TickerList<- TickerList[-RemoveTickers]

    
  return(TickerList)
  }
  
  ############################################################################################
  ############################################################################################
      ##symbol defines the Ticker to lookup on the various yahoo finance pages
  if (symbol != "Finder") {    
      ##Define Lookup metadata values for Google Site 
    GooglePageLookup<- data.frame(Name=character(),URL=character(),XPATHQuery=character())
    GooglePageLookup<- rbind(GooglePageLookup,data.frame(Name="Category",           URL="http://www.google.com/finance?q=MUTF:", XPATHQuery= "//*[@id='gf-viewc']//div[contains(@class, 'sector')]//div[contains(@class, 'subsector')]"))
    GooglePageLookup<- rbind(GooglePageLookup,data.frame(Name="SummaryReturns",     URL="http://www.google.com/finance?q=MUTF:", XPATHQuery= "//div[contains(@class, 'sector performance')]//td[not(contains(@class, 'bar'))]"))

    GPLID<- row.names(GooglePageLookup)
    GooglePageLookup<- cbind(GPLID,GooglePageLookup)

      #Loop through PageLookup values to define data.frame yahoo values
    PageCnt<- 1
    for(PageCnt in 1:nrow(GooglePageLookup)){

      if(PageCnt == 1){
        html_text <- htmlParse(paste0(GooglePageLookup$URL[PageCnt], symbol), encoding="UTF-8")
      }else if(GooglePageLookup$URL[PageCnt] != GooglePageLookup$URL[PageCnt-1]){
        html_text <- htmlParse(paste0(GooglePageLookup$URL[PageCnt], symbol), encoding="UTF-8")
      }
     
    nodes     <- getNodeSet(html_text,GooglePageLookup$XPATHQuery[PageCnt])      

  if(length(nodes) > 0 ) {
      ##Define column names and data values
    measures <- sapply(nodes, xmlValue)
    values <- sapply(nodes, function(x)  xmlValue(getSibling(x)))

    
      #Clean up the column name (Global Cleanup)
    measures <- gsub("\\s$","",gsub("[:*:]$","",gsub("[:(:].*[:):]","",gsub("\n\\s*","",gsub(" *[0-9]*:", "", gsub(" \\(.*?\\)[0-9]*:","", measures)))))) 

            #Custom Cleanup for the SummaryReturns Page
        if (GooglePageLookup$Name[PageCnt] == "SummaryReturns"){ #remove columns which contain only numerical values
          #remove Blanks
          measures <- measures[which(measures != '')]
          
          #Clean up the column name (Global Cleanup)
          measures <- gsub("\\s$","",gsub("[:*:]$","",gsub("[:(:].*[:):]","",gsub("\n\\s*","",gsub(" *[0-9]*:", "", gsub(" \\(.*?\\)[0-9]*:","", measures)))))) 
          measures <- measures[which(measures != '')]
          
          #Remove *annualized
          measures<-measures[-17]
          
          #Define Odd vs Even
          SummaryReturns.DF<-as.data.frame(cbind(measures,c("Odd","Even")))
          colnames(SummaryReturns.DF)<-c("measures","recordType")

          #re-define measures and values
          measures<- as.character(SummaryReturns.DF$measures[which(SummaryReturns.DF$recordType == "Odd")])
          values  <- as.character(SummaryReturns.DF$measures[which(SummaryReturns.DF$recordType == "Even")])
          
          #only care about first 8 columns
          measures<-measures[1:8]
          values  <-values[1:8]
          }else if(GooglePageLookup$Name[PageCnt] == "Category"){
            #Identify Morningstar Category from measures
            measures<-measures[grep('Morningstar category', measures, perl=T)]
            
            #re-define measures and values
            
            values  <-substr(measures[1],22,999)
            measures<-substr(measures[1],1,20)
            }

          #Define Data.Frame df as ticker symbol and Measure/values defined above
        df <- data.frame(symbol,t(values),stringsAsFactors = FALSE)
        colnames(df) <- c("Symbol",measures)
        
          #If Category, Merge df with categories.
        if(GooglePageLookup$Name[PageCnt] == "Category"){
          source("functions/categoriesSource.R")
          df<- merge(df,Categories,by.x = "Morningstar category",by.y = "Category", all.x = TRUE)
          df <- df[,c("Symbol","CategoryGroup","Morningstar category")]
          }
        
          #If first loop, initialize GoogleDataFrame, else Merge GoogleDataFrame with new df Page data by Ticker Symbol
        if(PageCnt == 1){
          
          # df$SymbolDescription<-paste(getQuote(symbol, what=yahooQF("Name"))[,2]) 
          desc <- as.data.frame(paste(getQuote(symbol, what=yahooQF("Name"))[,2]))
          colnames(desc)<-"Symbol Description"
          GoogleDataFrame<-cbind(desc,df)
          }
        else
          GoogleDataFrame<-merge(GoogleDataFrame,df,by="Symbol")
      }
      else
          return(NA)
    }
    GoogleDataFrame
  return(GoogleDataFrame)
  }
}

n.cores<-detectCores()
stopCluster(cl)
cl<-makeCluster(3)
registerDoParallel(cl)
getDoParWorkers()

TickerList<-getKeyStats_xpath()


TickerList<-TickerList[1:10]
# TickerList

#print(paste("Estimated Time to completion:",length(TickerList)/3/60/60,"Hours"))
StartTime<-Sys.time()
  ##GoogleDataFrame<-as.data.frame(do.call(rbind,lapply(TickerList,getKeyStats_xpath)),stringsAsFactors=FALSE)
  TickerCnt<- 1
  
  GoogleDataFrame<-foreach(TickerCnt = 1:length(TickerList), .combine = rbind,.packages = c('XML','quantmod')) %dopar% 
                      {
                        GGLrow<-getKeyStats_xpath(TickerList[[TickerCnt]])
                        
                        if(TickerCnt %% 10 == 0) {
                          diff<-as.numeric(Sys.time()-StartTime)
                          sleepTime<-sample(1:30,1)     ###(60-(diff %% 60))*30 ## only 299 yahoo scrapes allowed every half hour per core(i.e. 1200 scrapes per half hour)
                          Sys.sleep(sleepTime)
                        }
                        
                        return(GGLrow)
                      }
EndTime<-Sys.time()
print(EndTime-StartTime)


library(quantmod) 
sym <- "FOCPX" 
paste(getQuote(sym, what=yahooQF("Name"))[,2]) 
"Cisco Systems, In" 




#######Clean data
str(GoogleDataFrame)

formattable(GoogleDataFrame)

GoogleDataFrame.Cleaned<- GoogleDataFrame[which(  !is.na(GoogleDataFrame$Symbol) 
                                              & !is.na(GoogleDataFrame$`Morningstar Return Rating`) 
                                              & !is.na(GoogleDataFrame$`Morningstar Risk Rating`) 
                                              & GoogleDataFrame$`Morningstar Return Rating`          != '' 
                                              & GoogleDataFrame$`Morningstar Risk Rating`            != '' 
                                              & (
                                                 GoogleDataFrame$`Min Initial Investment`            <= 3000   |
                                                 GoogleDataFrame$`Min Initial Investment, IRA`       <= 3000
                                                )
                                              & GoogleDataFrame$`Morningstar Return Rating`          >= 3
                                              & GoogleDataFrame$`Morningstar Risk Rating`            <= 3
                                              )
                                        ,]

str(GoogleDataFrame.Cleaned)

formattable(GoogleDataFrame.Cleaned)


# 
# TickerCnt<- 1
# for(TickerCnt in 1:length(TickerList)) {
#   if(TickerCnt == 1)
#     GoogleDataFrame<-getKeyStats_xpath(TickerList[[TickerCnt]])
#   else
#     GoogleDataFrame<-rbind(GoogleDataFrame,getKeyStats_xpath(TickerList[[TickerCnt]]))
#   print(TickerList[[TickerCnt]])
#   print(paste("TickerCnt for loop:",TickerCnt))
#   print(paste("RowCnt Generated:",nrow(GoogleDataFrame)))
#   TickerCnt <- TickerCnt+1
# }
# 
# TickerList <- TickerList[1:30]
# length(TickerList)
# TickerCnt<- 1
# foreach(TickerCnt = 1:length(TickerList), .combine = rbind,.packages = 'XML') %dopar% 
#   getKeyStats_xpath(TickerList[[TickerCnt]])

