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
library(XML)
library(plyr)
library(formattable)
library(doParallel)
library(foreach)

# install.packages(devtools)
# 
# library(devtools)
# install_github(repo = "RYQL", username = "juuussi")
# 
# library(RYQL)
# 
# queryYQL(query="SELECT * FROM yahoo.finance.keystats WHERE symbol IN (\\"fb\\")")
# queryYQLtoR("SELECT * FROM yahoo.finance.quant WHERE symbol IN ('focpx','fpurx')")
# queryYQLtoR("show tables")

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
      ##Define Lookup metadata values for Yahoo Site 
    YahooPageLookup<- data.frame(Name=character(),URL=character(),AttrType=character(),AttrValue=character())
    YahooPageLookup<- rbind(YahooPageLookup,data.frame(Name="Profile",     URL="http://finance.yahoo.com/q/pr?s=", AttrType="@class", AttrValue="yfnc_datamodlabel1"))
    YahooPageLookup<- rbind(YahooPageLookup,data.frame(Name="Performance", URL="http://finance.yahoo.com/q/pm?s=", AttrType="@class", AttrValue="yfnc_datamodlabel1"))
    YahooPageLookup<- rbind(YahooPageLookup,data.frame(Name="Risk",        URL="http://finance.yahoo.com/q/rk?s=", AttrType="@class", AttrValue="yfnc_datamodlabel1"))
    YPLID<- row.names(YahooPageLookup)
    YahooPageLookup<- cbind(YPLID,YahooPageLookup)
    
      #Loop through PageLookup values to define data.frame yahoo values
    PageCnt<- 1
    for(PageCnt in 1:nrow(YahooPageLookup)){
      html_text <- htmlParse(paste0(YahooPageLookup$URL[PageCnt], symbol), encoding="UTF-8")
      nodes     <- getNodeSet(html_text,paste0("//td[contains(",YahooPageLookup$AttrType[PageCnt],",'",YahooPageLookup$AttrValue[PageCnt],"')]"))
      
      if(length(nodes) > 0 ) {
          ##Define column names and data values
        measures <- sapply(nodes, xmlValue)
        values <- sapply(nodes, function(x)  xmlValue(getSibling(x)))
        
          #Clean up the column name (Global Cleanup)
        measures <- gsub("\\s$","",gsub("[:*:]$","",gsub("[:(:].*[:):]","",gsub("\n\\s*","",gsub(" *[0-9]*:", "", gsub(" \\(.*?\\)[0-9]*:","", measures)))))) 
        
          #Custom Cleanup for the Performance Page
        if (YahooPageLookup$Name[PageCnt] == "Performance"){ #remove columns which contain only numerical values
            #Find positions which only contain numeric values (these are years) -- We do not want to store these
          measures <- gsub("^[0-9]*$","",measures)
          removepositions<-which(measures=="")
          
            #only run this logic to remove positions if we found at least one column that matches criteria above 
          if (sum(removepositions)>0) {
            measures<-measures[-removepositions]
            values<-values[-removepositions]
          }
            
            #There should be 28 positions left on the page after removing those values: Add descriptors to define page table sections
          measures[10:13]<- paste("Load Adjusted Returns:",measures[10:13])
          measures[14:21]<- paste("Trailing Returns %:",measures[14:21])
          measures[21:28]<- paste("Rank %:",measures[21:28])
        }
        
          #We only need the first position from the Risk Tab to give us the risk rating
        if (YahooPageLookup$Name[PageCnt]== "Risk"){
          measures <- measures[1]
          values <- values[1]
        }

          #Define Data.Frame df as ticker symbol and Measure/values defined above
        df <- data.frame(symbol,t(values),stringsAsFactors = FALSE)
        colnames(df) <- c("Symbol",measures)
        
        
          #If first loop, initialize YahooDataFrame, else Merge YahooDataFrame with new df Page data by Ticker Symbol
        if(PageCnt == 1)
          YahooDataFrame<-df
        else
          YahooDataFrame<-merge(YahooDataFrame,df,by="Symbol")
      }
      else
          return(NA)
    }
  return(YahooDataFrame)
  }
}

n.cores<-detectCores()
stopCluster(cl)
cl<-makeCluster(3)
registerDoParallel(cl)
getDoParWorkers()

TickerList<-getKeyStats_xpath()
# TickerList<-TickerList[1:100]
#print(paste("Estimated Time to completion:",length(TickerList)/3/60/60,"Hours"))
StartTime<-Sys.time()
  ##YahooDataFrame<-as.data.frame(do.call(rbind,lapply(TickerList,getKeyStats_xpath)),stringsAsFactors=FALSE)
  TickerCnt<- 1
  
  YahooDataFrame<-foreach(TickerCnt = 1:length(TickerList), .combine = rbind,.packages = 'XML') %dopar% 
                      {
                        getKeyStats_xpath(TickerList[[TickerCnt]])
                        if(TickerCnt %% 299 == 0) {
                          diff<-as.numeric(Sys.time()-StartTime)
                          sleepTime<-(60-(diff %% 60))*30 ## only 400 yahoo scrapes allowed every half hour per core(i.e. 1200 scrapes per half hour)
                          Sys.sleep(sleepTime)
                        }
                      }
EndTime<-Sys.time()
print(EndTime-StartTime)


#######Clean data
str(YahooDataFrame)

YahooDataFrame.Cleaned<- YahooDataFrame[which(  !is.na(YahooDataFrame$Symbol) 
                                              & !is.na(YahooDataFrame$`Morningstar Return Rating`) 
                                              & !is.na(YahooDataFrame$`Morningstar Risk Rating`) 
                                              & YahooDataFrame$`Morningstar Return Rating`          != '' 
                                              & YahooDataFrame$`Morningstar Risk Rating`            != '' 
                                              & (
                                                 YahooDataFrame$`Min Initial Investment`            <= 3000   |
                                                 YahooDataFrame$`Min Initial Investment, IRA`       <= 3000
                                                )
                                              & YahooDataFrame$`Morningstar Return Rating`          >= 3
                                              & YahooDataFrame$`Morningstar Risk Rating`            <= 3
                                              )
                                        ,]

str(YahooDataFrame.Cleaned)

formattable(YahooDataFrame.Cleaned)


# 
# TickerCnt<- 1
# for(TickerCnt in 1:length(TickerList)) {
#   if(TickerCnt == 1)
#     YahooDataFrame<-getKeyStats_xpath(TickerList[[TickerCnt]])
#   else
#     YahooDataFrame<-rbind(YahooDataFrame,getKeyStats_xpath(TickerList[[TickerCnt]]))
#   print(TickerList[[TickerCnt]])
#   print(paste("TickerCnt for loop:",TickerCnt))
#   print(paste("RowCnt Generated:",nrow(YahooDataFrame)))
#   TickerCnt <- TickerCnt+1
# }
# 
# TickerList <- TickerList[1:30]
# length(TickerList)
# TickerCnt<- 1
# foreach(TickerCnt = 1:length(TickerList), .combine = rbind,.packages = 'XML') %dopar% 
#   getKeyStats_xpath(TickerList[[TickerCnt]])

