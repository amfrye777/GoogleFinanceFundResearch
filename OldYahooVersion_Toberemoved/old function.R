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
        df <- data.frame(symbol,t(values))
        colnames(df) <- c("Symbol",measures)
        
        #If first loop, initialize YahooDataFrame, else Merge YahooDataFrame with new df Page data by Ticker Symbol
        if(PageCnt == 1)
          YahooDataFrame<-df
        else
          YahooDataFrame<-merge(YahooDataFrame,df,by="Symbol")
      }
    }
    return(YahooDataFrame)
  }
}

TickerList<-getKeyStats_xpath()

TickerList <- c("FOCPX","FPURX") #TickerList [1:1000]
head(TickerList,100)
tail(TickerList,100)
length(TickerList)

TickerCnt<- 1

for (TickerCnt in 1:length(TickerList)) {
  if(TickerCnt == 1)
    YahooDataFrame<-getKeyStats_xpath(TickerList[TickerCnt])
  else
    YahooDataFrame<-rbind(YahooDataFrame,getKeyStats_xpath(TickerList[TickerCnt]))
  
  print(paste("TickerCnt",TickerCnt,"Completed!"))
  TickerCnt<-TickerCnt+1
}
getKeyStats_xpath("FOCPX")
YahooDataFrame <- ldply(as.list(TickerList),getKeyStats_xpath)

formattable(tail(YahooDataFrame,100))

which(duplicated(YahooDataFrame$Symbol))

formattable(YahooDataFrame[27511,])

