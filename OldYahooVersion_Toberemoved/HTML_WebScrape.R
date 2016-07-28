##Base logic for getKeyStats_xpath found here: http://www.r-bloggers.com/pull-yahoo-finance-key-statistics-instantaneously-using-xml-and-xpath-in-r/

#######################################################################
##Alternate method to download all key stats using XML and x_path - PREFERRED WAY
#######################################################################

##setwd("D:/Documents/School/SMU/2016 Summer/MSDS 6306 - Into to Data Science")
require(XML)
require(plyr)
library(formattable)

getKeyStats_xpath <- function(symbol) {
  #yahoo.URL <- "http://finance.yahoo.com/q/ks?s="     #q/pi?s="  #q/ks?s="
  
  yahoo.URL2 <- "http://finance.yahoo.com/q/pi?s="     #q/pi?s="  #q/ks?s="
  #html_text <- htmlParse(paste(yahoo.URL, "ABC", sep = ""), encoding="UTF-8")
  html_text2 <- htmlParse(paste(yahoo.URL2, symbol, sep = ""), encoding="UTF-8")
  
  #search for <td> nodes anywhere that have class 'yfnc_tablehead1'
  #nodes <- getNodeSet(html_text,"//td[@class='yfnc_tablehead1']") #"/*//td[@class='yfnc_mod_table_title1']")  #"/*//td[@class='yfnc_tablehead1']")
  
  nodes <- getNodeSet(html_text2,"//td[@class='yfnc_datamodlabel1']") #"/*//td[@class='yfnc_mod_table_title1']")  #"/*//td[@class='yfnc_tablehead1']")
  
  
  if(length(nodes) > 0 ) {
    measures <- sapply(nodes, xmlValue)
    
    #Clean up the column name
    measures <- gsub(" *[0-9]*:", "", gsub(" \\(.*?\\)[0-9]*:","", measures))   
    
    #Remove dups
    dups <- which(duplicated(measures))
    #print(dups) 
    for(i in 1:length(dups)) 
      measures[dups[i]] = paste(measures[dups[i]], i, sep=" ")
    
    #use siblings function to get value
    values <- sapply(nodes, function(x)  xmlValue(getSibling(x)))
    
    df <- data.frame(t(values))
    colnames(df) <- measures
    return(df)
  } else {
    break
  }
}

tickers <- c("FOCPX","FPURX")
stats <- ldply(getKeyStats_xpath(symbol = tickers))
rownames(stats) <- tickers
?ldply

formattable(stats)
stats$Ticker<-rownames(stats)
#######################################################################