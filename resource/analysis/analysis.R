library(bigrf)
library(boot)
library(pROC)
library(psych)
library(Hmisc)

set.seed(10)

dataset <- read.csv(file="/Users/jirayus-j/Dropbox/java/FExtraction/resource/smallDataset.csv",header=TRUE)
#Remove perplexity from demo running
myData$perplexity <- NULL
myData$id <- NULL

#explanatory variable
#indep <-  c("contentLength","ari","colemanIndex","fleschKincaid","fleschReading","gunningFox","smog","sentenceCount","wordCount","nTag","loc","hasCode")

#remove high correlation variable. Remove NumComment and NumFiles in OpenStack
#remove Rendundant variable. Remove NumChangedLine in OpenStack
indep <-  c("contentLength","ari","colemanIndex","fleschKincaid","fleschReading","gunningFox","smog","sentenceCount","wordCount","nTag","loc","hasCode")


#objective variable
dep <- "hasAnswer"

#to know correlation, should remove the metrics whose score is over 0.7
plot(varclus(myData, data=dataset[,indep],trans="abs"))

#to know multicollinearity
#redun(~WC+Bug+NumPatch+PerDisOwner+NumAddedLine+NumDeletedLine+NumSubsystem+NumComponent,nk=0,data=dataset)

dataset <- dataset[, colnames(dataset) %in% c(dep,indep)]

#write.table(t(print(c("precision", "recall", "F1", "AUC"), quote = FALSE)), file="~/Documents/research/consensus/MeasureMetrics/prediction_ManualDiscrepancy/bootstrap_removedVariable/Qt_accuracy_discrepancy_all.csv", quote=F, sep=",", row.names=F,append = TRUE)

main <- function(dataset, indices){
  
  learning_dataset<-dataset[indices,] 
  estimate_dataset<-dataset
  preds <- learning_dataset
  preds$ManualDis <- NULL
  
  ###build prediction model
  data.learning.random <- bigrfc(preds, learning_dataset$ManualDis,ntree=10,cachepath=NULL)  
  data.estimate.predict = bigrf::predict(data.learning.random,estimate_dataset,estimate_dataset$ManualDis)
  
  # get importance
  var.imp <- varimp(data.learning.random, learning_dataset, impbyexample=T)$importance
  
  # get performance
  result<-slot(data.estimate.predict, "testconfusion")
  precision<-(result[1,1]/(result[1,1]+result[2,1]))
  recall<-(result[1,1]/(result[1,1]+result[1,2]))
  F1 <-  ( 2 * recall * precision) / (recall + precision) 
  
  # get TP, TN, FN, FP
  tmp <- slot(data.estimate.predict, "testvotes")
  prob = tmp[,1] / (tmp[,1] + tmp[,2])
  data.auc <- auc(roc(estimate_dataset$ManualDis, prob))
  
  return(c(precision, recall, F1, data.auc, var.imp))
}

results <- boot(dataset, statistic=main, R=1000)
results$t

