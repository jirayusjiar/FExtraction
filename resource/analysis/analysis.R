library(bigrf)
library(boot)
library(pROC)
library(psych)
library(Hmisc)
library(party)


dataset <- read.csv(file="/Users/jirayus-j/Dropbox/Source/java/FExtraction/resource/analysis/smallDataset.csv",header=TRUE)
#Remove perplexity from demo running
dataset$perplexity <- NULL
dataset$id <- NULL
dataset$hasCode <- NULL
dataset$hasAnswer <- factor(dataset$hasAnswer,levels = c("t","f"))

#explanatory variable
#indep <-  c("contentLength","ari","colemanIndex","fleschKincaid","fleschReading","gunningFox","smog","sentenceCount","wordCount","nTag","loc","hasCode")

#remove high correlation variable. Remove NumComment and NumFiles in OpenStack
#remove Rendundant variable. Remove NumChangedLine in OpenStack
indep <-  c("contentLength","ari","colemanIndex","fleschKincaid","fleschReading","gunningFox","smog","sentenceCount","wordCount","nTag","loc")


#objective variable
dep <- "hasAnswer"

iris_ctree <- ctree(hasAnswer 
                    ~ 
                    contentLength+ari+colemanIndex+fleschKincaid+fleschReading+gunningFox+smog+sentenceCount+wordCount+nTag+loc
                    , data=dataset)
print(iris_ctree)
plot(iris_ctree)
plot(iris_ctree, type="simple")
