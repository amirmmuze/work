library("tm")
library("partykit")
pathpred <- function(object, ...)
{
  ## coerce to "party" object if necessary
  if(!inherits(object, "party")) object <- as.party(object)
  
  ## get standard predictions (response/prob) and collect in data frame
  rval <- data.frame(response = predict(object, type = "response", ...))
  rval$prob <- predict(object, type = "prob", ...)
  
  ## get rules for each node
  rls <- partykit:::.list.rules.party(object)
  
  ## get predicted node and select corresponding rule
  rval$rule <- rls[as.character(predict(object, type = "node", ...))]
  
  return(rval)
}

txt <- c("some text", "text  baba this", "text as example vector")
file_path = "/Users/danielbarkan/rules/data_docs/pants_male_#dress_code_active#.csv"
f<-read.csv(file_path)
View(f)
corpus <- Corpus(VectorSource(f$doc))
dtm <- DocumentTermMatrix(corpus)
removeSparseTerms(dtm, 0.1)

library(tidytext)
DF <- tidy(dtm)

df<-as.data.frame(as.matrix(dtm), stringsAsFactors=False)
df$class<-f$class
library(rpart)
library(rpart.plot)
library(e1071)
library(nnet)
tree = rpart(class~.,  method = "class", data = df, control=rpart.control(cp=0.1));  
prp(tree)

pred.tree = predict(tree, df,  type="class")
table(df$class,pred.tree,dnn=c("Obs","Pred"))
library(rpart.plot)
rpart.plot(tree, box.palette="RdBu", shadow.col="gray", nn=TRUE)

rp_pred <- pathpred(tree)
rp_pred[c(1, 51, 101), ]
rpart.rules(tree,nn=TRUE)

