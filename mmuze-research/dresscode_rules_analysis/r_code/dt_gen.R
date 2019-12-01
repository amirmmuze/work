library("tm")
library("partykit")
library(rattle)
library(rpart)
library(rpart.plot)
library(e1071)
library(nnet)
library(rpart.plot)
library(randomForest)

# to get prediction rule for every data row
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


files = list.files(path = "../data_docs", pattern = NULL, all.files = FALSE,
                   full.names = FALSE, recursive = FALSE,
                   ignore.case = FALSE, include.dirs = FALSE, no.. = FALSE)

files  = sort(files)
n=length(files)
full_filespath = "../data_docs"
full_rulespath = "../data_dt_rules"
cat("n",n)
for (i in 1:n)
{   
  cat("i",i)
  
  filename= files[i]
  #filename = "gym bag#female#dress_code_active.csv"
  print (filename)
  parent_type = unlist(strsplit(filename,"[#.]"))[1]
  gender = unlist(strsplit(filename,"[#.]"))[2]
  dress_code  = unlist(strsplit(filename,"[#.]"))[3]
  postfix_filename = unlist(strsplit(filename,"[#.]"))[4]
  
  full_filename = paste0(full_filespath,"/",filename) 
  print(full_filename)
  print(dress_code)
  rules_fullfilename = paste0(full_rulespath,"/","#",parent_type,"#",gender,"#",dress_code,"#","rules.",postfix_filename)
  print(rules_fullfilename)
  f<-read.csv(full_filename)
  corpus <- Corpus(VectorSource(f$doc))
  dtm <- DocumentTermMatrix(corpus)
  removeSparseTerms(dtm, 0.1)
  df<-as.data.frame(as.matrix(dtm), stringsAsFactors=True)
  df$class<-f$class
  print (length(df$class ==1))
  positiveWeight = 1.0 / (nrow(subset(df, df$class == 1)) / nrow(df))
  negativeWeight = 1.0 / (nrow(subset(df, df$class != 1)) / nrow(df))
  modelWeights <- ifelse(df$class== 1, positiveWeight, negativeWeight)
  print(positiveWeight)
  print(negativeWeight)
  print (nrow(df))
  print(nrow(subset(df, df$class == 1)))
  
  #decision tree breaks if there is no negative examples..happens if parent type has only
  # one dress code
  
  if (nrow(df)==nrow(subset(df, df$class == 1)))
  {
    sink(file=rules_fullfilename)
    cat("Rule number: 1 [class=1 cover=%d (100%) prob=1.00]",nrow(df))
    cat("  product_type-%s>=0.5","all")
    sink()
    next
  }
  tree = rpart(class~.,  method = "class", data = df,minbucket=5,weights =modelWeights,control = rpart.control(minsplit = 4) );  
  rpart.plot(tree, box.palette="RdBu", shadow.col="gray", nn=TRUE,main=filename)
  printcp(tree)
  rpart.rules(tree)
  sink(file=rules_fullfilename)
  asRules(tree)
  sink()
}





