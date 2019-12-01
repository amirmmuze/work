library("arules")

files = list.files(path = "../data_items", pattern = NULL, all.files = FALSE,
           full.names = FALSE, recursive = FALSE,
           ignore.case = FALSE, include.dirs = FALSE, no.. = FALSE)
n=length(files)
for (i in 1:n)
{   
    filename= files[i]
    print (filename)
    prefix_filename = unlist(strsplit(filename,"[#]"))[1]
    dress_code  = unlist(strsplit(filename,"[#]"))[2]
    postfix_filename = unlist(strsplit(filename,"[#]"))[3]
    print(dress_code)
    rules_fullfilename = paste0(full_rulespath,"/",prefix_filename,dress_code,"_rules",postfix_filename)
    print(rules_fullfilename)
    

full_filespath = "~/rules/data_items"
full_rulespath = "~/rules/data_rules"
full_filename = paste0(full_filespath,"/",filename) 

tr <- read.transactions(full_filename, sep=",", skip = 0)
rules <- apriori(tr, parameter = list(supp = 0.01, conf = 0.8,minlen=2,maxlen=8,target = "rules"),appearance = list(rhs=c(dress_code)))
rules<-unique(rules)
#rules<-rules[!is.redundant(rules)]
inspect(rules)
summary(rules)


#maximal rules
maximal <- is.maximal(rules)
inspect(rules[maximal])
summary(rules[maximal])
max_ruls<-rules[maximal]

max_ruls <- sort(max_ruls, by = "count")

write(max_ruls,
      file = rules_fullfilename,
      sep = ",",
      quote = TRUE,
      row.names = FALSE)

#df <-read.csv(rules_fullfilename)
#View(df)
}
