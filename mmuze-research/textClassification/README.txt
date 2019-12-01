Folder to generate proof of concepts for text classification.

1. Run - python preProcess.py ARG1 - example: python preProcess.py data
ARG1 = the name of a csv file (without ".csv") whose lines are data examples, each data example is label(string) and text(string)
example is data.csv attached

- The script outputs train.csv, dev.csv, test.csv and label_to_id.csv files which are to be used in the script below
the text is cleaned, rare labels (less than 10 samples) are removed and samples whose label is not frequent (< 100) are augmented

2. Run - python trainClassifier.py ARG2 ARG3 ARG4 - example: python trainClassifier.py train dev label_to_id 
ARG2 = the name of a csv file (without ".csv") which holds the training examples, each line has label (integer), text (string)
ARG3 = the name of a csv file (without ".csv") which holds the testing examples, each line has label (integer), text (string)
ARG4 = the name of a csv file (without ".csv") which holds the label to id mapping, each line is label (string), id (int)

- The script outputs classificationReport.csv
