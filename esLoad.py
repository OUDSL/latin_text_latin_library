import codecs
import nltk, string
import pandas as pd
import re
import sys
import os
import pandas as pd
import json
import requests
from glob import iglob
from datetime import datetime
import sys
import csv
import math
import numpy as np
import csv
from nltk import sent_tokenize
from elasticsearch import Elasticsearch
import logging


# detailed info logging
logging.basicConfig(filename='esloadlog.json',
                    filemode='w',
                    level = logging.DEBUG,
                    format="%(levelname) -10s %(asctime)s %(module)s:%(lineno)s %(funcName)s %(message)s",)

f = open("log.json", 'w')
sys.stdout = f
es = Elasticsearch(['localhost:9200'])

def getFileList():
    """
    Create a file directory list
    :return: filelist
    """
    filelist= []

    for path, subdirs, files in os.walk('.'): # goes through file paths for every file in the root directory
        for name in files:
            if not path.split('/')[-1].startswith(os.getcwd()):
                filename= os.path.join(path, name)
                filen,file_extension = os.path.splitext(filename)
                if file_extension.lower() == '.txt':
                    filelist.append(filename)

    return filelist


def tokenizeSentences(data):
    """
    # Tokenize the  raw latin texts. Parsing by period.
    :return: sentence list
    """
    sentences = sent_tokenize(data)  # parse sentences by period
    cleaned_sentences = [x for x in sentences if x != "."] # if the sentence has only '.', remove it from the list
    # TO-DOS: # Clean the text some more
    return cleaned_sentences #cleaned_sentences # return the list of sentences

def readTexts(filename):
    with codecs.open(filename,'r', encoding='utf-8', errors='ignore') as f:
        textblob= f.read()
    return tokenizeSentences(textblob)

def es_load(directory,row, index="latin",doctype="library"):
    filename = row['filename']
    author = row['author']
    title = row['title']
    url = row['url']
    data = readTexts(directory)
    for esindex, esrow in enumerate(data):
        doc = {"author": author, "filename": filename, "title": title, "sentence_id": esindex+1,
            "sentence": esrow, "url": url}
        res = es.index(index=index, doc_type=doctype, body=doc, request_timeout=180)
    print("filename:", filename.encode('utf-8'), "number of sentences: ",esindex+1, " inserted: ",res['created'])


if __name__ == '__main__':
    success = 0
    fail = 0
    failedList = []
    inputfile = sys.argv[1]
    #inputfile = "/Users/Andrew/github/esload/latin_text_latin_library/staging_data-1-2017-07-13.csv"
    sinput = pd.read_csv(inputfile, encoding="ISO-8859-1")  # Read meta from input file
    #Filelist = getFileList() # returns the list which has directories for all files

    for index, row in sinput.iterrows(): # iterate over the input file
        author = row['author'].lower()
        filename = row['filename'] # get the filename in each
        url = row['url']
        fullpath=row["filename"]
        if (len(url.split('/')) >4):
            fullpath = "{0}/{1}".format(url.split('/')[-2],filename)

        print("File{0}".format(index+1))
        try:
            if not os.path.exists(fullpath):
                print("ERROR: FILE NOT FOUND", "filename:", fullpath)
                fail+= 1
                failedList.append(fullpath)
            else:
                es_load(fullpath, row)
                success += 1
        except Exception as e:
            logging.exception("message")

    print("-----------------------------------------------------------------")
    print("Number of files inserted successfully:", success)
    print("Number of files failed to insert:", fail)
    print("\n")
    for p in failedList: print p
    print("-----------------------------------------------------------------")
    f.close()
