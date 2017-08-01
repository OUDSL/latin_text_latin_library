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
import unicodecsv as csv
import numpy as np
import csv
from nltk import sent_tokenize
from elasticsearch import Elasticsearch
es = Elasticsearch(['localhost:9200'])

regex = re.compile(r'[\n\r\t]')

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
        res = es.index(index=index, doc_type=doctype, body=doc, request_timeout=60)
        print(filename, esindex+1, " inserted: ",res['created'])


if __name__ == '__main__':
    inputfile = sys.argv[1]

    sinput = pd.read_csv(inputfile, encoding="ISO-8859-1")  # Read meta from input file
    Filelist = getFileList() # returns the list which has directories for all files

    for index, row in sinput.iterrows(): # iterate over the input fil
        author = row['author'].lower()
        filename = row['filename'] # get the filename in each
        url = row['url']
        if (len(url.split('/')) >4):
            temp = url.split('/')[-2]+'/'+url.split('/')[-1]
            temp = temp.replace(".html",".txt")
            temp = temp.replace(".shtml",".txt")
            dir ='./latin_text_latin_library/' + temp
        else:
            temp = url.split('/')[-1]
            temp = temp.replace(".html", ".txt")
            temp = temp.replace(".shtml", ".txt")
            dir ='./latin_text_latin_library/' + temp

        indices = [i for i, x in enumerate(Filelist) if temp in x]

        try:
            es_load(Filelist[indices[0]], row)
        except:
            print("ERROR:","filename:", filename)