
# coding: utf-8

# In[12]:


import pandas as pd,numpy as np
import re,emoji,json,pickle,nltk,string
from sklearn.naive_bayes import MultinomialNB,GaussianNB
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn import svm
from sklearn.feature_extraction.text import CountVectorizer,TfidfTransformer #converts to term-doc matrix
from sklearn.feature_extraction import text 
from sklearn.metrics import accuracy_score,confusion_matrix,f1_score,precision_score,recall_score,classification_report
from sklearn.model_selection import  train_test_split,KFold
from sklearn.preprocessing import normalize
from flask import Flask,url_for,render_template,jsonify,request
from nltk.stem.wordnet import WordNetLemmatizer
#from hunspell import Hunspell 
from sklearn.externals import joblib
from collections import OrderedDict
from imblearn.over_sampling import SMOTE
from imblearn.under_sampling import RandomUnderSampler # doctest: +NORMALIZE_WHITESPACE
from imblearn.combine import SMOTEENN
from xgboost.sklearn import XGBClassifier
import warnings


import matplotlib.pyplot as plt
plt.style.use('ggplot')

from keras.models import Sequential
from keras.layers import Dense
from keras.utils.np_utils import to_categorical
import tensorflow as tf

warnings.filterwarnings("ignore", category=DeprecationWarning) 

class BaseText:
    def __init__(self,path,Tone=True):
        #HCorrection of the words
        #for Linux
        #self.spellChecker = HunSpell('/usr/share/hunspell/en_US.dic','/usr/share/hunspell/en_US.aff')
        #for windows
        #self.spellChecker =Hunspell("en_US")
        self.emotion= None
        #lemmantizing the the word 
        self.lemmantizer = WordNetLemmatizer()
        #stop words
        stop_words = text.ENGLISH_STOP_WORDS
        #Sparse matrix of term and its feature indices like("goe":2345)
        self.vect= CountVectorizer(stop_words=stop_words,analyzer='word')
        #Converting the Sparse Matrix of term frequecy to tf-idf sparse Matrix
        self.tfIdf= TfidfTransformer(norm='l2')
        #reading the dataset
        self.textData= pd.read_csv(path)
        self.fre=pd.Series(' '.join(self.textData['content']).split()).value_counts()
        self.fre1=pd.Series(' '.join(self.textData['content']).split()).value_counts()
        self.freWord=list(self.fre.index[self.fre.values<=2500])
        self.freWord1=list(self.fre1.index[self.fre1.values>=2])
        self.textData['content']=pd.Series(self.textData['content'].map(lambda x: " ".join(w for w in x.split() if x  not in self.freWord)))
        self.textData['content']=pd.Series(self.textData['content'].map(lambda x: " ".join(w for w in x.split() if x  not in self.freWord1)))
        self.textData=self.textData[self.textData.content.isnull() != True]
        if Tone:
            self.textData=self.textData[['content','ParallelTone']]
            print(self.textData['ParallelTone'].value_counts())
            noList=['Bored','Fear']
            for t in noList:
                self.textData=self.textData[self.textData.ParallelTone!=t]
            print("After Removing the Some Tones",self.textData.ParallelTone.value_counts())
        else:
            self.textData=self.textData[['content','ParallelSentiment']]
            print(self.textData['ParallelSentiment'].value_counts())
        self.sm= SMOTE(random_state=100)
        self.smUnder=RandomUnderSampler(random_state=100)
        self.smenn=SMOTEENN(random_state=100)
        self.words = set(nltk.corpus.words.words())
    #function to remove emails and urls from the content
    def clean_text(self,input_text,test=False):
        try:
            self.input_text= " ".join(word  for word in input_text.split() if len(word) > 1)
            self.input_text= re.sub(r'[@&$_~`][a-zA-Z0-9]+', ' ',self.input_text)
            #removing URLS
            self.input_text= re.sub(r'http\S+', '',self.input_text)
            #removing punctuations
            table = str.maketrans('', '', string.punctuation)
            self.input_text=self.input_text.translate(table)
            #removing numbers
            self.input_text= re.sub(r'[0-9]', ' ',self.input_text)
            #replacing emojis with their meaning
            #self.input_text= re.findall(r'[^\w\s,]', self.input_text)
            #print(self.input_text)
            #print(self.input_)
            #self.input_text= "".join([emoji.demojize(word) for word in self.input_text])
            #Reoving the HTML tags
            #Correction of the spelling by hunspell
            #lemmantizing using verb
        except Exception as e:
            pass
        self.input_text= " ".join([self.lemmantizer.lemmatize(word,'n') for word in self.input_text.split()])
        self.input_text= " ".join([self.lemmantizer.lemmatize(word,'v') for word in self.input_text.split()])
        self.input_text= " ".join(w for w in nltk.wordpunct_tokenize(self.input_text) if w.lower() in self.words or not w.isalpha())
        
        return self.input_text

    def transformerTrainData(self,trainXX):
        trainX_tdm=self.vect.fit_transform(trainXX)
        #Normalized the term counts
        tfidf0= self.tfIdf.fit_transform(trainX_tdm)
        return( tfidf0)

    def transformerTestData(self,testX):
        #now transformthe test dataset
        testX_tdm= self.vect.transform(testX)
        #calculationg tf-idf weights
        tfidf1= self.tfIdf.transform(testX_tdm)
        return(tfidf1)
    def manualTesting(self,inputText1):
        inputText1= self.vect.transform([self.clean_text(inputText1)])
        tfidf2= self.tfIdf.transform(inputText1)
        return(tfidf2)
    

    def plot_history(self,history):
        acc = history.history['acc']
        val_acc = history.history['val_acc']
        loss = history.history['loss']
        val_loss = history.history['val_loss']
        x = range(1, len(acc) + 1)

        plt.figure(figsize=(12, 5))
        plt.subplot(1, 2, 1)
        plt.plot(x, acc, 'b', label='Training acc')
        plt.plot(x, val_acc, 'r', label='Validation acc')
        plt.title('Training and validation accuracy')
        plt.legend()
        plt.subplot(1, 2, 2)
        plt.plot(x, loss, 'b', label='Training loss')
        plt.plot(x, val_loss, 'r', label='Validation loss')
        plt.title('Training and validation loss')
        plt.legend()
    #training the Model on the dataset

