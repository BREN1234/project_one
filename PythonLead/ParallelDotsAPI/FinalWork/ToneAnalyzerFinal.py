from basetext import *
from collections import Counter
import warnings
warnings.filterwarnings("ignore",category=DeprecationWarning)

class ToneAnalyzer(BaseText):
    def __init__(self,path):
        super(ToneAnalyzer,self).__init__(path,Tone=True)
    def main(self):
        #separating the predictor and response variable
        dft=pd.read_csv("DefteamTone.csv")
        #self.textData['content']=self.textData['content'].append(dft.content)
        #self.textData['ParallelTone']=self.textData['ParallelTone'].append(dft.emotion)
        #appending new datasets
        self.newTextX=dft['content'].map(self.clean_text)
        self.textX= self.textData['content'].map(self.clean_text)
        
        self.textX=self.textX.append(self.newTextX)
        print("After cleaning\n",pd.Series(' '.join(self.textX).split()).value_counts())
        self.textY= self.textData['ParallelTone'].append(dft.emotion)
        self.emotion= self.textY.unique()
        #mapping the labels to numeric interger
        print("All Emotions: ",self.emotion)
        self.textY=self.textY.map({"Bored":0,"Sad":1,"Fear":2,"Angry":3,"Excited":4,"Happy":5}).astype(int)
        self.models=[]
        #self.sm=SMOTE(random_state=100)#SMOTEENN(random_state=100)#
        #x,y= self.sm.fit_resample(self.transformerTrainData(self.textX),self.textY)
        x,y= self.transformerTrainData(self.textX),self.textY
        print('Resampled dataset shape %s' % Counter(y))
        self.model= LogisticRegression(solver='newton-cg',multi_class='multinomial')
        #XGBClassifier(max_depth=100,n_estimators=100,random_state=100)#
        #print(self.model)
        self.model.fit(x,y)
        #print(self.model)
        
    def testSentence(self,sentences):
        self.testDataDf= pd.DataFrame()
        self.testDataDf['content']=[sentences]
        self.testCasesData= self.testDataDf.content.map(self.clean_text)
        print("Tone in Test sentence : ",self.testCasesData.shape)
        self.resultTable= np.zeros([1,len(self.emotion),self.testCasesData.shape[0]],dtype='float')
        self.predicted= self.model.predict_proba(self.transformerTestData(self.testCasesData)).T
        print("Tone Model and predicts: ",self.predicted,self.model)
        self.resultTable[0,:,:]=self.predicted
        self.probabilty_of_emotion= (self.resultTable.sum(axis=0))
        print("Tone Probabilities:",self.probabilty_of_emotion)
        self.resultDataFrame=pd.DataFrame(data=self.probabilty_of_emotion,index=self.emotion,columns=self.testDataDf.content)
        print("Tone Dataframe: ",self.resultDataFrame)
        return(self.resultDataFrame)
class ToneAnalyzerTensorFlow(BaseText):
    def __init__(self,path):
        super(ToneAnalyzerTensorFlow,self).__init__(path,Tone=True)
    def main(self):
        dft=pd.read_csv("DefteamTone.csv")
        self.newTextX=dft['content'].map(self.clean_text)
        self.textX= self.textData['content'].map(self.clean_text)        
        self.textY= self.textData['ParallelTone'].append(dft.emotion)
        self.emotion= self.textY.unique()
        self.textY=self.textY.map({"Sad":0,"Angry":1,"Excited":2,"Happy":3}).astype(int)
        #self.X,self.Y= self.sm.fit_sample(self.transformerTrainData(self.textX),self.textY)
        self.X,self.Y= self.transformerTrainData(self.textX),self.textY
        #self.y_categorical =to_categorical(self.textY)
        #print(Counter(self.Y))
        #change to one hot encoder
        self.Y=self.y_categorical =to_categorical(self.Y)
        
        print("shape: ",self.X.shape,self.Y.shape,self.emotion)
        #self.trainData=self.transformerTrainData(self.X) #returns tf-idf sparse matrix
        print("transformation over")
        self.models=[]
        #K-Fold Validation
        self.folds= KFold(n_splits=10,random_state=100,shuffle=True)
        self.i=0;
        for trainIndex, valIndex in self.folds.split(self.X):
            dim= self.X.shape[1] #length of features
            model=Sequential()
            model.add(Dense(4,activation='relu',input_dim=dim))
            #model.add(Dense(20,activation='relu'))
            #model.add(Dense(len(self.textY.unique()),activation='softmax'))
            model.add(Dense(self.Y.shape[1],activation='softmax'))
            model.compile(loss='categorical_crossentropy',optimizer='Adadelta',metrics=['accuracy'])
            self.history=model.fit(self.X[trainIndex].toarray(),self.y_categorical[trainIndex],epochs=10,batch_size=5000,verbose=0,\
                      validation_data=(self.X[valIndex],self.y_categorical[valIndex]))
            self.i=self.i + 1
            #validation results
            loss, accuracy = model.evaluate(self.X[valIndex], self.y_categorical[valIndex], verbose=False)
            print("the Training evaluation accuracy: {}".format(accuracy))
            #test dataset result
            #predicted= model.predict(self.X[valIndex].toarray())
            self.models.append(model)
            self.plot_history(self.history)
    #return np.argmax(datum)
    def testSentence(self,sentence):
        #clean and convert to tfidf
        self.test_text=self.clean_text(sentence)
        print("After Clean: ",self.test_text)
        #self.test_text=" ".join([word if self.spellChecker.spell(word) else self.spellChecker.suggest(word)[0]                                  for word in self.test_text.split()])
        #print("Test sentence is Corrected as: ",self.test_text)
        self.tfidf_matrix=self.tfIdf.transform(self.vect.transform([self.test_text]))
        #np.zeros([10,1,2],dtype='float')
        #number of models, number of sentence,unique class
        #print("Number of unique Class: ",len(self.textY.unique()))
        self.resultTable= np.zeros([len(self.models),self.tfidf_matrix.shape[0],self.Y.shape[1]])
        i=0
        for model in self.models:
            self.resultTable[i,:,:]= model.predict_proba(self.tfidf_matrix.toarray())
            i=i+1
        print("from Test",self.resultTable)
        self.finalResult=self.resultTable.sum(axis=0)/len(self.models)
        #print(self.finalResult)
        self.resultDataFrame=pd.DataFrame(data=self.finalResult[0],index=self.emotion,columns=[self.test_text])
        #print(self.resultDataFrame)
        return(self.resultDataFrame)
