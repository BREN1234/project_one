from basetext import *
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning) 


class SentimentAnalyzer(BaseText):
    def __init__(self,path):
        super(SentimentAnalyzer,self).__init__(path,Tone=False)
    def main(self):
        #separating the predictor and response variable
        self.textX= self.textData['content'].map(self.clean_text)
        print("Cleaning over")
        self.textY= self.textData['ParallelSentiment']
        #mapping the labels to numeric interger
        #'negative',  'positive' ,'neutral'
        self.textY=self.textY.map({"negative":0,"positive":1,"neutral":2}).astype(int)
        #f=pd.DataFrame({"content":self.textX,"sentiment":self.textY})
        #f.to_csv("D:/Datasets/TextDataset/SentimentDatasets/refined.csv",index=False)
        #print("written")
        self.emotion= np.sort(self.textY.unique())
        #print(self.emotion,"Classes")
        self.trainData=self.transformerTrainData(self.textX) #returns tf-idf sparse matrix
        #x,y= self.sm.fit_sample(self.transformerTrainData(self.textX),self.textY)
        #x,y= self.smenn.fit_sample(self.transformerTrainData(self.textX),self.textY)
        #self.x,self.y= self.smUnder.fit_sample(self.transformerTrainData(self.textX),self.textY)
        self.x,self.y= self.transformerTrainData(self.textX),self.textY
        print("transformation over")
        #counting number of sentiments
        unique, counts = np.unique(self.y, return_counts=True)
        print (np.asarray((unique, counts)).T)
        self.y=self.y_categorical=to_categorical(self.y)
        self.models=[]
        #K-Fold Validation
        self.folds= KFold(n_splits=10,random_state=100,shuffle=True)
        self.i=0;
        for trainIndex, valIndex in self.folds.split(self.x):
            dim= self.x.shape[1] #length of features
            model=Sequential()
            model.add(Dense(2,activation='relu',input_dim=dim))
            #model.add(Dense(20,activation='relu'))
            #model.add(Dense(len(self.textY.unique()),activation='softmax'))
            model.add(Dense(self.y.shape[1],activation='softmax'))
            model.compile(loss='categorical_crossentropy',optimizer='Adadelta',metrics=['accuracy'])
            self.history=model.fit(self.x[trainIndex].toarray(),self.y_categorical[trainIndex],epochs=10,batch_size=5000,verbose=0,\
                     validation_data=(self.x[valIndex],self.y_categorical[valIndex]))
            self.i=self.i + 1
            loss,accuracy= model.evaluate(self.x[valIndex],self.y_categorical[valIndex],verbose=False)
            print("The Model Accuracy on Training is {}".format(accuracy))
            #predicted= model.predict(self.x[valIndex].toarray())
            self.models.append(model)
            self.plot_history(self.history)
            """
            #model=#svm.SVC(kernel='rbf')####LogisticRegression(solver='newton-cg',multi_class='multinomial')
            model=MultinomialNB()#RandomForestClassifier(n_estimators=100,criterion='gini')
            #MultinomialNB()#XGBClassifier(max_depth=50,n_estimators=10,random_state=100)
            model.fit(x[trainIndex],y[trainIndex])
            #saving the model for future uses
            #joblib.dump(model,"LogisticRegressionModel_"+str(self.i)+".pkl")
            self.i=self.i + 1
            predicted= model.predict(x[valIndex])
            print("Accuracy:",accuracy_score(y_pred=predicted,y_true=y[valIndex]))
            print("Confusion Matrix: \n",confusion_matrix(y_pred=predicted,y_true=y[valIndex]))
            self.models.append(model)  
            """
    #this is the function which classifies the user's inpu        
    def testSentence(self,sentence):
         #clean and convert to tfidf
        self.test_text=self.clean_text(sentence)
        print("After Clean: ",self.test_text)
        #self.test_text=" ".join([word if self.spellChecker.spell(word) else self.spellChecker.suggest(word)[0]                                  for word in self.test_text.split()])
        #print("Test sentence is Corrected as: ",self.test_text)
        self.tfidf_matrix=self.tfIdf.transform(self.vect.transform([self.test_text]))
        #np.zeros([10,1,2],dtype='float')
        #number of models, number of sentence,unique class
        self.resultTable= np.zeros([len(self.models),self.tfidf_matrix.shape[0],len(self.textY.unique())])
        i=0
        for model in self.models:
            self.resultTable[i,:,:]= model.predict_proba(self.tfidf_matrix.toarray())
            i=i+1
        self.finalResult=self.resultTable.sum(axis=0)/len(self.models)
        self.resultDataFrame=pd.DataFrame(data=self.finalResult[0],index=self.emotion,columns=[self.test_text])
        #print("Sentiment Df1:",self.resultDataFrame)
        return(self.resultDataFrame)

