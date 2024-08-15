from nltk.tokenize import TweetTokenizer
import pandas
import codecs
from nltk.corpus import stopwords
import numpy as np
from sklearn.neighbors.nearest_centroid import NearestCentroid


topic = "Atheism"
pathTrainingSet = "train.txt"
pathTestSet = "test.txt"
stopwordList = set(stopwords.words('english'))
vocabularyTupleSet = set()
tknzr = TweetTokenizer()
classifier = None

def main():
    classifier = initClassifier()
    (matrix, actualStance ) = calculateTestMatrix()
    calculatedStance = classifier.predict(matrix)

    calculateF1(calculatedStance, actualStance)


def initClassifier():
    # read File
    doc = codecs.open(pathTrainingSet, 'r', 'UTF-8')
    df = pandas.read_csv(doc, sep='\t')

    # cut to topic
    df = df[df.Target == topic]

    #tokenize
    tweetsTokenList = []

    for index, row in df.iterrows():
        tweetTokens = tokenize(row["Tweet"])
        tweetsTokenList.append(tweetTokens)

        # build tupleVocabulary
        for word1, word2 in zip(tweetTokens[:-1], tweetTokens[1:]):
            vocabularyTupleSet.add((word1, word2))


    #create vectorMatrix with bigram count
    tweetCount = len(df.index)
    bigramCount = len(vocabularyTupleSet)
    vocabularyTupleList = list(vocabularyTupleSet)

    bigramMatrix = np.zeros(shape=(tweetCount, bigramCount))

    rowIndex = 0

    for tweetTokens in tweetsTokenList:

        for word1, word2 in zip(tweetTokens[:-1], tweetTokens[1:]):
            position = vocabularyTupleList.index((word1, word2))
            bigramMatrix[rowIndex][position] += 1
        rowIndex += 1

    #train classifier
    actualStances = np.array(df["Stance"])
    clf = NearestCentroid()
    clf.fit(bigramMatrix, actualStances)
    return clf

def calculateTestMatrix():
    # read File
    doc = codecs.open(pathTestSet, 'r', 'UTF-8')
    df = pandas.read_csv(doc, sep='\t')

    # cut to topic
    df = df[df.Target == topic]

    # tokenisation of target tweets
    tweetsTokenList = []

    for index, row in df.iterrows():
        tweetTokens = tokenize(row["Tweet"])

        tweetsTokenList.append(tweetTokens)

     # create vectorMatrix with bigram count
    tweetCount = len(df.index)
    bigramCount = len(vocabularyTupleSet)
    tupleVocabularyList = list(vocabularyTupleSet)

    bigramMatrix = np.zeros(shape=(tweetCount, bigramCount))

    rowIndex = 0

    for tweetTokens in tweetsTokenList:

        for word1, word2 in zip(tweetTokens[:-1], tweetTokens[1:]):
            if (word1, word2) not in tupleVocabularyList: continue
            position = tupleVocabularyList.index((word1, word2))
            bigramMatrix[rowIndex][position] += 1
        rowIndex += 1

    return (bigramMatrix, df["Stance"])


def tokenize(tweet):
    tweetTokens = tknzr.tokenize(tweet)
    tweetTokens = [i.lower() for i in tweetTokens if i not in stopwordList]
    return tweetTokens

def calculateF1(calculatedStances, actualStances):
    calculatedStances = list(calculatedStances)
    actualStances = list(actualStances)

    stances = set(calculatedStances + actualStances)

    for possibleStance in stances:
        (tp, fn, fp) = (0.,0.,0.)
        i = 0
        for actualStance, calculatedStance in zip(actualStances, calculatedStances):
            if(actualStance == possibleStance or actualStance == possibleStance):
                if(actualStance == calculatedStance): tp += 1
                else:
                    if (actualStance == possibleStance): fn += 1
                    if (calculatedStance == possibleStance): fp += 1

                    #print (actualStance, calculatedStance,i)
            i += 1
        print (possibleStance, tp, fp, fn, 2*tp/(2*tp + fn +fp))


main()