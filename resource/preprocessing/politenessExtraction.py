__author__ = 'jirayus-j'

import sys
import time
import psycopg2
import os
import cPickle
reload(sys)
sys.setdefaultencoding('utf8')

try:
    import numpy as np
except:
    sys.stderr.write("Package not found: Politeness model requires python package numpy\n")
    sys.exit(2)

try:
    import scipy
    from scipy.sparse import csr_matrix
except:
    sys.stderr.write("Package not found: Politeness model requires python package scipy\n")
    sys.exit(2)

try:
    import sklearn
except:
    sys.stderr.write("Package not found: Politeness model requires python package scikit-learn\n")
    sys.exit(2)

try:
    import nltk
except:
    sys.stderr.write("Package not found: Politeness model requires python package nltk\n")
    sys.exit(2)

####
# Check versions for sklearn, scipy, numpy, nltk
# Don't error out, just notify

packages2versions = [("scikit-learn", sklearn, "0.15.1"), ("numpy", np, "1.9.0"), ("nltk", nltk, "3.0.0"), ("scipy", scipy, "0.12.0")]

for name, package, expected_v in packages2versions:
    if package.__version__ < expected_v:
        sys.stderr.write("Warning: package '%s', expected version >= %s, detected %s. Code functionality not guaranteed.\n" % (name, expected_v, package.__version__))


####

from features.vectorizer import PolitenessFeatureVectorizer


####
# Serialized model filename

MODEL_FILENAME = os.path.join(os.path.split(__file__)[0], 'politeness-svm.p')

####
# Load model, initialize vectorizer

clf = cPickle.load(open(MODEL_FILENAME))
vectorizer = PolitenessFeatureVectorizer()

def score(request):
    """
    :param request - The request document to score
    :type request - dict with 'sentences' and 'parses' field
        sample (taken from test_documents.py)--
        {
            'sentences': [
                "Have you found the answer for your question?", 
                "If yes would you please share it?"
            ],
            'parses': [
                ["csubj(found-3, Have-1)", "dobj(Have-1, you-2)", "root(ROOT-0, found-3)", "det(answer-5, the-4)", "dobj(found-3, answer-5)", "poss(question-8, your-7)", "prep_for(found-3, question-8)"], 
                ["prep_if(would-3, yes-2)", "root(ROOT-0, would-3)", "nsubj(would-3, you-4)", "ccomp(would-3, please-5)", "nsubj(it-7, share-6)", "xcomp(please-5, it-7)"]
            ]
        } 

    returns class probabilities as a dict
        {
            'polite': float, 
            'impolite': float
        }
    """
    # vectorizer returns {feature-name: value} dict
    features = vectorizer.features(request)
    fv = [features[f] for f in sorted(features.iterkeys())]
    # Single-row sparse matrix
    X = csr_matrix(np.asarray([fv]))
    probs = clf.predict_proba(X)
    # Massage return format
    probs = {"polite": probs[0][1], "impolite": probs[0][0]}
    return probs

def getIdToProcess():
    # get a connection, if a connect cannot be made an exception will be raised here
    conn = psycopg2.connect(conn_string)

    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    cursor = conn.cursor()
    print ("select id from question_features where \"politeness\" == 0")
    #"Process query\nselect id,\"tokenizedSentence\",\"dependencyParsed\" from question_preprocess where id >= "+str(numIteration*numQuery)+" and id < "+str((numIteration+1)*numQuery)+" and \"tokenizedSentence\" is not null and \"dependencyParsed\" is not null "

    cursor.execute("select id from question_features where \"politeness\" == 0")

    print "Get text data to process politeness score"


    outputId = []
    for row in cursor:
        # row[0] id
        outputId.append(row[0])
    cursor.close()
    conn.close()
    print "Finish getting id to process politeness"
    return outputId

def execute(listTargetId):
    # get a connection, if a connect cannot be made an exception will be raised here
    conn = psycopg2.connect(conn_string)

    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    cursor = conn.cursor()
    print ("select id,\"tokenizedSentence\",\"dependencyParsed\" from question_preprocess where \"tokenizedSentence\" is not null and \"dependencyParsed\" is not null")
    #"Process query\nselect id,\"tokenizedSentence\",\"dependencyParsed\" from question_preprocess where id >= "+str(numIteration*numQuery)+" and id < "+str((numIteration+1)*numQuery)+" and \"tokenizedSentence\" is not null and \"dependencyParsed\" is not null "

    cursor.execute("select id,\"tokenizedSentence\",\"dependencyParsed\" from question_preprocess where \"tokenizedSentence\" is not null and \"dependencyParsed\" is not null")

    print "Get text data to process politeness score"

    
    updateEntity = []
    for row in cursor:
        # row[0] id 
        # row[1] tokenizedSentence 
        # row [2] dependency Parsed
        if(listTargetId.__contains__(row[0])==False):
           continue
        sentences = row[1].split("\n")
        parses = row[2].split("\n")
        for i in range (0,len(parses)):
            parses[i] = parses[i].split("|")
            parses[i] = filter(None, parses[i])
        doc = {'sentences': sentences, 'parses': parses}
        #print doc
        politenessScore = score(doc)
        #print politenessScore
        updateEntity.append({'id': row[0],'positive': politenessScore['polite']})
        print ("Id : "+str(row[0])+" Politeness : "+str(politenessScore['polite']))

    cursor.close()
    conn.close()
    print "Finish preprocessing\nStart updating data to DB"

    updateConn = psycopg2.connect(conn_string)
    updateCursor = updateConn.cursor()
    updateCursor.executemany(
    '''
        UPDATE question_features
        SET
            \"politeness\" = %(positive)s
        WHERE
            id = %(id)s
    ''',
    updateEntity
    )
    updateConn.commit()
    print "Finish updating the DB"
    updateCursor.close()
    updateConn.close()

#Define our connection string
conn_string = "host='163.221.172.217' dbname='so_msr2015' user='oss' password='kenjiro+'"

while True:
    print "Start execution : %s" % time.ctime()
    # print the connection string we will use to connect
    print "Connecting to database\n	->%s" % (conn_string)
    targetId = getIdToProcess()
    execute()
    print "Finish execution : %s" % time.ctime()
    print "Sleep the program for 1 hour"
    time.sleep( 3600 )
