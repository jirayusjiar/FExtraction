__author__ = 'jirayus-j'

import sys
import psycopg2
import nltk.data

#Define our connection string
conn_string = "host='163.221.172.217' dbname='so_msr2015' user='oss' password='kenjiro+'"

# print the connection string we will use to connect
print "Connecting to database\n	->%s" % (conn_string)

# get a connection, if a connect cannot be made an exception will be raised here
conn = psycopg2.connect(conn_string)

# conn.cursor will return a cursor object, you can use this cursor to perform queries
cursor = conn.cursor()

cursor.execute("select id,parsedText from question_features")

print "Get text data to preprocess..."

for row in cursor:
    if targetId.__contains__(row[0]):
        print "Preprocess this text : "+row[1]


text = '''
Punkt knows that the periods in Mr. Smith and Johann S. Bach
do not mark sentence boundaries.  And sometimes sentences
can start with non-capitalized words.  i is a good variable
name.
'''
sent_detector = nltk.data.load('tokenizers/punkt/english.pickle')

print(sent_detector.tokenize(text.replace("\n"," ").strip()))


cursor.close()