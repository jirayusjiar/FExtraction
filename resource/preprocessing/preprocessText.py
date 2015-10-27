__author__ = 'jirayus-j'

import sys
import psycopg2
import nltk.data
reload(sys)
sys.setdefaultencoding('utf8')

def execute(numQuery,numIteration):
    # get a connection, if a connect cannot be made an exception will be raised here
    conn = psycopg2.connect(conn_string)

    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    cursor = conn.cursor()
    print "Process query\nselect id,\"parsedText\" from question_preprocess where id >= "+str(numIteration*numQuery)+" and id < "+str((numIteration+1)*numQuery)

    cursor.execute("select id,\"parsedText\" from question_preprocess where id >= "+str(numIteration*numQuery)+" and id < "+str((numIteration+1)*numQuery))

    print "Get text data to preprocess..."

    sent_detector = nltk.data.load('tokenizers/punkt/english.pickle')

    updateContent = []
    for row in cursor:
        tokenizedText = '\n'.join(sent_detector.tokenize(row[1].replace("\n"," ").strip()))
        updateContent.append({'id': row[0], 'tokenized': tokenizedText})
    cursor.close()
    conn.close()
    print "Finish preprocessing\nStart updating data to DB"

    updateConn = psycopg2.connect(conn_string)
    updateCursor = updateConn.cursor()
    updateCursor.executemany(
    '''
        UPDATE question_preprocess
        SET
            \"tokenizedSentence\" = %(tokenized)s
        WHERE
            id = %(id)s
    ''',
    updateContent
    )
    updateConn.commit()
    print "Finish updating the DB"
    updateCursor.close()
    updateConn.close()

#Define our connection string
conn_string = "host='163.221.172.217' dbname='so_msr2015' user='oss' password='kenjiro+'"

# print the connection string we will use to connect
print "Connecting to database\n	->%s" % (conn_string)
for i in range(0,27):
    print "Iteration "+str(i)
    execute(1000000,i)
