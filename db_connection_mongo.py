#-------------------------------------------------------------------------
# AUTHOR: Andrew Perez
# FILENAME: db_connection_mongo.py
# SPECIFICATION: functionality for driver
# FOR: CS 4250- Assignment #3
# TIME SPENT: 3 hr
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
from pymongo import MongoClient

def connectDataBase():

    # Create a database connection object using pymongo
    DB_NAME = "CPP"
    DB_HOST = "localhost"
    DB_PORT = 27017
    try:
        client = MongoClient(host=DB_HOST, port=DB_PORT)
        db = client[DB_NAME]
        return db
    except:
        print("Database not connected successfully")

def createDocument(col, doc_Id, text, title, date, category):

    # create a dictionary indexed by term to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    edittext = text.lower()
    punc = '''!()-[]{};:'"\,<>./?@#$%^&*_~'''
    for word in edittext:
        if word in punc:
            edittext = edittext.replace(word, "")
    
    terms = {}
    for term in edittext.split():
        terms[term] = terms.get(term, 0) + 1

    # create a list of objects to include full term objects. [{"term", count, num_char}]
    terms_entries = []
    for term, count in terms.items():
        terms_entries.append({"term": term, "count": count, "num_char": len(term)})


    # produce a final document as a dictionary including all the required document fields
    document = {
        "doc_Id": doc_Id,
        "text": text,
        "title": title,
        "date": date,
        "category": category,
        "terms": terms_entries
    }

    # insert the document
    col.insert_one(document)

def deleteDocument(col, doc_Id):

    # Delete the document from the database
    col.delete_one({"doc_Id": doc_Id})

def updateDocument(col, doc_Id, text, title, date, category):

    # Delete the document
    col.delete_one({"doc_Id": doc_Id})

    # Create the document with the same id
    createDocument(col, doc_Id, text, title, date, category)

def getIndex(col):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    pipeline = [
        {"$unwind": "$terms"},
        {"$group": {"_id": "$terms.term", "docs": {"$addToSet": {"title": "$title", "count": "$terms.count"}}}}
    ]

    index = {}
    for doc in col.aggregate(pipeline):
        term = doc["_id"]
        counts = [f"{item['title']}:{item['count']}" for item in doc["docs"]]
        index[term] = ",".join(counts)

    return index
