from pymongo import MongoClient
import base64, requests, time, random


def RandomString(min=16, max=32, letterBytes="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"):
    len_ = len(letterBytes) - 1
    RandomStringLen = random.randrange(min, max)
    s = ''
    for i in range(0, RandomStringLen):
        ii = random.randrange(0, len_)
        s += letterBytes[ii]
    print(s)
    return s


def webImgToBase64Str(imgUrl):
    try:
        r = requests.get(imgUrl)
        imgByte = r.content
        b64encode = base64.b64encode(imgByte)
        # 'data:image/jpg;base64,' OK
        Base64Str = 'data:image/png;base64,' + b64encode.decode('utf-8')
        return Base64Str
    except Exception as e:
        time.sleep(random.random() + 1)
        print(e)
        return ''


def selectToDic(k, collection_name, fields={}, where={},
                c=MongoClient("mongodb://hbaseU:123@192.168.3.103:27017/hbase"), dbName='hbase'):
    db = c[dbName]
    collection, r = db[collection_name], {}
    if fields == {}:
        cursor = collection.find(where)
    else:
        cursor = collection.find(where, fields)
    try:
        for doc in cursor:
            r[doc[k]] = doc
    finally:
        cursor.close()
    return r


def updateOne(filter_id, update, collection_name,
              c=MongoClient("mongodb://hbaseU:123@192.168.3.103:27017/hbase"), dbName='hbase'):
    try:
        db = c[dbName]
        collection = db[collection_name]
        collection.update_one({"_id": filter_id}, {'$set': update})
    except Exception as e:
        print(e)


# 便于在循环中使用逐个跟新
def updateOneIdKV(Id, k, v, tab='todayUrls'):
    print(k, v)
    updateOne(Id, {k: v}, tab)


def deleteMany(filte, collection_name,
               c=MongoClient("mongodb://hbaseU:123@192.168.3.103:27017/hbase"), dbName='hbase'):
    try:
        db = c[dbName]
        collection = db[collection_name]
        collection.delete_many(filte)
    except Exception as e:
        print(e)


# collection – Collection level operations — PyMongo 3.7.1 documentation http://api.mongodb.com/python/current/api/pymongo/collection.html
def deleteOne(filte, collection_name,
              c=MongoClient("mongodb://hbaseU:123@192.168.3.103:27017/hbase"), dbName='hbase'):
    try:
        db = c[dbName]
        collection = db[collection_name]
        collection.delete_one(filte)
    except Exception as e:
        print(e)
