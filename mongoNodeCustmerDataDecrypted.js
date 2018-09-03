// crypto-js - npm https://www.npmjs.com/package/crypto-js
const CryptoJS = require('crypto-js')
const customerDecode = (str) => {
    return CryptoJS.enc.Base64.parse(CryptoJS.enc.Base64.parse(str).toString(CryptoJS.enc.Utf8)).toString(CryptoJS.enc.Utf8)
}

// mongodb - npm https://www.npmjs.com/package/mongodb
const mongoCfg = {
    uri: 'mongodb://hbaseU:123@192.168.3.103:27017/hbase',
    dbName: 'hbase',
    collectionName: 'todayUrls'
}

const MongoClient = require('mongodb').MongoClient
// npm install assert
const assert = require('assert')
// Use connect method to connect to the server
MongoClient.connect(mongoCfg.uri, function (err, client) {
    assert.equal(null, err)
    console.log('Connected successfully to server')
    const db = client.db(mongoCfg.dbName)
    const collectionName = mongoCfg.collectionName
    findDocuments(db, collectionName, {
        Base64parse2times: {
            $exists: true
        }
    }, function (docs) {
        for (let i in docs) {
            const ii = docs[i]
            const mgid = ii._id
            const filter = {
                _id: mgid
            }
            const comInfo = customerDecode(ii.Base64parse2times.replace('{"r":"', '').replace('"}', ''))
            const val = {
                $set: {
                    comInfo: comInfo
                }
            }
            updateDocument(db, collectionName, filter, val, function (result) {
                console.log(result)
            })
        }
        // 在回调中关闭数据库
        // 保证读写完全结束后关闭数据库
        // 以与之（读写完全结束）同步的方式关闭数据库
        client.close()
    })
    // 避免读写任务没有结束，异步任务没有完成，同步指令提前关闭数据库：'MongoError: server instance pool was destroyed'
    // client.close()
})

const findDocuments = function (db, collectionName, filter, callback) {
    // Get the documents collection
    const collection = db.collection(collectionName)
    // Find some documents
    collection.find(filter).toArray(function (err, docs) {
        assert.equal(err, null)
        console.log("Found the following records")
        console.log(docs.length)
        callback(docs)
    })
}

const updateDocument = function (db, collectionName, filter, val, callback) {

    // Get the documents collection
    const collection = db.collection(collectionName)
    collection.updateOne(filter, val, function (err, result) {
        console.log(err)
        assert.equal(err, null)
        assert.equal(1, result.result.n)
        callback(result)
    })
}