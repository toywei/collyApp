from pymongo import MongoClient
import base64
import requests


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


def updateOneIdKV(Id, k, v, tab='todayUrls'):
    print(k)
    updateOne(Id, {k: v}, tab)


def webImgToBase64Str(imgUrl):
    try:
        r = requests.get(imgUrl)
        imgByte = r.content
        b64encode = base64.b64encode(imgByte)
        # 'data:image/jpg;base64,' OK
        Base64Str = 'data:image/png;base64,' + b64encode.decode('utf-8')
        return Base64Str
    except Exception as e:
        time.sleep(1)
        print(e)
        return ''

import time
# 请求图片
cleanData = selectToDic('_id', 'todayUrls', fields={'url': 1, 'telImg': 1, 'phoneImg': 1, 'wxImg': 1})
for i in cleanData:
    _id = i
    item = cleanData[i]
    kl = ['telImg', 'phoneImg', 'wxImg']
    for k in kl:
        if k in item:
            time.sleep(0.1)
            imgUrl = item[k]
            webImgToBase64Str(imgUrl)
            Base64Str = webImgToBase64Str(imgUrl)
            if Base64Str != '':
                updateOneIdKV(_id, '{}base64'.format(k), Base64Str)
                print(_id)
                print(item['url'])
                # print(imgUrl)
                # print(Base64Str)

# http://www.cnhan.com/hyzx/20180829/7138924.html 直接取comInfoTxt
# http://www.cnhan.com/shantui/mrOzcDD/news-63027.html  取comInfo，转json-dict结构
# http://www.cnhan.com/pinfo/company-72947-contact.html 直接取comInfoTxt

cleanData = selectToDic('_id', 'todayUrls', fields={'url': 1, 'comInfo': 1, 'comInfoTxt': 1, 'comName': 1})

for i in cleanData:
    break
    _id = i
    # updateOne(_id, {'cleanData': {}}, 'todayUrls')

    item = cleanData[i]
    url = item['url']
    # 字段是否在mongodb-doc中存在
    if 'comInfo' in item:
        ii = json.loads(item['comInfo'])['contactInfoDto']
        # comName,contactName,addr,telPhone,mobilePhone,qq,website
        try:
            k_t_d = {'comName': 'compName', 'contactName': 'contactName', 'telPhone': 'telPhone',
                     'mobilePhone': 'mobilePhone', 'addr': 'address', 'qq': 'qq1', 'webSite': 'webSite'}
            for k in k_t_d:
                k_k = k_t_d[k]
                if k_k in ii:
                    v = ii[k_k]
                    updateOneIdKV(_id, k, v)
        except Exception as e:
            print(e)
            print(item)
            print(item['url'])
    elif 'www.cnhan.com/pinfo/' in url:
        '''
        '
东莞市鹏诚包装制品有限公司
联系电话：0769- 85590686 
传真号码：0769- 85820218-0769
详细地址：东莞市厚街镇白濠村工业区
'
        '''
        if 'comInfoTxt' in item:
            if 'comName' in item:
                comName = item['comName']
                c = item['comInfoTxt'].split(comName)[-1].split('\n')
            else:
                c = item['comInfoTxt'].split('\n')
            d = {}
            d['comName'] = comName
            for cc in c:
                if '传真号码' in cc:
                    d['fax'] = cc.split('：')[-1]
                elif '手机号码' in cc:
                    d['mobilePhone'] = cc.split('：')[-1]
                elif '联系电话' in cc:
                    d['telPhone'] = cc.split('：')[-1]
                elif '详细地址' in cc:
                    d['addr'] = cc.split('：')[-1]
                elif '公司地址' in cc:
                    d['addr'] = cc.split('：')[-1]
                elif '网址' in cc:
                    d['webSite'] = cc.split('：')[-1]
            for k in d:
                v = d[k]
                updateOneIdKV(_id, k, v)
    elif 'www.cnhan.com/hyzx/' in url:
        '''
        '


联系人：李先生
电 话：+86-028-86677110
传 真：+86-028-85083456-806
手 机：15828112050
邮 编：610041
邮 箱：426263801@qq.com
官 网：http://www.cdjsa.com
地 址：四川省、成都市、武侯区、广福路99号1-2-307、308

'
        '''
        if 'comInfoTxt' in item:
            if 'comName' in item:
                comName = item['comName']
                c = item['comInfoTxt'].split(comName)[-1].split('\n')
            else:
                c = item['comInfoTxt'].split('\n')

            d = {}
            d['comName'] = comName

            for cc in c:
                # comName,contactName,addr,telPhone,mobilePhone,qq,website
                d = {}
                if '传 真' in cc:
                    d['fax'] = cc.split('：')[-1]
                elif '手 机' in cc:
                    d['mobilePhone'] = cc.split('：')[-1]
                elif '电 话' in cc:
                    d['telPhone'] = cc.split('：')[-1]
                elif '地 址' in cc:
                    d['addr'] = cc.split('：')[-1]
                elif '公司地址' in cc:
                    d['addr'] = cc.split('：')[-1]
                elif '官 网' in cc:
                    d['webSite'] = cc.split('：')[-1]
                elif '联系人' in cc:
                    d['contactName'] = cc.split('：')[-1]
            for k in d:
                v = d[k]
                updateOneIdKV(_id, k, v)
'''
废除清洗后的数据聚合到mongodb-collection-一个key
        if 'comInfoTxt' in item:
            if 'comName' in item:
                comName = item['comName']
                c = item['comInfoTxt'].split(comName)[-1].split('\n')
            else:
                c = item['comInfoTxt'].split('\n')

            d = {}
            d['comName'] = comName
            for cc in c:
                # comName,contactName,addr,telPhone,mobilePhone,qq,website
                d = {}
                if '传 真' in cc:
                    d['fax'] = cc.split('：')[-1]
                elif '手 机' in cc:
                    d['mobilePhone'] = cc.split('：')[-1]
                elif '电 话' in cc:
                    d['telPhone'] = cc.split('：')[-1]
                elif '地 址' in cc:
                    d['addr'] = cc.split('：')[-1]
                elif '公司地址' in cc:
                    d['addr'] = cc.split('：')[-1]
                elif '官 网' in cc:
                    d['webSite'] = cc.split('：')[-1]
                elif '联系人' in cc:
                    d['contactName'] = cc.split('：')[-1]


        updateOne(_id, {'cleanData': d}, 'todayUrls')
'''
