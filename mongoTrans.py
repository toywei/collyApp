from tool import *
import re

'''
危险区，操作不可逆
开始--》
'''
# 批量删除
# deleteMany({'spiderDate': '20180903'}, 'todayUrls')
'''
危险区，操作不可逆
结束《--
'''
'''
以下为
可逆操作
'''

# 更新日期字符串冗余- .replace('-', '')
# 更新website字段冗余 .replace('URL:', '')
# cleanData = selectToDic('_id', 'todayUrls', fields={'webSite': 1, 'spiderDate': 1})
# cleanData = selectToDic('_id', 'todayUrls', fields={'Base64parse2times': { '$exists': True}})
if 11 > 2:
    collection_name = 'todayUrls'
    cleanData = selectToDic('_id', collection_name, fields={'url': 1}, where={'spiderDate': '20180906'})
    delIds = []
    for i in cleanData:
        _id = i
        item = cleanData[i]
        url = item['url']
        pathTag = 'cnhan.com/pinfo/'
        # 通过正则删除
        if pathTag in url and re.match('^http://www.cnhan.com/pinfo/\d+\.html$',
                                       url) is None:
            print(_id, url)
            deleteOne({'_id': _id}, collection_name)


def improve():
    cleanData = selectToDic('_id', 'todayUrls', fields={})
    for i in cleanData:
        _id = i
        item = cleanData[i]
        spiderDate = item['spiderDate']
        updateOneIdKV(_id, 'spiderDate', spiderDate.replace('-', ''))
        print('improve', _id)
        if 'webSite' in item:
            webSite = item['webSite']
            updateOneIdKV(_id, 'webSite', webSite.replace('URL:', ''))


def uniqueUrlSpiderDate(collectionMame='todayUrls'):
    '''
    当日url + spiderDate留其一
    :return:
    '''
    spiderDate_url_set = {}
    cleanData = selectToDic('_id', collectionMame, fields={'spiderDate': 1, 'url': 1})
    for i in cleanData:
        _id = i
        item = cleanData[i]
        url, spiderDate = item['url'], item['spiderDate']
        k = url + spiderDate
        if k not in spiderDate_url_set:
            spiderDate_url_set[k] = []
        spiderDate_url_set[k].append(_id)

    save_id_l = []
    for k in spiderDate_url_set:
        save_id_l.append(spiderDate_url_set[k][0])
    for i in cleanData:
        _id = i
        if _id not in save_id_l:
            deleteOne({'_id': _id}, collectionMame)
            print('uniqueUrlSpiderDate', _id)


if __name__ == "__main__":
    improve()
    uniqueUrlSpiderDate()
    uniqueUrlSpiderDate('siteUserPage')
