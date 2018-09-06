from tool import selectToDic, updateOneIdKV, webImgToBase64Str
import json, time

# 全部访问路径特征清单
# 可追加，不删除
passPaths = ['sonhoo.com/wukong/', 'cnhan.com/shantui/', 'cnhan.com/hyzx/', 'cnhan.com/pinfo/', 'heze.cn/info/',
             'heze.cn/qiye/']
#  'cnhan.com/shantui/'  调用nodejs解密

# 本次程序实际处理的访问路径特征
# 可追加，可删除
dealPaths = ['sonhoo.com/wukong/', 'cnhan.com/shantui/', 'cnhan.com/hyzx/', 'cnhan.com/pinfo/', 'heze.cn/info/',
             'heze.cn/qiye/']
for i in dealPaths:
    del passPaths[passPaths.index(i)]

delImgSwitcher = True if 2 > 61 else False

spiderDate = time.strftime("%Y%m%d", time.localtime()) if 11 > 2 else '20180901'
mongoWhere = {'spiderDate': spiderDate} if 61 > 2 else {}

# 请求图片
# 本地不保存，二进制转为base64后直接写入mongodb
cleanData = selectToDic('_id', 'todayUrls', fields={'url': 1, 'telImg': 1, 'phoneImg': 1, 'wxImg': 1},
                        where=mongoWhere)
for i in cleanData:
    if not delImgSwitcher:
        break
    _id = i
    item = cleanData[i]
    kl = ['telImg', 'phoneImg', 'wxImg']
    url = item['url']
    if 'cnhan.com/shantui' in url:
        continue
    for k in kl:
        kk = '{}base64'.format(k)
        # 字段存在，则认为，生成成功
        if kk in item:
            continue
        # if k in item and kk not in item:
        if k in item and 1:  # 重新生成
            imgUrl = item[k]
            if imgUrl == '':
                continue
            webImgToBase64Str(imgUrl)
            Base64Str = webImgToBase64Str(imgUrl)
            if Base64Str != '':
                updateOneIdKV(_id, '{}base64'.format(k), Base64Str)
                print(_id)
                print(item['url'])
            time.sleep(0.1)

# http://www.cnhan.com/hyzx/20180829/7138924.html 直接取comInfoTxt
# http://www.cnhan.com/shantui/mrOzcDD/news-63027.html  取comInfo，转json-dict结构
# http://www.cnhan.com/pinfo/company-72947-contact.html 直接取comInfoTxt

cleanData = selectToDic('_id', 'todayUrls', fields={'url': 1, 'comInfo': 1, 'comInfoTxt': 1, 'comName': 1},
                        where=mongoWhere)
for i in cleanData:
    _id = i
    # updateOne(_id, {'cleanData': {}}, 'todayUrls')
    item = cleanData[i]
    url = item['url']

    pathTag = 'cnhan.com/shantui/'
    if pathTag in url:
        if pathTag in passPaths:
            continue
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

    pathTag = 'cnhan.com/pinfo/'
    if pathTag in url:
        if pathTag in passPaths:
            continue
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
                elif '联系电话' in cc or '客服热线：' in cc:
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

    pathTag = 'cnhan.com/hyzx/'
    if pathTag in url:
        if pathTag in passPaths:
            continue
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
            for _ in c:
                # comName,contactName,addr,telPhone,mobilePhone,qq,website
                cc = _.replace(' ', '').replace('    ', '')
                print(_)
                print(cc)
                if '传真：' in cc or 'FAX' in cc:
                    d['fax'] = cc.split('：')[-1]
                elif '手机：' in cc or 'MOB' in cc:
                    d['mobilePhone'] = cc.split('：')[-1]
                elif '电话：' in cc or '热线：' in cc or 'TEL' in cc:
                    d['telPhone'] = cc.split('：')[-1]
                elif '地址：' in cc:
                    d['addr'] = cc.split('：')[-1]
                elif '网站：' in cc or '官网：' in cc or '网址：' in cc or 'URL' in cc:
                    d['webSite'] = cc.split('：')[-1].replace('URL:', '')
                elif '联系：' in cc or '联系人：' in cc:
                    d['contactName'] = cc.split('：')[-1]
            for k in d:
                v = d[k]
                updateOneIdKV(_id, k, v)

    pathTag = 'heze.cn/info/'
    if pathTag in url:
        if pathTag in passPaths:
            continue
        if 'comInfoTxt' in item:
            if 'comName' in item:
                comName = item['comName']
                c = item['comInfoTxt'].split(comName)[-1].split('\n')
            else:
                c = item['comInfoTxt'].split('\n')
            d = {}
            d['comName'] = comName

            for _ in c:
                # comName,contactName,addr,telPhone,mobilePhone,qq,website
                cc = _.replace(' ', '').replace('    ', '')
                print(_)
                print(cc)
                if '公司所在地：' in cc:
                    d['addr'] = cc.split('：')[-1]
                elif '网站：' in cc or '官网：' in cc or '网址：' in cc or 'URL' in cc:
                    d['webSite'] = cc.split('：')[-1]
                elif '联系：' in cc or '联系人：' in cc:
                    d['contactName'] = cc.split('：')[-1]
            for k in d:
                v = d[k]
                updateOneIdKV(_id, k, v)

    pathTag = 'heze.cn/qiye/'
    if pathTag in url:
        if pathTag in passPaths:
            continue
        if 'comInfoTxt' in item:
            c = item['comInfoTxt'].split('\n')
            d = {}
            updateOneIdKV(_id, 'addr', '')
            updateOneIdKV(_id, 'webSite', '')
            for _ in c:
                # comName,contactName,addr,telPhone,mobilePhone,qq,website
                cc = _.replace(' ', '').replace('    ', '')
                print(_)
                print(cc)
                if '地址:' in cc:
                    d['addr'] = cc.split(':')[-1]
                elif '企业官网' in cc:
                    d['webSite'] = cc.split(':')[-1]
                elif '联系人' in cc:
                    d['contactName'] = cc.split(':')[-1]
            for k in d:
                v = d[k]
                updateOneIdKV(_id, k, v)
'''
废除清洗后的数据聚合到mongodb-collection-一个key
        updateOne(_id, {'cleanData': d}, 'todayUrls')
'''
