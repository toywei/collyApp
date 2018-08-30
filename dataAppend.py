from pymongo import MongoClient
from  bs4 import BeautifulSoup
import requests
import time

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

spiderDate=time.strftime("%Y%m%d", time.localtime())
urlHtml = selectToDic('_id', 'todayUrls', fields={'url': 1, 'html': 1}, where={'spiderDate':spiderDate})
# urlHtml = selectToDic('_id', 'todayUrls', fields={'url': 1, 'html': 1},where={})


for i in urlHtml:
    _id = i
    url = urlHtml[i]['url'].replace('\n', '')
    updateOne(_id, {'url': url}, 'todayUrls')
    html = urlHtml[i]['html']

    # window.compId = "3644";
    comIdTag = 'window.compId = "'
    if comIdTag in html:

        compId = html.split(comIdTag)[-1].split('"')[0]
        updateOne(_id, {'comId': compId}, 'todayUrls')
        try:
            url = 'http://www.cnhan.com/shantui//dynamic/get/data/allCompanyInfoByCompId.json?compId={}'.format(compId)
            r = requests.get(url)
            comInfo = r.text
            updateOne(_id, {'comInfo': comInfo}, 'todayUrls')
        except Exception as e:
            print(e)

    # 客户资料在mogo的html中，但是没有联系方式详情页完整
    plainTag = 'http://www.cnhan.com/hyzx/'
    if plainTag in url:

        soup = BeautifulSoup(html, 'html.parser')
        _ = soup.find_all('a')
        contactTag = '../contact_'
        for i in _:
            if 'href' in i.attrs:
                href = i.attrs['href']
                if contactTag in href:
                    contactUrl = '{}{}'.format(plainTag, href.replace('../', ''))
                    updateOne(_id, {'contactUrl': contactUrl}, 'todayUrls')
                    try:
                        r = requests.get(contactUrl)
                        # 已经在页面校验,
                        # 1-出现则唯一;2-出现且出现其中的一个；
                        class_l = ['lxfs', 'cp_rcc', 'about', 'lx_c', 'describe', 'nsmsg', 'lxwmjs', 'newscon',
                                   'dis_content2', 'lxwm', 'case_right_box', 'mrb2_list', 'cen_lt']
                        for c_ in class_l:
                            findChk = BeautifulSoup(r.text, 'html.parser').find(class_=c_)
                            if findChk is not None:
                                comInfoTxt = findChk.text.replace('\n\n', '\n').replace('\t\t', '\t')
                                dropTag_tail_l = ['纬度']
                                for sp in dropTag_tail_l:
                                    comInfoTxt = comInfoTxt.split(sp)[0]
                                    comName = r.text.split('title>')[1].split('-')[0]
                                    print(comInfoTxt)
                                updateOne(_id, {'comName': comName, 'comInfoTxt': comInfoTxt}, 'todayUrls')
                    except Exception as e:
                        print('NOT-match')
                        print(e)
                        print(url)
                        print(contactUrl)

    # 客户联系电话在唯普通的可以ocr识别的图片中，但是联系方式详情页存在于可以直接获取的html中
    plainTag = 'http://www.heze.cn/info/'
    if plainTag in url:

        bizInfoAuthorId = html.split('nav?uid=')[-1].split('"></script>')[0]
        # js 写入http://www.heze.cn/info/index/author/author/1461.html
        # 呈现的页面 http://www.heze.cn/info/product/contactus/id/1461.html
        contactUrl = 'http://www.heze.cn/info/product/contactus/id/{}.html'.format(bizInfoAuthorId)
        updateOne(_id, {'contactUrl': contactUrl, 'bizInfoAuthorId': bizInfoAuthorId}, 'todayUrls')
        try:
            r = requests.get(contactUrl)
            comName = r.text.split('title>')[1].split('-')[0]
            print(comName)
            updateOne(_id, {'comName': comName}, 'todayUrls')

            # document.write(' <div class="contactBox"> <div class="classTitleBar"> 联系方式 </div> <div class="content"> <p style="white-space:normal;"> <span style="line-height:1.5;"> 联系人：魏建新 </span> </p> <p style="white-space:normal;">座机：<img src="/info/themes/heze/Public/tel/?tel=MTM0NTA2ODQxMDQ="></p> <p style="white-space:normal;">手机：<img src="/info/themes/heze/Public/tel/?tel=MTM0NTA2ODQxMDQ="></p> <p style="white-space:normal;">QQ：<a style="display:inline-block;vertical-align:middle;" href="http://wpa.qq.com/msgrd?v=3&uin=1489537908&site=qq&menu=yes"><img src="/info/themes/heze/Public/qs/images/qq.gif" style="vertical-align:middle;"></a></p> <p style="white-space:normal;">主营产品：</p> <p style="white-space:normal;">公司所在地：东莞市塘厦凤凰岗，园林路32号</p> </div> </div>');
            # 注意有联系电话的img-url,但是没有公司名称
            jsUrl = 'http://www.heze.cn/info/index/author/author/{}.html'.format(bizInfoAuthorId)
            r = requests.get(jsUrl)
            jsHtmlCode = r.text.split("document.write(' ")[-1].split("');")[0]
            soup = BeautifulSoup(jsHtmlCode, 'html.parser')
            comInfoTxt = soup.text.split('联系方式')[-1]
            telImg, phoneImg = ['http://www.heze.cn' + i.attrs['src'] for i in soup.find_all('img')[0:2]]
            updateOne(_id, {'comInfoTxt': comInfoTxt, 'jsHtmlCode': jsHtmlCode, 'telImg': telImg, 'phoneImg': phoneImg},
                      'todayUrls')
        except Exception as e:
            print(e)
            print('NOT-match')
            print(url)
            print(contactUrl)

            dd = 9

    # 客户联系电话在唯普通的可以ocr识别的图片中，但是联系方式详情页存在于可以直接获取的html中
    # 入口页 http://www.heze.cn/qiye/15044035888/show-30-4885060.html
    # 联系页 http://www.heze.cn/qiye/sp-15044035888-lianxi.html
    plainTag = 'http://www.heze.cn/qiye/'
    if plainTag in url:
        bizQiyeId = url.split('http://www.heze.cn/qiye/')[-1].split('/')[0]
        contactUrl = 'http://www.heze.cn/qiye/sp-{}-lianxi.html'.format(bizQiyeId)
        updateOne(_id, {'contactUrl': contactUrl, 'bizQiyeId': bizQiyeId}, 'todayUrls')
        try:
            r = requests.get(contactUrl)
            r.encoding = 'utf-8'
            comName = r.text.split('title>')[1].split('-')[0].replace(' ', '')
            print(comName)
            updateOne(_id, {'comName': comName}, 'todayUrls')

            # document.write("<div class=\"right-box\"> <div class=\"title\"> <a class=\"title-name\" style=\"color: #3759ED\">联系我们</a> <span>CONTACT US</span> </div> <ul class=\"article-contact-list\">  <li> <span class=\"tag\">认证名称:</span> <span id=\"company\">沧州鑫源晟防腐保温管道有限公司</span> </li> <li> <span class=\"tag\">联&nbsp;系&nbsp;人:</span> <span id=\"lianxiren\">张经理  </span> </li>   <li> <span class=\"tag\">手机号码:</span><img src=\"http://www.heze.cn/qiye/index.php?a=txtimg&lianxi=K35wmieoRRSbm8XqBk1YgBUvdQ_c_c\" style=\"width:200px;height:18px\"> </li>  <li> <span class=\"tag\">电话号码:</span><img src=\"http://www.heze.cn/qiye/index.php?a=txtimg&lianxi=e3EswHuqFBOanc3uGU9aihMndgQ_c\" style=\"width:200px;height:18px\"> </li>  <li> <span id=\"email\">联系 QQ: </span><a href=\"http://wpa.qq.com/msgrd?v=3&uin=1090753908&site=qq&menu=yes\"  style=\"background:url(\'http://www.heze.cn/qiye/skin/images/qq.gif\') no-repeat center; padding:3px; height:40px;width:77px;\">&nbsp;&nbsp; &nbsp;&nbsp; &nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp; &nbsp; </a> </span> </li>  <li>     <span class=\"tag\">企业官网:</span><span id=\"gsgwang\"><a href=\"http://www.hzgdzz.com\" target=\"_blank\" rel=\"nofollow\"/>http://www.hzgdzz.com</a></span>   </li> <li>     <h3 style=\"border-top: 1px dashed #dfdfdf;\"><a href=\"javascript:;\" style=\"text-align:center;\">扫一扫加微信</a></h3><p style=\"text-align:center;\"><img src=\"http://upload.xunpaibao.com/u/66329/152997883648145.jpg\" style=\"width:160px;height:160px;\"></p>   </li> </ul> </div><style>.article-contact-list h3{font-weight: 700;font-size: 15px;color: #db0402;text-align: left;height: 27px;line-height: 27px;overflow: hidden;white-space: nowrap;text-overflow: ellipsis;vertical-align: middle;}.article-contact-list h3 a {    color: #3759ED;    padding: 5px 0px;    display: block;    font-size: 16px;}</style>");
            jsUrl = 'http://www.heze.cn/qiye/index.php?a=userinfo&username={}'.format(bizQiyeId)
            r = requests.get(jsUrl)
            r.encoding = 'utf-8'
            jsHtmlCode = r.text.split("document.write('")[-1].split("');")[0]
            soup = BeautifulSoup(jsHtmlCode, 'html.parser')
            comInfoTxt = soup.text.split('CONTACT US')[-1].split('扫一扫加微信')[0]
            imgNum = len(soup.find_all('img'))
            if imgNum == 2:
                telImg, phoneImg = [i.attrs['src'].replace('\\', '') for i in soup.find_all('img')[0:2]]
                wxImg = 'NO'
            elif imgNum == 3:
                telImg, phoneImg, wxImg = [i.attrs['src'].replace('\\', '') for i in soup.find_all('img')[0:3]]
            updateOne(_id, {'comInfoTxt': comInfoTxt, 'jsHtmlCode': jsHtmlCode, 'telImg': telImg, 'phoneImg': phoneImg,
                            'wxImg': wxImg}, 'todayUrls')
        except Exception as e:
            print(e)
            print(url)
            print(contactUrl)

    # 入口页 http://www.cnhan.com/pinfo/313509.html
    # 店铺页 http://www.cnhan.com/pinfo/company-67751.html
    # 联系页 http://www.cnhan.com/pinfo/company-67751-contact.html
    plainTag = 'http://www.cnhan.com/pinfo/'
    if plainTag in url:
        bizPinfoId = html.split('.html">进入店铺')[0].split('company-')[-1]
        if bizPinfoId == '':
            siteException = '{} 店铺页面不存在或已删除'.format(url)
            updateOne(_id, {'siteException': siteException}, 'todayUrls')
            print(siteException)
            continue

        contactUrl = 'http://www.cnhan.com/pinfo/company-{}-contact.html'.format(bizPinfoId)
        updateOne(_id, {'contactUrl': contactUrl, 'bizPinfoId': bizPinfoId}, 'todayUrls')
        try:
            r = requests.get(contactUrl)
            comName = r.text.split('title>')[1].split('-')[-1].split('<')[0]
            updateOne(_id, {'comName': comName}, 'todayUrls')

            # 已经在页面校验,
            # 1-出现则唯一;2-出现且出现其中的一个；
            class_l = ['con_left', 'n_contact']
            for c_ in class_l:
                findChk = BeautifulSoup(r.text, 'html.parser').find(class_=c_)
                if findChk is not None:
                    comInfoTxt = findChk.text
                    # dropTag_tail_l = ['']
                    # for sp in dropTag_tail_l:
                    #     comInfoTxt = comInfoTxt.split(sp)[0]
                    #     comName = r.text.split('title>')[1].split('-')[-1].split('<')[0]
                    #     print(comInfoTxt)
                    print(comInfoTxt)
                    updateOne(_id, {'comInfoTxt': comInfoTxt}, 'todayUrls')
        except Exception as e:
            print(e)
            print(url)
            print(contactUrl)
