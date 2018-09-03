from pymongo import MongoClient
import requests
import time

'''
http://www.cnhan.com/shantui//templates/MC530/TP001/js/template.js
河北国标3PE防腐钢管现货 http://www.cnhan.com/shantui/2M02DgM/news-66903.html
    <script type="text/javascript">
        window.platformPath = "http://www.cnhan.com/shantui/";
        window.cId = "3691";
        window.cCode= "2M02DgM";
        window.staticPath = "http://www.cnhan.com/shantui/";
        // 获取当前链接
        window.currentUrl = window.location.href;
        window.moreProduction = "http://www.cnhan.com/shantui/production-3691.shtml";
        window.moreArticle = "http://www.cnhan.com/shantui/article-3691.shtml";
    </script>

function decode(data) {
    var a = CryptoJS.enc.Base64.parse(data);
    return a.toString(CryptoJS.enc.Utf8);
}
var qiaoContent="0";//0默认没有商桥内容 1有商桥
$.ajax({
    url: window.platformPath + '/dynamic/get/data/allCompanyInfoByCompCode.json?compCode=' + window.cCode,
    dataType: 'json',
    success: function(data){
        var str = decode(data.r);
        var str2 = decode(str);
        str2 = JSON.parse(str2)
        var contactInfoDto = str2.contactInfoDto;
        var pcNavInfoDtoList = str2.pcNavInfoDtoList;
        var productInfoDtoList = str2.productInfoDtoList;
        var carouselInfoDtoList = str2.carouselInfoDtoList;
        var mobileNavInfoDtoList = str2.mobileNavInfoDtoList;
        var qiaoMap = str2.qiaoMap;
        // 1.
        MC500TP001site_footer(contactInfoDto);
        MC500TP001contact_us(contactInfoDto);
        MC500TP001company_card(contactInfoDto);
        MC500TP001mobile_footer(contactInfoDto);
        // 2.
        MC500TP001site_header(contactInfoDto,pcNavInfoDtoList);
        // 3.
        MC500TP001businesses(productInfoDtoList);
        // 4.
        MC500TP001carousel(carouselInfoDtoList);
        // 5.
        MC500TP001mobile_nav(mobileNavInfoDtoList);
        // 6.
        MC500TP001Qiao(qiaoMap);
    },
    error: function(data){
        console.log(data);
    }
});

'''


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


spiderDate = time.strftime("%Y%m%d", time.localtime())
collectionName = 'todayUrls'
mongoWhere = {'spiderDate': spiderDate} if 1 > 2 else {}
urlHtml = selectToDic('_id', collectionName, fields={'url': 1, 'html': 1}, where=mongoWhere)

for i in urlHtml:
    _id = i
    url = urlHtml[i]['url'].replace('\n', '')
    updateOne(_id, {'url': url}, collectionName)
    html = urlHtml[i]['html']

    if 'cnhan.com/shantui' in url:

        # window.compId = "3644";
        # window.platformPath = "http://www.cnhan.com/shantui/";
        # window.cId = "3691";
        # window.cCode= "2M02DgM"; html原js变量名更改
        # 对过去数据在命名上不再兼容，跟随平台命名
        cCodeTag = 'window.cCode = "'
        if cCodeTag in html:
            cCode = html.split(cCodeTag)[-1].split('"')[0]
        else:
            # http://www.cnhan.com/shantui/2M02DgM/news-66903.html
            cCode = url.split('/')[-2]
        print(url)
        updateOne(_id, {'cCode': cCode}, collectionName)
        try:
            url = 'http://www.cnhan.com/shantui//dynamic/get/data/allCompanyInfoByCompCode.json?compCode={}'.format(
                cCode)
            print(url)
            r = requests.get(url)
            Base64parse2times = r.text
            updateOne(_id, {'Base64parse2times': Base64parse2times}, collectionName)
        except Exception as e:
            print(e)

    continue
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
                    updateOne(_id, {'contactUrl': contactUrl}, collectionName)
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
                                updateOne(_id, {'comName': comName, 'comInfoTxt': comInfoTxt}, collectionName)
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
        updateOne(_id, {'contactUrl': contactUrl, 'bizInfoAuthorId': bizInfoAuthorId}, collectionName)
        try:
            r = requests.get(contactUrl)
            comName = r.text.split('title>')[1].split('-')[0]
            print(comName)
            updateOne(_id, {'comName': comName}, collectionName)

            # document.write(' <div class="contactBox"> <div class="classTitleBar"> 联系方式 </div> <div class="content"> <p style="white-space:normal;"> <span style="line-height:1.5;"> 联系人：魏建新 </span> </p> <p style="white-space:normal;">座机：<img src="/info/themes/heze/Public/tel/?tel=MTM0NTA2ODQxMDQ="></p> <p style="white-space:normal;">手机：<img src="/info/themes/heze/Public/tel/?tel=MTM0NTA2ODQxMDQ="></p> <p style="white-space:normal;">QQ：<a style="display:inline-block;vertical-align:middle;" href="http://wpa.qq.com/msgrd?v=3&uin=1489537908&site=qq&menu=yes"><img src="/info/themes/heze/Public/qs/images/qq.gif" style="vertical-align:middle;"></a></p> <p style="white-space:normal;">主营产品：</p> <p style="white-space:normal;">公司所在地：东莞市塘厦凤凰岗，园林路32号</p> </div> </div>');
            # 注意有联系电话的img-url,但是没有公司名称
            print(bizInfoAuthorId)
            jsUrl = 'http://www.heze.cn/info/index/author/author/{}.html'.format(bizInfoAuthorId)
            r = requests.get(jsUrl)
            jsHtmlCode = r.text.split("document.write('")[1].rstrip("');")
            soup = BeautifulSoup(jsHtmlCode, 'html.parser')
            comInfoTxt = soup.text
            telImg, phoneImg = ['http://www.heze.cn' + i.attrs['src'] for i in soup.find_all('img')[0:2]]
            updateOne(_id, {'comInfoTxt': comInfoTxt, 'jsHtmlCode': jsHtmlCode, 'telImg': telImg, 'phoneImg': phoneImg},
                      collectionName)
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
        updateOne(_id, {'contactUrl': contactUrl, 'bizQiyeId': bizQiyeId}, collectionName)
        try:
            r = requests.get(contactUrl)
            r.encoding = 'utf-8'
            comName = r.text.split('title>')[1].split('-')[0].replace(' ', '')
            print(comName)
            updateOne(_id, {'comName': comName}, collectionName)
            updateOne(_id, {'jsHtmlCode': ''}, collectionName)
            updateOne(_id, {'wxImg': ''}, collectionName)
            updateOne(_id, {'telImg': ''}, collectionName)
            updateOne(_id, {'phoneImg': ''}, collectionName)

            jsUrl = 'http://www.heze.cn/qiye/sp-{}-lianxi.html'.format(bizQiyeId)
            print(jsUrl)
            r = requests.get(jsUrl)
            r.encoding = 'utf-8'
            soup = BeautifulSoup(r.text, 'html.parser')
            item = soup.find(class_='article-contact-list')
            comInfoTxt = item.text
            itemSub = item.find_all('img')
            imgNum = len(item.find_all('img'))
            if imgNum == 2:
                phoneImg, telImg = itemSub[0].attrs['src'], ''
            elif imgNum == 3:
                phoneImg, telImg = [i.attrs['src'] for i in itemSub[0:2]]
            updateOne(_id, {'comInfoTxt': comInfoTxt, 'telImg': telImg, 'phoneImg': phoneImg}, collectionName)
            print(comInfoTxt)
            print(telImg, phoneImg)
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
            updateOne(_id, {'siteException': siteException}, collectionName)
            print(siteException)
            continue
        contactUrl = 'http://www.cnhan.com/pinfo/company-{}-contact.html'.format(bizPinfoId)
        updateOne(_id, {'contactUrl': contactUrl, 'bizPinfoId': bizPinfoId}, collectionName)
        try:
            r = requests.get(contactUrl)
            comName = r.text.split('title>')[1].split('-')[-1].split('<')[0]
            updateOne(_id, {'comName': comName}, collectionName)

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
                    updateOne(_id, {'comInfoTxt': comInfoTxt}, collectionName)
        except Exception as e:
            print(e)
            print(url)
            print(contactUrl)
