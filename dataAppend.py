from tool import RandomString, selectToDic, updateOne
from mongoTrans import improve, uniqueUrlSpiderDate
from bs4 import BeautifulSoup
import requests, time, json, random

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

cleanDbSwitcher = True if 7 > 9 else False
if cleanDbSwitcher:
    improve()
    uniqueUrlSpiderDate()
    uniqueUrlSpiderDate('siteUserPage')

# 全部访问路径特征清单
# 可追加，不删除
passPaths = ['sonhoo.com/wukong/', 'cnhan.com/shantui/', 'cnhan.com/hyzx/', 'cnhan.com/pinfo/', 'heze.cn/info/',
             'heze.cn/qiye/']
#  'cnhan.com/shantui/'  调用nodejs解密

# 本次程序实际处理的访问路径特征
# 可追加，可删除
dealPaths = ['cnhan.com/pinfo/']
for i in dealPaths:
    del passPaths[passPaths.index(i)]

spiderDate = time.strftime("%Y%m%d", time.localtime()) if 11 > 9 else '20180830'
collectionName = 'todayUrls'
mongoWhere = {'spiderDate': spiderDate} if 11 > 9 else {}
urlHtml = selectToDic('_id', collectionName, fields={'url': 1, 'html': 1, 'spiderDate': 1, 'Base64parse2times': 1},
                      where=mongoWhere)

for i in urlHtml:
    _id = i
    item = urlHtml[i]
    url, spiderDate, html = item['url'].replace('\n', ''), item['spiderDate'], item['html']
    updateOne(_id, {'url': url}, collectionName)

    pathTag = 'sonhoo.com/wukong/'
    if pathTag in url:
        if pathTag in passPaths:
            continue
        soup = BeautifulSoup(html, 'html.parser')
        try:
            comName = soup.find("title").text.split('-')[-1].replace('【', '').replace('】', '')
            s = soup.text.split('"app-contact-data":')[1].split('},"pages":')[0]
            d = json.loads(s, encoding='utf-8')
            dd = d['data']
            updateOne(_id, {'comName': comName, 'mobilePhone': dd['telephone'], 'qq': dd['qq'], 'addr': dd['address'],
                            'contactName': dd['linkman']}, collectionName)
            print('ok-->', spiderDate, url, soup.find("title").text)
        except Exception as e:
            print(e)
            print(url)

    pathTag = 'cnhan.com/shantui/'
    if pathTag in url:
        if pathTag in passPaths:
            continue
        # 仅仅请求一次，且假设返回正确、正确入库
        if 'Base64parse2times' in item:
            continue
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

    # 客户资料在mogo的html中，但是没有联系方式详情页完整
    pathTag = 'cnhan.com/hyzx/'
    if pathTag in url:
        if pathTag in passPaths:
            continue
        soup = BeautifulSoup(html, 'html.parser')
        _ = soup.find_all('a')
        contactTag = '../contact_'
        for i in _:
            if 'href' in i.attrs:
                href = i.attrs['href']
                if contactTag in href:
                    contactUrl = '{}{}{}'.format('http://www.', pathTag, href.replace('../', ''))
                    updateOne(_id, {'contactUrl': contactUrl}, collectionName)
                    try:
                        headers = {'User-Agent': RandomString()}
                        r = requests.get(contactUrl, headers=headers)
                        html = r.text
                        print(r)
                        soup = BeautifulSoup(html, 'html.parser')
                        comName = soup.find('title').text.split('-')[0]
                        updateOne(_id, {'comName': comName}, collectionName)
                        time.sleep(random.random())
                        # 已经在页面校验,
                        # 1-出现则唯一;2-出现且出现其中的一个；
                        class_l = ['lxfs', 'cp_rcc', 'about', 'lx_c', 'describe', 'nsmsg', 'lxwmjs', 'newscon',
                                   'dis_content2', 'lxwm', 'case_right_box', 'mrb2_list', 'cen_lt', 'contact_top',
                                   'content', 'center']
                        for c_ in class_l:
                            findChk = soup.find(class_=c_)
                            if findChk is not None:
                                comInfoTxt = findChk.text.replace('\n\n', '\n').replace('\t\t', '\t')
                                dropTag_tail_l = ['纬度']
                                for sp in dropTag_tail_l:
                                    comInfoTxt = comInfoTxt.split(sp)[0]
                                    print(comInfoTxt)
                                updateOne(_id, {'comName': comName, 'comInfoTxt': comInfoTxt}, collectionName)
                    except Exception as e:
                        print('NOT-match-------------------->')
                        print(e)
                        print(url)
                        print(contactUrl)
                        print('NOT-match<--------------------')

    # 客户联系电话在唯普通的可以ocr识别的图片中，但是联系方式详情页存在于可以直接获取的html中
    pathTag = 'heze.cn/info/'
    if pathTag in url:
        if pathTag in passPaths:
            continue
        print(html)
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

    # 客户联系电话在唯普通的可以ocr识别的图片中，但是联系方式详情页存在于可以直接获取的html中
    # 入口页 http://www.heze.cn/qiye/15044035888/show-30-4885060.html
    # 联系页 http://www.heze.cn/qiye/sp-15044035888-lianxi.html
    pathTag = 'heze.cn/qiye/'
    if pathTag in url:
        if pathTag in passPaths:
            continue
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
    pathTag = 'cnhan.com/pinfo/'
    if pathTag in url:
        if pathTag in passPaths:
            continue
        print(url, spiderDate)

        soup = BeautifulSoup(html, 'html.parser')
        # 不准确的字段，更新
        comName = soup.find('title').text.split('-')[0].replace('  ', '').replace(' ', '')
        updateOne(_id, {'comName': comName}, collectionName)

        bizPinfoId = html.split('.html">进入店铺')[0].split('company-')[-1]

        if bizPinfoId == '':
            siteException = '{} 店铺页面不存在或已删除'.format(url)
            updateOne(_id, {'siteException': siteException}, collectionName)
            print(siteException)
            continue
        contactUrl = 'http://www.cnhan.com/pinfo/company-{}-contact.html'.format(bizPinfoId)
        updateOne(_id, {'contactUrl': contactUrl, 'bizPinfoId': bizPinfoId}, collectionName)
        try:
            headers = {'User-Agent': RandomString()}
            r = requests.get(contactUrl, headers=headers)
            comName = r.text.split('title>')[1].split('-')[-1].split('<')[0]
            updateOne(_id, {'comName': comName}, collectionName)

            html = r.text
            # 不准确的字段，先入库后再发起请求更新
            comName = soup.find('title').text.split('-')[-1].replace('  ', '').replace(' ', '')
            updateOne(_id, {'comName': comName}, collectionName)
            print(contactUrl, html)

            # 已经在页面校验,
            # 1-出现则唯一;2-出现且出现其中的一个；
            class_l = ['con_left', 'n_contact', 'con_con']
            for c_ in class_l:
                findChk = BeautifulSoup(html, 'html.parser').find(class_=c_)
                if findChk is not None:
                    comInfoTxt = findChk.text
                    print(comInfoTxt)
                    updateOne(_id, {'comInfoTxt': comInfoTxt}, collectionName)
        except Exception as e:
            print(e)
            print(url)
            print(contactUrl)
