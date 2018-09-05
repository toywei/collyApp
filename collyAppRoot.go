package main

import (
	"fmt"
	"github.com/gocolly/colly"
	"regexp"
	"strings"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/bson"
	"context"
	"log"
	"gopkg.in/mgo.v2"
	mgoBson "gopkg.in/mgo.v2/bson"
	"time"
	"math/rand"
)

// Configuration | Colly http://go-colly.org/docs/introduction/configuration/
const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

type SiteUrl struct {
	Url string `bson:"url"`
}

//  指定日期数据采集
var targetDate = time.Now().Format("20060102")
var mongoCollectioName = "siteUserPage"
var thisVisitedUrls [] string
var thisVisitedUrlsLimit = 30000
var batchWriteDbLimit = 3
var targetDateNewUrls []string
var targetDateInDbUrls [] string

func RandomString() string {
	b := make([]byte, rand.Intn(10)+10)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

// 检查元素是否存在于数组？遍历？如何集合运算方法
func eleInArr(ele string, arr [] string) bool {
	for _, v := range arr {
		if (ele == v) {
			fmt.Println("eleInArr", ele)
			return true
		}
	}
	return false
}

// 检查href的是否为url
func isUrl(str string) bool {
	f := [] string{"javascript:", "tel:"}
	if (strings.Replace(str, " ", "", -1) == "") {
		return false
	}
	for _, v := range f {
		if ( strings.Contains(str, v)) {
			return false
		}
	}
	// 可概括上述规则
	reg := regexp.MustCompile("^.+[0-9a-zA-z]{1,}$")
	data := reg.Find([]byte(str))
	if (data == nil) {
		return false
	}
	return true
}

/*
task
http://www.cnhan.com/hyzx/
http://www.cnhan.com/shantui/
http://www.cnhan.com/pinfo/
http://www.heze.cn/info
http://www.heze.cn/qiye/
http://cn.sonhoo.com/wukong/
采集站点当日更新数据的客户联系方式

20180828
站点日期字符串特征
"cnhan.com/hyzx/":    "20180902",
"cnhan.com/shantui/": "20180902",
"cnhan.com/pinfo/":   "2018.09.02",
"heze.cn/info/":      "2018.09.02",
"heze.cn/qiye/":      "2018.09.02",
"sonhoo.com/wukong/": "2018-09-02",

20180904
http://cn.sonhoo.com/wukong/c16?offset=600&limit=50 先去文章含有文章日期的列表页遍历出符合条件的文章url，
再去文章详情页http://cn.sonhoo.com/wukong/a213383采集客户资料

采集用户页
认为用户页和文章详情页平级，但是客户页的对象是全站和无页面日期特征字符串要求
*/

func refreshtargetDateInDbUrls() {
	//查询mongodb数据
	session, err := mgo.Dial("mongodb://hbaseU:123@192.168.3.103:27017/hbase")
	if err != nil {
		panic(err)
	}
	defer session.Close()
	// Optional. Switch the session to a monotonic behavior.
	session.SetMode(mgo.Monotonic, true)
	Collection := session.DB("hbase").C(mongoCollectioName)
	var SiteUrls [] SiteUrl
	err = Collection.Find(mgoBson.M{"spiderDate": targetDate}).All(&SiteUrls)
	if err != nil {
		log.Fatal(err)
	}
	p := &targetDateInDbUrls
	for i, v := range SiteUrls {
		fmt.Println(i)
		fmt.Println(v.Url)
		*p = append(*p, v.Url)
	}
	return
}

func batchWriteDb() {
	refreshtargetDateInDbUrls()
	p := &targetDateNewUrls
	pVisited := &thisVisitedUrls
	for _, targetDateUrl := range *p {
		t := eleInArr(targetDateUrl, *pVisited)
		if (!t) {
			// Instantiate default collector
			c := colly.NewCollector()
			// On every a element which has href attribute call callback
			c.OnScraped(func(r *colly.Response) {
				reqUrl := fmt.Sprintln(r.Request.URL)
				strings.Replace(reqUrl, "\n", "", -1)
				wholePageHtml := string(r.Body)
				client, err := mongo.Connect(context.Background(), "mongodb://hbaseU:123@192.168.3.103:27017/hbase", nil)
				db := client.Database("hbase")
				coll := db.Collection(mongoCollectioName)
				if err != nil {
					fmt.Println(err)
				}
				//目标日多次采集，目标日同站点且同路径url不重复入库
				//存入时间戳，分析目标站点的信息更新规律
				result, err := coll.InsertOne(
					context.Background(),
					bson.NewDocument(
						bson.EC.String("spiderDate", targetDate),
						bson.EC.String("url", reqUrl),
						bson.EC.String("html", wholePageHtml),
					))
				fmt.Println(err)
				fmt.Println(result)
			})
			c.Visit(targetDateUrl)
		}
	}
}
func getTargetDateNewUrlsBatchSave() {
	refreshtargetDateInDbUrls()
	p := &targetDateNewUrls
	pVisited := &thisVisitedUrls
	pTargetDateInDb := &targetDateInDbUrls
	c := colly.NewCollector()
	c = colly.NewCollector(
		colly.AllowedDomains("cn.sonhoo.com"),
		colly.URLFilters(
			// 全站放开，遍历全站url，按照规则入库
			// 但为提高目标数据获取速率，只允许目标数据的前置路径可以访问
			regexp.MustCompile("^http://cn.sonhoo.com/wukong/$"),
			regexp.MustCompile("^http://cn.sonhoo.com/wukong/[ac]{1}\\d+$"),
			regexp.MustCompile("^http://cn.sonhoo.com/wukong/c\\d+\\?offset=\\d+\\&limit=\\d+$"),
			// 数据量个位数，放宽过滤器
			regexp.MustCompile("^http://cn.sonhoo.com/wukong/.+$"),
		),
		// 不加UA，无数据
		// colly.UserAgent("Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"),
	)
	// 限制线程数，引入随机延迟
	// Limit the number of threads started by colly to two
	// when visiting links which domains' matches "*httpbin.*" glob
	c.Limit(&colly.LimitRule{
		DomainGlob:  "*sonhoo.*",
		Parallelism: 5,
		RandomDelay: 5 * time.Second,
	})
	// 控制对发现的url是否发起访问
	// 全站url
	c.OnHTML("a[href]", func(e *colly.HTMLElement) {
		fmt.Println(*p, "NEW------------------")
		link := e.Attr("href")
		fmt.Println(link)
		if ( !strings.Contains(link, "http://") ) {
			link = "http://cn.sonhoo.com" + link
		}
		// 不考虑同一路径的页面更新，不重复访问uri
		t := eleInArr(link, *p)
		t2 := eleInArr(link, *pVisited)
		t3 := eleInArr(link, *pTargetDateInDb)
		t4 := isUrl(link)
		if (!t && !t2 && !t3 && !t4) {
			fmt.Println("本次没被访问的url，发起访问，但可能被过滤", link)
			c.Visit(e.Request.AbsoluteURL(link))
		} else {
			fmt.Println("跳过，原因：1.1、本次程序已经访问；1.2、已经入库；2.1、非url格式；")
		}
	})
	c.OnScraped(func(r *colly.Response) {
		fmt.Println("Finished", r.Request.URL)
		wholePageHtml := string(r.Body)
		fmt.Println(wholePageHtml)
	})
	// Before making a request print "Visiting ..."
	c.OnRequest(func(r *colly.Request) {
		fmt.Println("Visiting", r.URL.String())
		r.Headers.Set("User-Agent", RandomString())
	})
	c.OnError(func(_ *colly.Response, err error) {
		log.Println("Something went wrong:", err)
	})
	// 数据落盘
	c.OnResponse(func(r *colly.Response) {
		fmt.Println("Visited", r.Request.URL)
		url := fmt.Sprintln(r.Request.URL)
		*pVisited = append(*pVisited, url)
		// http://cn.sonhoo.com/wukong/u/200078/index
		reg := regexp.MustCompile("^.{0,}/wukong/u/\\d+/index$")
		// 写入落盘队列
		data := reg.Find([]byte(url))
		if (len(data) > 0) {
			t := eleInArr(url, *p)
			t3 := eleInArr(url, *pTargetDateInDb)
			if (!t && !t3) {
				*p = append(*p, url)
				fmt.Println(*p, "ADD------------")
			}
		}
		// 检测是否达到批量落盘时机
		// 待落盘数达到上限
		// 已经访问的站点url数达到上限
		if (len(*p) > batchWriteDbLimit || len(*pVisited) > thisVisitedUrlsLimit) {
			fmt.Println("len(targetDateNewUrls) > ", batchWriteDbLimit, "len(thisVisitedUrls) > ", thisVisitedUrlsLimit)
			//  及时入库
			batchWriteDb()
			refreshtargetDateInDbUrls()
			*p = nil
		}
	})
	// Start scraping on
	c.Visit("http://cn.sonhoo.com/wukong/")
	// 等待线程结束
	// Wait until threads are finished
	c.Wait()
}

func main() {
	// 无限循环
	for {
		targetDate = time.Now().Format("20060102")
		getTargetDateNewUrlsBatchSave()
	}
}
