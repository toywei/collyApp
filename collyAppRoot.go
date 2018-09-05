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
var TodayDate = time.Now().Format("20060102")
var TargetDate = TodayDate
var MongoCollectioName = "siteUserPage"
var ThisVisitedUrls [] string
var ThisVisitedUrlsLimit = 20
var TargetDateNewUrls []string
var TargetDateSpideredUrls [] string

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

func refreshTargetDateSpideredUrls() {
	//查询mongodb数据
	session, err := mgo.Dial("mongodb://hbaseU:123@192.168.3.103:27017/hbase")
	if err != nil {
		panic(err)
	}
	defer session.Close()
	// Optional. Switch the session to a monotonic behavior.
	session.SetMode(mgo.Monotonic, true)
	Collection := session.DB("hbase").C(MongoCollectioName)
	var SiteUrls [] SiteUrl
	err = Collection.Find(mgoBson.M{"spiderDate": TargetDate}).All(&SiteUrls)
	if err != nil {
		log.Fatal(err)
	}
	p := &TargetDateSpideredUrls
	for i, v := range SiteUrls {
		fmt.Println(i)
		fmt.Println(v.Url)
		*p = append(*p, v.Url)
	}
	return
}

func batchWriteDb() {
	refreshTargetDateSpideredUrls()
	p := &TargetDateNewUrls
	for _, targetDateUrl := range *p {
		t := eleInArr(targetDateUrl, ThisVisitedUrls)
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
				coll := db.Collection(MongoCollectioName)
				if err != nil {
					fmt.Println(err)
				}
				//目标日多次采集，目标日同站点且同路径url不重复入库
				//存入时间戳，分析目标站点的信息更新规律
				result, err := coll.InsertOne(
					context.Background(),
					bson.NewDocument(
						bson.EC.String("spiderDate", TargetDate),
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
	refreshTargetDateSpideredUrls()
	c := colly.NewCollector()
	c = colly.NewCollector(
		colly.AllowedDomains("cn.sonhoo.com"),
		colly.URLFilters(
			// 全站放开，遍历全站url，按照规则入库
			// 但为提高目标数据获取速率，只允许目标数据的前置路径可以访问
			regexp.MustCompile("^http://cn.sonhoo.com/wukong/$"),
			regexp.MustCompile("^http://cn.sonhoo.com/wukong/[ac]{1}\\d+$"),
			regexp.MustCompile("^http://cn.sonhoo.com/wukong/c\\d+\\?offset=\\d+\\&limit=\\d+$"),
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
	// 全站url
	c.OnHTML("a[href]", func(e *colly.HTMLElement) {
		link := e.Attr("href")
		fmt.Println(link)
		// http://cn.sonhoo.com/wukong/u/200078/index
		reg := regexp.MustCompile("^.+/wukong/u/\\d+/index$")
		data := reg.Find([]byte(link))
		if (len(data) > 0) {
			link = "http://cn.sonhoo.com" + link
			t := eleInArr(link, TargetDateSpideredUrls)
			if (!t) {
				TargetDateNewUrls = append(TargetDateNewUrls, link)
				fmt.Printf("Link New: %q -> %s\n", e.Text, link)
			}
		}
		// 不考虑同一路径的页面更新，不重复访问uri
		t := eleInArr(link, ThisVisitedUrls)
		if (!t) {
			fmt.Println("本次没被访问的url，发起访问，但可能被过滤", link)
			if (len(ThisVisitedUrls) > ThisVisitedUrlsLimit) {
				fmt.Println("len(ThisVisitedUrls) > 50")
				//  及时入库
				batchWriteDb()
				refreshTargetDateSpideredUrls()
			}
			c.Visit(e.Request.AbsoluteURL(link))
		} else {
			fmt.Println("跳过，本次程序已经访问")
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
	c.OnResponse(func(r *colly.Response) {
		fmt.Println("Visited", r.Request.URL)
		url := fmt.Sprintln(r.Request.URL)
		ThisVisitedUrls = append(ThisVisitedUrls, url)
	})
	// Start scraping on
	c.Visit("http://cn.sonhoo.com/wukong/")
	// 等待线程结束
	// Wait until threads are finished
	c.Wait()
}

func main() {
	// 无线循环
	for {
		p := &TodayDate
		*p = time.Now().Format("20060102")
		getTargetDateNewUrlsBatchSave()
	}
}
