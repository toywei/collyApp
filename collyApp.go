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

*/

type PotentialCustomerWebSiteUrl struct {
	Url string `bson:"url"`
}

// Configuration | Colly http://go-colly.org/docs/introduction/configuration/
const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandomString() string {
	b := make([]byte, rand.Intn(10)+10)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

// url汇总页的日期筛选方法
func eggSitePathListTargetDate(TargetDate string) map[string]string {
	SitePathListTargetDateFmt := map[string]string{
		"cnhan.com/hyzx/":    "",
		"cnhan.com/shantui/": "ONLYtoday",
		"cnhan.com/pinfo/":   ".",
		"heze.cn/info/":      ".",
		"heze.cn/qiye/":      ".",
		"sonhoo.com/wukong/": "-",
	}
	for k, v := range SitePathListTargetDateFmt {
		fmt.Println(k)
		if ( v == "") {
			SitePathListTargetDateFmt[k] = TargetDate
		} else {
			SitePathListTargetDateFmt[k] = TargetDate[0:4] + v + TargetDate[4:6] + v + TargetDate[6:8]
		}
	}
	fmt.Println(SitePathListTargetDateFmt)
	return SitePathListTargetDateFmt
}

//  指定日期数据采集
var TargetDate = "20180904"
var TodayDate = time.Now().Format("20060102")
var mongoCollectioName = "todayUrls0904TestData"
var SitePathListTargetDate = eggSitePathListTargetDate(TargetDate)

func getTargetDateSpideredUrl() []string {
	//查询mongodb数据
	session, err := mgo.Dial("mongodb://hbaseU:123@192.168.3.103:27017/hbase")
	if err != nil {
		panic(err)
	}
	defer session.Close()
	// Optional. Switch the session to a monotonic behavior.
	session.SetMode(mgo.Monotonic, true)
	Collection := session.DB("hbase").C(mongoCollectioName)
	var PotentialCustomerWebSiteUrls [] PotentialCustomerWebSiteUrl
	err = Collection.Find(mgoBson.M{"spiderDate": TargetDate}).All(&PotentialCustomerWebSiteUrls)
	if err != nil {
		log.Fatal(err)
	}
	var PotentialCustomerWebSiteUrlSet [] string
	for i, v := range PotentialCustomerWebSiteUrls {
		fmt.Println(i)
		fmt.Println(v.Url)
		PotentialCustomerWebSiteUrlSet = append(PotentialCustomerWebSiteUrlSet, v.Url)
	}
	return PotentialCustomerWebSiteUrlSet
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

func getTargetDateUrls() []string {
	var targetDateUrls []string
	PotentialCustomerWebSiteUrlSet := getTargetDateSpideredUrl()
	c := colly.NewCollector()
	if (TargetDate == TodayDate) {
		// 路径下只有当日url
		// Instantiate default collector
		c = colly.NewCollector(
			colly.AllowedDomains("www.cnhan.com"),
		)
		// On every a element which has href attribute call callback
		// 类选择器
		//url仅在本页
		c.OnHTML(".showSort a[href]", func(e *colly.HTMLElement) {

			link := e.Attr("href")
			t := eleInArr(link, PotentialCustomerWebSiteUrlSet)
			if (!t) {
				targetDateUrls = append(targetDateUrls, link)
				fmt.Printf("Link found: %q -> %s\n", e.Text, link)
			}
		})
		// Start scraping on http://www.cnhan.com/shantui/
		c.Visit("http://www.cnhan.com/shantui/")

		// 起始路由改变
		// Instantiate default collector
		c = colly.NewCollector(
			colly.AllowedDomains("www.cnhan.com"),
			colly.URLFilters(
				//请求页面的正则表达式，满足其一即可
				//http://www.cnhan.com/hyzx/
				//http://www.cnhan.com/hyzx/index-all-2.html
				//硬代码：目标日最多更新99页http://www.cnhan.com/hyzx/index-all-99.html
				//^[1-9][0-9]{0,1}[^0-9]{0,1}$
				regexp.MustCompile("^http://www.cnhan.com/hyzx/(.{0}$)|(index-all-[1-9][0-9]{0,1}[^0-9]{0,1}\\.html$)"),
			),
		)

		// On every a element which has href attribute call callback
		c.OnHTML("a[href]", func(e *colly.HTMLElement) {
			link := e.Attr("href")
			fmt.Printf("Link found: %q -> %s\n", e.Text, link)
			c.Visit(e.Request.AbsoluteURL(link))
			d := SitePathListTargetDate["cnhan.com/hyzx/"]
			reg := regexp.MustCompile(d) // http://www.cnhan.com/hyzx/20180827/7109076.html 通过url格式过滤出目标日的url
			data := reg.Find([]byte(link))
			regRes := len(data)
			if regRes > 0 {
				link = "http://www.cnhan.com/hyzx/" + link
				t := eleInArr(link, PotentialCustomerWebSiteUrlSet)
				if (!t) {
					targetDateUrls = append(targetDateUrls, link)
					fmt.Printf("Link found: %q -> %s\n", e.Text, link)
				}
			}
		})
		// Before making a request print "Visiting ..."
		c.OnRequest(func(r *colly.Request) {
			fmt.Println("Visiting", r.URL.String())
		})
		// Start scraping on http://www.cnhan.com/shantui/
		c.Visit("http://www.cnhan.com/hyzx/")

	}

	// 起始路由改变
	// Instantiate default collector
	c = colly.NewCollector(
		colly.AllowedDomains("www.cnhan.com"),
		colly.URLFilters(
			//请求页面的正则表达式，满足其一即可
			//http://www.cnhan.com/pinfo/
			//http://www.cnhan.com/pinfo/index-5.html
			//硬代码：目标日最多更新99页http://www.cnhan.com/pinfo/index-99.html
			regexp.MustCompile("^http://www.cnhan.com/pinfo/(.{0}$)|(index-[1-9][0-9]{0,1}[^0-9]{0,1}\\.html$)"),
		),
	)
	// On every a element which has href attribute call callback
	c.OnHTML("a[href]", func(e *colly.HTMLElement) {
		link := e.Attr("href")
		fmt.Printf("Link found: %q -> %s\n", e.Text, link)
		c.Visit(e.Request.AbsoluteURL(link))
		//文本过滤
		eDate := e.ChildText(".span2")
		//http://www.cnhan.com/pinfo/313257.html   周口水泥彩砖具有的特色是什么2018.08.27
		d := SitePathListTargetDate["cnhan.com/pinfo/"]
		if (strings.Contains(eDate, d)) {
			link := e.Attr("href")
			link = "http://www.cnhan.com" + link
			fmt.Printf("Link found: %q -> %s\n", e.Text, link)
			t := eleInArr(link, PotentialCustomerWebSiteUrlSet)
			if (!t) {
				targetDateUrls = append(targetDateUrls, link)
				fmt.Printf("Link found: %q -> %s\n", e.Text, link)
			}
		}
	})
	// Before making a request print "Visiting ..."
	c.OnRequest(func(r *colly.Request) {
		fmt.Println("Visiting", r.URL.String())
	})
	// Start scraping on http://www.cnhan.com/shantui/
	c.Visit("http://www.cnhan.com/pinfo/")

	// 起始路由改变
	// Instantiate default collector
	c = colly.NewCollector(
		colly.AllowedDomains("www.heze.cn"),
	)
	// On every a element which has href attribute call callback
	// 类选择器
	c.OnHTML(".news_list_r a[href]", func(e *colly.HTMLElement) {
		link := e.Attr("href")
		fmt.Printf("Link found: %q -> %s\n", e.Text, link)
		targetDateUrls = append(targetDateUrls, link)
	})
	// Start scraping on http://www.cnhan.com/shantui/
	c.Visit("http://www.heze.cn/info/")

	/*
	站内目标url
	http://www.heze.cn/info/
	http://www.heze.cn/qiye/
	检测思路：
	1、按父url，分别进入 http://www.heze.cn/qiye/18240670888/show-37-1367148.html  http://www.heze.cn/info/LEbigong/show-1-13931879.html
	   与2反
	2、按照全站进入
	   优点：过滤规则简单，代码代码简单；爬取结果数据不便于分类处理，比如产品类型、发布时间；
	   缺点：爬爬取速度慢
	*/

	// 起始路由改变
	//http://www.heze.cn/qiye/  该页面、其主体子页面，刷新，内容变化
	//http://www.heze.cn/qiye/list-8.html
	// Instantiate default collector
	c = colly.NewCollector(
		colly.AllowedDomains("www.heze.cn"),
		colly.URLFilters(
			//请求页面的正则表达式，满足其一即可
			regexp.MustCompile("^http://www.heze.cn/qiye/(.{0}$)|(list-\\d+-\\d+\\.html$)"),
		),
	)
	// On every a element which has href attribute call callback
	c.OnHTML("a[href]", func(e *colly.HTMLElement) {
		link := e.Attr("href")
		fmt.Printf("Link found: %q -> %s\n", e.Text, link)
		c.Visit(e.Request.AbsoluteURL(link))
		// http://www.heze.cn/qiye/hongfei688/show-44-14825619.html
		reg := regexp.MustCompile("^http://www.heze.cn/qiye/[0-9a-zA-Z]+/show-\\d+-\\d+\\.html$")
		data := reg.Find([]byte(link))
		regRes := len(data)
		if regRes > 0 {
			fmt.Printf("Link found: %q -> %s\n", e.Text, link)
			t := eleInArr(link, PotentialCustomerWebSiteUrlSet)
			if (!t) {
				targetDateUrls = append(targetDateUrls, link)
				fmt.Printf("Link found: %q -> %s\n", e.Text, link)
			}
		}
	})
	// Before making a request print "Visiting ..."
	c.OnRequest(func(r *colly.Request) {
		fmt.Println("Visiting", r.URL.String())
	})
	// Start scraping on http://www.heze.cn/qiye/
	c.Visit("http://www.heze.cn/qiye/")

	// 起始路由改变
	// 目标日页面部分存在于起始页且与飞目标日页面混杂，无法通过选择器判断，需解析文本
	// 全站过滤
	// 类目页 http://cn.sonhoo.com/wukong/c133
	// 文章页 http://cn.sonhoo.com/wukong/a191114
	c = colly.NewCollector(
		colly.AllowedDomains("cn.sonhoo.com"),
		colly.URLFilters(
			//请求页面的正则表达式，满足其一即可
			regexp.MustCompile("^http://cn.sonhoo.com/wukong/$"),
			//regexp.MustCompile("^http://cn.sonhoo.com/wukong/[ac]{1}\\d+$"),
			regexp.MustCompile("^http://cn.sonhoo.com/wukong/[c]{1}\\d+$"),
			// http://cn.sonhoo.com/wukong/c0?offset=150&limit=50 文章列表页
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

	// 保证遍历http://cn.sonhoo.com/wukong/c4?offset=100&limit=50各个页面的，获取可能的目标日的url
	// On every a element which has href attribute call callback
	c.OnHTML("a[href]", func(e *colly.HTMLElement) {
		link := e.Attr("href")
		fmt.Printf("Link found: %q -> %s\n", e.Text, link)
		c.Visit(e.Request.AbsoluteURL(link))
	})

	// 目标日的url入库
	// div[class=page-articles__articles]>ul>li
	c.OnHTML("div[class=page-articles__articles]>ul>li", func(e *colly.HTMLElement) {
		link := e.ChildAttr("a", "href")
		dat1 := e.ChildText("span")
		fmt.Println(dat1)
		fmt.Println(link)
		pageDate := e.ChildText("span")
		d := SitePathListTargetDate["sonhoo.com/wukong/"]
		fmt.Println("tDate", d)
		fmt.Println("pageDate", pageDate)
		if (strings.Contains(pageDate, d)) {
			link = "http://cn.sonhoo.com" + link
			t := eleInArr(link, PotentialCustomerWebSiteUrlSet)
			if (!t) {
				targetDateUrls = append(targetDateUrls, link)
				fmt.Printf("Link found: %q -> %s\n", e.Text, link)
			}
		}
		c.Visit(e.Request.AbsoluteURL(link))
	})

	c.OnScraped(func(r *colly.Response) {
		fmt.Println("Finished", r.Request.URL)
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
	})
	// Start scraping on
	// http://cn.sonhoo.com/wukong/ 不带日期文章列表页
	// http://cn.sonhoo.com/wukong/c0 20180904 带日期的文章列表页：共50页，没有l类目
	c.Visit("http://cn.sonhoo.com/wukong/c0")
	// 等待线程结束
	// Wait until threads are finished
	c.Wait()

	return targetDateUrls
}

func main() {
	var targetDateUrls = getTargetDateUrls()
	fmt.Println(targetDateUrls)
	fmt.Println(len(targetDateUrls))
	for _, targetDateUrl := range targetDateUrls {
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
