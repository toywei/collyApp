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

站点日期字符串特征
"cnhan.com/hyzx/":    "20180902",
"cnhan.com/shantui/": "20180902",
"cnhan.com/pinfo/":   "2018.09.02",
"heze.cn/info/":      "2018.09.02",
"heze.cn/qiye/":      "2018.09.02",
"sonhoo.com/wukong/": "2018-09-02",

*/

type PotentialCustomerWebSiteUrl struct {
	Url string `bson:"url"`
}

func eggSitePathTargetDate(TargetDate string) map[string]string {
	SitePathTargetDateFmt := map[string]string{
		"cnhan.com/hyzx/":    "",
		"cnhan.com/shantui/": "ONLYtoday",
		"cnhan.com/pinfo/":   ".",
		"heze.cn/info/":      ".",
		"heze.cn/qiye/":      ".",
		"sonhoo.com/wukong/": "-",
	}
	for k, v := range SitePathTargetDateFmt {
		fmt.Println(k)
		if ( v == "-") {
			SitePathTargetDateFmt[k] = TargetDate
		} else {
			SitePathTargetDateFmt[k] = strings.Replace(TargetDate, "-", v, -1)
		}
	}
	return SitePathTargetDateFmt
}

//  指定日期数据采集
var TargetDate = "2018-09-03"
var TodayDate = time.Now().Format("2006-01-02")
var mongoCollectioName = "todayUrls"
var TargetDateMongoFmt = strings.Replace(TargetDate, "-", "", -1)
var SitePathTargetDate = eggSitePathTargetDate(TargetDate)

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
			d := SitePathTargetDate["cnhan.com/hyzx/"]
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
		d := SitePathTargetDate["cnhan.com/pinfo/"]
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
		colly.AllowedDomains("Tcn.sonhoo.com"),
		colly.URLFilters(
			//请求页面的正则表达式，满足其一即可
			regexp.MustCompile("^http://cn.sonhoo.com/wukong/$"),
			//regexp.MustCompile("^http://cn.sonhoo.com/wukong/[ac]{1}\\d+$"),
			regexp.MustCompile("^http://cn.sonhoo.com/wukong/[ac]{1}\\d+$"),
		),
		// 不加UA，无数据
		colly.UserAgent("Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"),


	)
	// Add callbacks to a Collector

	// On every a element which has href attribute call callback
	c.OnHTML("a[href]", func(e *colly.HTMLElement) {
		// 对方网站内容不明，试探性强制退出遍历
		link := e.Attr("href")
		//fmt.Printf("Link found: %q -> %s\n", e.Text, link)
		c.Visit(e.Request.AbsoluteURL(link))
		// 无法通过href的DOM相关节点判断时间
	})

	c.OnHTML(".app-col", func(e *colly.HTMLElement) {
		fmt.Println("First column of a table row:", e.Text)
		dat1 := e.ChildText(".app-news-detail")
		fmt.Println(dat1)
		dat := e.ChildText(".app-news-detail>.app-news-detail__meta>.app-news-detail__meta-item")
		fmt.Println("c.OnHTML--->html")
		fmt.Println(dat)
	})

	// On every a element which has href attribute call callback
	c.OnScraped(func(r *colly.Response) {
		fmt.Println("Finished", r.Request.URL)
	})

	// Before making a request print "Visiting ..."
	c.OnRequest(func(r *colly.Request) {
		fmt.Println("Visiting", r.URL.String())
	})

	c.OnError(func(_ *colly.Response, err error) {
		log.Println("Something went wrong:", err)
	})

	c.OnResponse(func(r *colly.Response) {
		fmt.Println("Visited", r.Request.URL)
		reqUrl := fmt.Sprint(r.Request.URL)
		if strings.Contains(reqUrl, "http://cn.sonhoo.com/wukong/a") {
			wholePageHtml := string(r.Body)
			fmt.Println(wholePageHtml)
			s := strings.Replace(wholePageHtml, " ", "", -1)
			pageDate := strings.Split(s, "app-news-detail__meta-item\">")[1]
			pageDate = strings.Split(pageDate, "li")[0]
			d := SitePathTargetDate["sonhoo.com/wukong/"]
			if (strings.Contains(pageDate, d) ) {
				comName := strings.Split(s, "app-owner-card__title\">")[1]
				comName = strings.Split(comName, "</h3>")[0]
				telPhone := strings.Split(s, "app-owner-card__btn-phone\">")[1]
				telPhone = strings.Split(telPhone, "</a>")[0]
				comService := strings.Split(s, "app-owner-card__service\">")[1]
				comService = strings.Split(comService, "</p>")[0]
				// 建立func时，如何确定参数的类型？
				t := eleInArr(reqUrl, PotentialCustomerWebSiteUrlSet)
				if (!t) {
					strings.Replace(reqUrl, "\n", "", -1)
					client, err := mongo.Connect(context.Background(), "mongodb://hbaseU:123@192.168.3.103:27017/hbase", nil)
					db := client.Database("hbase")
					coll := db.Collection("targetDateUrls")
					if err != nil {
						fmt.Println(err)
					}
					// 判断日期的同样方法，获取其他字段
					result, err := coll.InsertOne(
						context.Background(),
						bson.NewDocument(
							bson.EC.String("spiderDate", TargetDateMongoFmt),
							bson.EC.String("url", reqUrl),
							bson.EC.String("html", wholePageHtml),
							bson.EC.String("comName", comName),
							bson.EC.String("telPhone", telPhone),
							bson.EC.String("comService", comService),
						))
					fmt.Println(err)
					fmt.Println(result)
				}
			}
		}
	})
	// Start scraping on http://cn.sonhoo.com/wukong/
	c.Visit("http://cn.sonhoo.com/wukong/")

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
					bson.EC.String("spiderDate", TargetDateMongoFmt),
					bson.EC.String("url", reqUrl),
					bson.EC.String("html", wholePageHtml),
				))
			fmt.Println(err)
			fmt.Println(result)
		})
		// Start scraping on http://www.cnhan.com/shantui/
		c.Visit(targetDateUrl)
	}
}
