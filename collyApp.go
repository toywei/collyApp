package main

import (
	"fmt"

	"github.com/gocolly/colly"
	"time"
	"regexp"
	"strings"

	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/bson"

	"context"
)

/*
task
http://www.cnhan.com/hyzx/
http://www.cnhan.com/shantui/
http://www.cnhan.com/pinfo/

http://www.heze.cn/info
http://www.heze.cn/qiye/

采集站点当日更新数据的客户联系方式

*/
func getTodayUrls() []string {
	var todayUrls []string
	// Instantiate default collector
	c := colly.NewCollector(
		colly.AllowedDomains("www.cnhan.com"),
	)
	// On every a element which has href attribute call callback
	// 类选择器
	//url仅在本页
	c.OnHTML(".showSort a[href]", func(e *colly.HTMLElement) {
		link := e.Attr("href")
		todayUrls = append(todayUrls, link)
		fmt.Printf("Link found: %q -> %s\n", e.Text, link)
	})

	// Start scraping on http://www.cnhan.com/shantui/
	c.Visit("http://www.cnhan.com/shantui/")

	//起始路由改变
	// Instantiate default collector
	c = colly.NewCollector(
		colly.AllowedDomains("www.cnhan.com"),
		colly.URLFilters(
			//请求页面的正则表达式，满足其一即可
			//http://www.cnhan.com/hyzx/
			//http://www.cnhan.com/hyzx/index-all-2.html
			//硬代码：当天最多更新99页http://www.cnhan.com/hyzx/index-all-99.html
			//^[1-9][0-9]{0,1}[^0-9]{0,1}$
			regexp.MustCompile("^http://www.cnhan.com/hyzx/(.{0}$)|(index-all-[1-9][0-9]{0,1}[^0-9]{0,1}\\.html$)"),
		),
	)
	// On every a element which has href attribute call callback
	c.OnHTML("a[href]", func(e *colly.HTMLElement) {
		link := e.Attr("href")
		fmt.Printf("Link found: %q -> %s\n", e.Text, link)
		c.Visit(e.Request.AbsoluteURL(link))
		datetime := time.Now().Format("20060102")
		fmt.Println(datetime)
		reg := regexp.MustCompile(datetime) // http://www.cnhan.com/hyzx/20180827/7109076.html 通过url格式过滤出今天的url
		data := reg.Find([]byte(link))
		regRes := len(data)
		if regRes > 0 {
			link = "http://www.cnhan.com/hyzx/" + link
			todayUrls = append(todayUrls, link)
		}
	})

	// Before making a request print "Visiting ..."
	c.OnRequest(func(r *colly.Request) {
		fmt.Println("Visiting", r.URL.String())
	})

	// Start scraping on http://www.cnhan.com/shantui/
	c.Visit("http://www.cnhan.com/hyzx/")

	//起始路由改变
	// Instantiate default collector
	c = colly.NewCollector(
		colly.AllowedDomains("www.cnhan.com"),
		colly.URLFilters(
			//请求页面的正则表达式，满足其一即可
			//http://www.cnhan.com/pinfo/
			//http://www.cnhan.com/pinfo/index-5.html
			//硬代码：当天最多更新99页http://www.cnhan.com/pinfo/index-99.html
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
		datetime := time.Now().Format("2006.01.02")
		if (strings.Contains(eDate, datetime)) {
			link := e.Attr("href")
			link = "http://www.cnhan.com" + link
			fmt.Printf("Link found: %q -> %s\n", e.Text, link)
			todayUrls = append(todayUrls, link)
		}
	})

	// Before making a request print "Visiting ..."
	c.OnRequest(func(r *colly.Request) {
		fmt.Println("Visiting", r.URL.String())
	})

	// Start scraping on http://www.cnhan.com/shantui/
	c.Visit("http://www.cnhan.com/pinfo/")

	//起始路由改变
	// Instantiate default collector
	c = colly.NewCollector(
		colly.AllowedDomains("www.heze.cn"),
	)
	// On every a element which has href attribute call callback
	// 类选择器
	c.OnHTML(".news_list_r a[href]", func(e *colly.HTMLElement) {
		link := e.Attr("href")
		fmt.Printf("Link found: %q -> %s\n", e.Text, link)
		todayUrls = append(todayUrls, link)
	})

	// Before making a request print "Visiting ..."
	c.OnRequest(func(r *colly.Request) {
		fmt.Println("Visiting", r.URL.String())
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

	//起始路由改变
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
			todayUrls = append(todayUrls, link)
		}
	})

	// Before making a request print "Visiting ..."
	c.OnRequest(func(r *colly.Request) {
		fmt.Println("Visiting", r.URL.String())
	})

	// Start scraping on http://www.heze.cn/qiye/
	c.Visit("http://www.heze.cn/qiye/")

	return todayUrls
}

func main() {
	var todayUrls = getTodayUrls()
	fmt.Println(todayUrls)
	fmt.Println(len(todayUrls))

	//网页快照写入mongo，认为数据采集，除了二进制图像外，结束
	//剩余的html网页内容提取、手机号的图片ocr郊游python处理
	for  _,todayUrl := range todayUrls{
		// Instantiate default collector
		c := colly.NewCollector()
		// On every a element which has href attribute call callback
		c.OnScraped(func(r *colly.Response) {
			reqUrl:=fmt.Sprintln(r.Request.URL)
			strings.Replace(reqUrl, "\n", "", -1)
			wholePageHtml := string(r.Body)
			//client, err := mongo.Connect(context.Background(), "mongodb://192.168.3.103:27017?username=hbaseU&password=123", nil)
			client, err := mongo.Connect(context.Background(), "mongodb://hbaseU:123@192.168.3.103:27017/hbase", nil)
			db := client.Database("hbase")
			coll := db.Collection("todayUrls")
			//当天多次采集，当天url不重复入库
			//存入时间戳，分析目标站点的信息更新规律
			result, err := coll.InsertOne(
				context.Background(),
				bson.NewDocument(
					bson.EC.String("spiderDate", "20180829"),
					bson.EC.String("url", reqUrl),
					bson.EC.String("html", wholePageHtml),
				))
			fmt.Println(err)
			fmt.Println(result)
			fmt.Println(db)
		})
		// Start scraping on http://www.cnhan.com/shantui/
		c.Visit(todayUrl)
	}

}
