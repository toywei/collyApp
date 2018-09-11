package main

import (
	"fmt"
	"github.com/gocolly/colly"
	"github.com/gocolly/colly/proxy"
	"strconv"
	"strings"
	"log"
	pTool "./mypack"
)

type ProxyIp struct {
	Ip                      string
	Port                    int
	IsHttps                 bool
	UpdateTime              int
	SourceUrl               string
	TimeTolive              int
	AnonymousInfo           string
	Area                    string
	InternetServiceProvider string
}

var ProxyIpPool []ProxyIp

func main() {
	p := &ProxyIpPool
	SourceUrl := "http://www.xicidaili.com/nt/"
	// Instantiate default collector
	c := colly.NewCollector(
		// MaxDepth is 2, so only the links on the scraped page
		// and links on those pages are visited
		colly.MaxDepth(1),
		colly.Async(true),
	)

	// Limit the maximum parallelism to 1
	// This is necessary if the goroutines are dynamically
	// created to control the limit of simultaneous requests.
	//
	// Parallelism can be controlled also by spawning fixed
	// number of go routines.
	c.Limit(&colly.LimitRule{DomainGlob: "*", Parallelism: 12})

	// On every a element which has href attribute call callback
	c.OnHTML("tr", func(e *colly.HTMLElement) {
		var item ProxyIp
		e.ForEach("td", func(i int, element *colly.HTMLElement) {
			t := element.Text
			switch i {
			case 1:
				item.Ip = t
				break
			case 2:
				p, n := strconv.Atoi(t)
				if n == nil {
					item.Port = p
				}
				break
			case 3:
				item.Area = t
				break
			case 4:
				item.IsHttps = strings.Contains(strings.ToLower(t), "https")
				break
			default:
				break
			}

		})
		item.SourceUrl = SourceUrl
		*p = append(*p, item)
	})

	// Start scraping on https://en.wikipedia.org
	c.Visit(SourceUrl)
	// Wait until threads are finished
	c.Wait()

	fmt.Println(*p)
	fmt.Println("fmt.Println(*p)----------------------------------->")

	var a [] string
	for _, v := range *p {
		http := "http"
		if v.IsHttps {
			http = "https"
		}
		if v.Ip != "" && v.Port != 0 {
			s := http + "://" + v.Ip + ":" + strconv.Itoa(v.Port)
			fmt.Println(s)
			a = append(a, s)
		}
	}

	fmt.Println("fmt.Println(*p)<-----------------------------------")
	c = colly.NewCollector(
		colly.AllowedDomains("cn.sonhoo.com"),
	)

	rp, err := proxy.RoundRobinProxySwitcher(a...)

	if err != nil {
		log.Fatal(err)
	}
	c.SetProxyFunc(rp)

	c.OnHTML("a[href]", func(e *colly.HTMLElement) {
		link := e.Attr("href")
		fmt.Printf("Link found: %q -> %s\n", e.Text, link)
		c.Visit(e.Request.AbsoluteURL(link))
	})
	c.OnRequest(func(r *colly.Request) {
		fmt.Println("Visiting", r.URL.String())
		r.Headers.Set("User-Agent", pTool.RandomString())
	})
	c.Visit("http://cn.sonhoo.com/wukong/a58627")

}
