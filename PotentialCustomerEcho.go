package main

import (
	"net/http"

	"github.com/labstack/echo"
	"io"
	"html/template"
	"fmt"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"log"
)

type PotentialCustomerClean struct {
	ComName     string `bson:"comName"`
	TelPhone    string `bson:"telPhone"`
	MobilePhone string `bson:"mobilePhone"`
	Addr        string `bson:"addr"`
	WebSite     string `bson:"webSite"`
	Qq          string `bson:"qq"`
}

//zaimongodb中为一个key
type PotentialCustomerOneKey struct {
	CleanData PotentialCustomerClean
}

type PotentialCustomerCleanHtml struct {
	ComName     string
	TelPhone    string
	MobilePhone string
	Addr        string
	WebSite     string
	Qq          string
}

type PotentialCustomerDetail struct {
	Url        string `bson:"url",json:"url"`
	ContactUrl string `bson:"contactUrl"`
	ComName    string `bson:"comName"`
	ComInfo    string `bson:"comInfo"`
	ComInfoTxt string `bson:"comInfoTxt"`
	TelImg     string `bson:"telImg"`
	PhoneImg   string `bson:"phoneImg"`
	WxImg      string `bson:"wxImg"`
	SpiderDate string `bson:"spiderDate"`
}

/*
1.实现 echo.Renderer 接口
*/
type Template struct {
	templates *template.Template
}

func (t *Template) Render(w io.Writer, name string, data interface{}, c echo.Context) error {
	return t.templates.ExecuteTemplate(w, name, data)
}

/*
4.在 action 中渲染模板
*/
func Hello(c echo.Context) error {
	return c.Render(http.StatusOK, "WeUI", "chkUrl")
}

/*
自定义一个 context
Define a custom context
Context - Go/Golang 框架 Echo 文档 http://go-echo.org/guide/context/
*/

type CustomContext struct {
	echo.Context
}

func (c *CustomContext) Foo() {
	println("foo")
}

func (c *CustomContext) Bar() {
	println("bar")
}

type ScriptStruct struct {
	Host       string
	Port       int
	Path       string
	ScriptName string
}

var s2 = ScriptStruct{"192.168.3.123", 8088, "/myDir/", "spider.go"}
//var ScriptArr [6]ScriptStruct
var ScriptArr [2]ScriptStruct

func (c *CustomContext) DumpScripts() {
	println("bar")
	ScriptArr[0] = ScriptStruct{"192.168.3.103", 8088, "/home/goDev/spider/", "spiderChkurl.go"}
	ScriptArr[1] = ScriptStruct{"192.168.3.110", 8088, "/home/goDev/spider/", "spiderChkurl.go"}
	//ScriptArr[2] = ScriptStruct{"192.168.3.123", 8088, "/home/goDev/spider/", "spiderChkurl.go"}
}

func main() {
	/*
	2.预编译模板
	*/
	t := &Template{
		templates: template.Must(template.ParseGlob("goEchopublic/views/*.html")),
	}

	/*
	3.注册模板
	*/
	e := echo.New()
	e.Renderer = t

	/*
	静态文件
	Echo#Static(prefix, root string) 用一个 url 路径注册一个新的路由来提供静态文件的访问服务。root 为文件根目录。
	这样会将所有访问/static/*的请求去访问assets目录。例如，一个访问/static/js/main.js的请求会匹配到assets/js/main.js这个文件。
	*/
	e.Static("/static", "assets")

	/*
	创建一个中间件来扩展默认的 context
	Create a middleware to extend default context
	*/

	e.Use(func(h echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			cc := &CustomContext{c}
			return h(cc)
		}
	})
	/*
	这个中间件要在所有其它中间件之前注册到路由上。
	This middleware should be registered before any other middleware.
	*/

	/*
	在业务处理中使用
	Use in handler
	*/
	e.GET("/scriptAdmin01", Hello)

	e.GET("/WeUI", func(c echo.Context) error {
		fmt.Println("test-")
		return c.Render(http.StatusOK, "WeUI", "data--")
	})
	e.GET("/scriptAdmin", func(c echo.Context) error {
		cc := c.(*CustomContext)
		cc.Foo()
		cc.Bar()
		cc.DumpScripts()
		fmt.Println(ScriptArr)
		return c.Render(http.StatusOK, "hello", s2)
	})

	e.GET("/chkUrl", func(c echo.Context) error {
		fmt.Println("test-chkUrl")
		cc := c.(*CustomContext)
		cc.DumpScripts()
		return c.Render(http.StatusOK, "chkUrl", ScriptArr)
	})
	e.POST("/chkUid", func(c echo.Context) error {
		fmt.Println("chkUid")
		uid := c.FormValue("uid")
		fmt.Println(uid)
		cc := c.(*CustomContext)
		cc.DumpScripts()

		msg := "uid:" + uid + "处理中。。。进度xxx"
		fmt.Println(msg)

		return c.Render(http.StatusOK, "tmp", msg)
	})

	/*
		e.GET("/GetDBHtmlCode/:iid", func(c echo.Context) error {
		info_id := c.Param("iid")
		cc := c.(*CustomContext)
		cc.GetDBHtmlCode(info_id)
		return c.Render(http.StatusOK, "DBHtmlCode", DBHtmlCode)
	})

	*/
	e.GET("/PotentialCustomerDetail/:spiderDate", func(c echo.Context) error {
		spiderDate := c.Param("spiderDate")

		//查询mongodb数据
		session, err := mgo.Dial("mongodb://hbaseU:123@192.168.3.103:27017/hbase")
		if err != nil {
			panic(err)
		}
		defer session.Close()
		// Optional. Switch the session to a monotonic behavior.
		session.SetMode(mgo.Monotonic, true)
		Collection := session.DB("hbase").C("todayUrls")
		var resultSOnline [] PotentialCustomerDetail
		err = Collection.Find(bson.M{"spiderDate": spiderDate}).All(&resultSOnline)
		if err != nil {
			log.Fatal(err)
		}

		return c.Render(http.StatusOK, "PotentialCustomerDetail", resultSOnline)
	})

	e.GET("/PotentialCustomer/:spiderDate", func(c echo.Context) error {
		spiderDate := c.Param("spiderDate")

		//查询mongodb数据
		session, err := mgo.Dial("mongodb://hbaseU:123@192.168.3.103:27017/hbase")
		if err != nil {
			panic(err)
		}
		defer session.Close()
		// Optional. Switch the session to a monotonic behavior.
		session.SetMode(mgo.Monotonic, true)
		Collection := session.DB("hbase").C("todayUrls")
		var resultSOnline [] PotentialCustomerClean
		err = Collection.Find(bson.M{"spiderDate": spiderDate}).All(&resultSOnline)
		if err != nil {
			log.Fatal(err)
		}
		// TODO 当日重复客户资料过滤：同平台的重复、跨平台的重复
		fmt.Println(resultSOnline)
		return c.Render(http.StatusOK, "PotentialCustomer", resultSOnline)
	})

	e.Logger.Fatal(e.Start(":1324"))
}
