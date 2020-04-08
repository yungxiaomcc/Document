package main

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

func main() {
	//初始化数据库
	err := initDB()
	if err != nil {
		panic(err)
	}
	r := gin.Default()
	// 加载页面
	r.LoadHTMLGlob("./book/templates/*")
	// 查询所有图书
	r.GET("/book/list", bookListHandler)
	_ = r.Run(":8000")
}

func bookListHandler(c *gin.Context) {
	bookList, err := queryAllBook()
	if err != nil {
		c.JSON(http.StatusOK, gin.H{
			"code": 1,
			"msg":  err,
		})
		return
	}
	// 返回数据
	c.HTML(http.StatusOK, "book_list.html", gin.H{
		"code": 0,
		"data": bookList,
	})
}
