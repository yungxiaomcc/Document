package model

import "time"

//``id``category_id``content``title``view_count``comment_count``username``STATUS``summary``create_time``update_time``

// 定义文章结构体
type ArticleInfo struct {
	Id         int64 `db:"id"`
	CategoryId int64 `db:"category_id"`
	// 文章摘要
	Summary   string `db:"summary"`
	Title     string `db:"title"`
	ViewCount uint32 `db:"view_count"`
	// 时间
	CreateTime   time.Time `db:"create_time"`
	CommentCount uint32    `db:"comment_count"`
	Username     string    `db:"username"`
}

// 用于文章详情页的实体
// 为了提升效率
type ArticleDetail struct {
	ArticleInfo
	// 文章内容
	Content string `db:"content"`
	Category
}

// 用于文章上下页
type ArticleRecord struct {
	ArticleInfo
	Category
}
