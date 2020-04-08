package db

import (
	_ "github.com/go-sql-driver/mysql"
	"go_5_blog/model"
)

// 插入文章
func InsertArticle(article *model.ArticleDetail) (articleId int64, err error) {
	// 加个验证
	if article == nil {
		return
	}
	//``id``category_id``content``title``view_count``comment_count``username``STATUS``summary``create_time``update_time``
	sqlstr := `insert into
				article(content,summary,title,username,category_id,view_count,comment_count) values(?,?,?,?,?,?,?)`
	result, err := DB.Exec(sqlstr, article.Content, article.Summary, article.Title, article.Username, article.ArticleInfo.CategoryId,
		article.ArticleInfo.ViewCount, article.ArticleInfo.CommentCount)
	if err != nil {
		return
	}
	articleId, err = result.LastInsertId()
	return
}

// 获取文章列表，做个分页
func GetAricleList(pageNum, pageSize int) (articleList []*model.ArticleInfo, err error) {
	if pageNum < 0 || pageSize <= 0 {
		return
	}
	// 时间降序排序
	//``id``category_id``content``title``view_count``comment_count``username``STATUS``summary``create_time``update_time``
	sqlstr := `select
					id,summary,title,view_count,create_time,comment_count,username,category_id
			from
				article
			where
				status = 1
			order by create_time desc
			limit ?,?`
	err = DB.Select(&articleList, sqlstr, pageNum, pageSize)
	return
}

// 根据文章id，查询单个文章
func GetArticleDetail(articleId int64) (articleDetail *model.ArticleDetail, err error) {
	if articleId < 0 {
		return
	}
	articleDetail = &model.ArticleDetail{}
	sqlstr := `select
					id,summary,title,view_count,content,create_time,comment_count,username,category_id
			from
				article
			where
				id = ?
			and
				status = 1
			`
	err = DB.Get(articleDetail, sqlstr, articleId)
	return
}

// 根据分类id，查询这一类的文章
func GetArticleListByCategoryId(categoryId, pageNum, pageSize int) (articleList []*model.ArticleInfo, err error) {
	if pageNum < 0 || pageSize <= 0 {
		return
	}
	sqlstr := `select
					id,summary,title,view_count,create_time,comment_count,username,category_id
			from
				article
			where
				status = 1
			and
				category_id = ?
			order by create_time desc
			limit ?,?`
	err = DB.Select(&articleList, sqlstr, categoryId, pageNum, pageSize)
	return
}
