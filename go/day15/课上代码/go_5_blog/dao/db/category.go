package db

import (
	"github.com/jmoiron/sqlx"
	"go_5_blog/model"
)

// 添加分类
func InsertCategory(category *model.Category) (categoryId int64, err error) {
	sqlstr := "insert into category(category_name,category_no) value(?,?)"
	result, err := DB.Exec(sqlstr, category.CategoryName, category.CategoryNo)
	if err != nil {
		return
	}
	categoryId, err = result.LastInsertId()
	return
}

// 获取单个分类
func GetCategoryById(id int64) (category *model.Category, err error) {
	category = &model.Category{}
	sqlstr := "select id ,category_name,category_no from category where id = ?"
	err = DB.Get(category, sqlstr, id)
	return
}

// 获取多个分类
func GetCategoryList(categoryIds []int64) (categoryList []*model.Category, err error) {
	sqlstr, args, err := sqlx.In("select id,category_name,category_no from category where id in(?)", categoryIds)
	if err != nil {
		return
	}
	// 查询
	err = DB.Select(&categoryList, sqlstr, args...)
	return
}

// 获取所有分类
func GetAllCategoryList() (categoryList []*model.Category, err error) {
	sqlstr := "select id ,category_name,category_no from category order by category_no asc"
	err = DB.Select(&categoryList, sqlstr)
	return
}
