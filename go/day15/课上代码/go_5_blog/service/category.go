package service

import (
	"go_5_blog/dao/db"
	"go_5_blog/model"
)

// 获取所有分类
func GetALLCategoryList() (categoryList []*model.Category, err error) {
	categoryList, err = db.GetAllCategoryList()
	if err != nil {
		return
	}
	return
}
