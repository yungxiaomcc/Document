package main

import (
	"github.com/samuel/go-zookeeper/zk"
	"fmt"
	"time"
)


//定义服务列表
var hosts = []string{"192.168.20.242:2181","192.168.20.237:2181","192.168.20.241:2181"}
//路径
var path = "/test01"
//定义为临时节点
var flags int32 = zk.FlagEphemeral
var data = []byte("hello xdl")
//全都定义为不控制权限
var acls = zk.WorldACL(zk.PermAll)

func main() {
	//设置监听的回调
	option := zk.WithEventCallback(callback)
	//创建连接
	conn,_,err := zk.Connect(hosts,time.Second*5,option)
	myErr(err)

	//为path节点设置监听
	_,_,_,err = conn.ExistsW(path)
	myErr(err)

	//创建节点
	create(conn,path,data)
	//time.Sleep(time.Second*2)
	//
	//_,_,_,err = conn.ExistsW(path)
	//myErr(err)
}

//回调用到的函数
func callback(event zk.Event)  {
	//打印节点的状态
	fmt.Println("*************************")
	fmt.Println("path:", event.Path)
	fmt.Println("type:",event.Type.String())
	fmt.Println("state:",event.State.String())
	fmt.Println("*************************")
}

func myErr(err error)  {
	if err != nil {
		fmt.Println(err)
		return
	}
}

//创建节点
func create(conn *zk.Conn,path string,data []byte)  {
	_,err_create := conn.Create(path,data,flags,acls)
	if err_create != nil{
		fmt.Println(err_create)
		return
	}
}










