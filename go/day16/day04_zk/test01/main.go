package main

import (
	"github.com/samuel/go-zookeeper/zk"
	"time"
	"fmt"
)

func main()  {
	//指定服务列表
	//只能写主机名，那么本机也要配hosts映射
	strs := []string{"192.168.20.242:2181","192.168.20.237:2181","192.168.20.241:2181"}
	//获取连接
	conn := getConn(strs)

	//创建节点
	var  path  = "/test"
	var data  = []byte("hello zk")
	//flags：有四种值
	//0：代表永久节点
	//1：临时节点
	//2:有序节点
	//3：临时有序节点
	var flags int32 = 0
	createNode(conn,path,data,flags)

	//直接查看目录下所有节点
	children,_,err := conn.Children("/")
	myErr(err)
	fmt.Printf("%v \n",children)

	//获取节点的信息
	data,stat,err:=conn.Get("/test")
	myErr(err)
	fmt.Printf("获取到节点：%+v %+v\n",string(data),stat)

	//节点修改值
	stat ,err = conn.Set("/test",[]byte("newdata"),stat.Version)
	myErr(err)
	fmt.Println("节点修改成功")

	//删除节点
	err = conn.Delete("/test",-1)
	myErr(err)
	fmt.Println("删除节点成功")

	//查看节点是否存在
	exists,stat,err := conn.Exists("/test")
	myErr(err)
	fmt.Println(exists,stat)

}

//获取连接的方法
func getConn(strs []string) *zk.Conn  {
	//参数：服务列表和超时时间，这里指定5秒
	conn ,_,err:=zk.Connect(strs,time.Second*5)
	//创建处理异常方法
	myErr(err)
	return conn
}

func myErr(err error)  {
	if err != nil{
		fmt.Println(err)
	}
}

// 创建节点的方法
func createNode(conn *zk.Conn,path string,data []byte,flags int32)  {
	//创建节点
	//zk.WorldACL(zk.PermAll)表示该节点没有权限限制
	path,err_create :=conn.Create(path,data,flags,zk.WorldACL(zk.PermAll))
	if err_create !=nil{
		fmt.Println(err_create)
		return
	}
	fmt.Println("创建了新节点：",path)
}

