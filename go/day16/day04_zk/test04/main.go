package main

import (
	"github.com/samuel/go-zookeeper/zk"
	"time"
	"fmt"
	"errors"
)

//定义zk的配置信息结构体
type ZookeeperConfig struct {
	//服务列表
	Servers    []string
	RootPath   string
	MasterPath string
}

//定义选举管理信息
type ElectionManager struct {
	//zk连接
	ZKClientConn *zk.Conn
	//zk配置
	ZKConfig *ZookeeperConfig
	//选举信息传递的信道
	IsMaster chan bool
}

func main() {
	//zk配置信息
	zkconfig := &ZookeeperConfig{
		Servers:    []string{"node01:2181", "node02:2181", "node03:2181"},
		RootPath:   "/test04",
		MasterPath: "/master",
	}

	//建立用于返回选取结果的信道
	isMasterChan := make(chan bool)

	//创建选举管理器，建立连接
	electionManager := NewElectionManager(zkconfig, isMasterChan)

	//节点的选举的逻辑
	//隐含：节点的监视，主挂掉，立刻再重新选举上位
	go electionManager.Run()

	//判断信道的返回值，做具体的业务
	var isMaster bool
	for {
		select {
		case isMaster = <-isMasterChan:
			if isMaster {
				//做具体的业务
				fmt.Println("实现具体的业务逻辑")
			}
		}
	}

}

//定义选举管理器
func NewElectionManager(zkConfig *ZookeeperConfig, isMaster chan bool) *ElectionManager {
	//创建选举管理信息对象
	electionManager := &ElectionManager{
		nil,
		zkConfig,
		isMaster,
	}

	//初始化连接
	electionManager.initConnection()

	return electionManager
}

//初始化zk连接
func (electionManager *ElectionManager) initConnection() error {
	//判断是否已经连接
	//如果连接有问题，或者没有连接的情况下
	if !electionManager.isConnected() {
		//连接有问题或未连接
		//连接zk
		conn, connChan, err := zk.Connect(electionManager.ZKConfig.Servers, time.Second*5)
		if err != nil {
			return err
		}
		for {
			isConnected := false
			select {
			case connEvcent := <-connChan:
				//判断信道的State是否是连接状态
				if connEvcent.State == zk.StateConnected {
					//连接成功
					isConnected = true
					fmt.Println("zk连接成功")
				}
				//如果3秒未成功，则返回连接超时
			case _ = <-time.After(time.Second * 3):
				return errors.New("zk连接超时！")
			}
			//当连接成功时跳出
			if isConnected {
				break
			}
		}
		//为ZKClientConn拿到连接
		electionManager.ZKClientConn = conn
	}
	return nil
}

//判断是否已经建立连接
func (electionManager *ElectionManager) isConnected() bool {
	//判断连接，也就是判断ZKClientConn是否为nil
	if electionManager.ZKClientConn == nil {
		//没有连接
		return false
	} else if electionManager.ZKClientConn.State() != zk.StateConnected {
		//连接有问题
		return false
	}
	return true
}

//选举的逻辑
func (electionManager *ElectionManager) Run() {
	//选主
	err := electionManager.electMaster()
	if err != nil{
		fmt.Println(err)
	}
	//监听master的方法
	electionManager.watchMaster()
}

//选主
func (electionManager *ElectionManager) electMaster() error {
	//严谨的逻辑判断
	//判断连接是否有问题
	err := electionManager.initConnection()
	if err != nil {
		return err
	}
	//判断zk中是否存在根目录
	isExist, _, err := electionManager.ZKClientConn.Exists(electionManager.ZKConfig.RootPath)
	if err != nil {
		return err
	}
	//不存在根目录
	if !isExist {
		//创建根目录，数据先不设，flags为0，创建持久化节点，权限不控制
		path, err := electionManager.ZKClientConn.Create(electionManager.ZKConfig.RootPath,
			nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
		if electionManager.ZKConfig.RootPath != path {
			return errors.New("创建的" + electionManager.ZKConfig.RootPath + " !=" + path)
		}
	}

	//拼接master地址
	masterPath := electionManager.ZKConfig.RootPath + electionManager.ZKConfig.MasterPath
	//创建master，创建个临时节点
	//zk.FlagEphemeral:1:临时节点
	path, err := electionManager.ZKClientConn.Create(masterPath, nil, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	//创建成功，表示选举master成功
	//将每次程序启动，理解为一个客户端节点
	//哪个客户端节点创建了master，认为是选举成功
	if err == nil {
		if path == masterPath {
			fmt.Println("选举master成功")
			electionManager.IsMaster <- true
		} else {
			return errors.New("创建的" + masterPath + "!=" + path)
		}
	} else {
		//创建master节点失败
		fmt.Println("选举master失败！", err)
		electionManager.IsMaster <- false
	}
	return nil
}

//监听master znode的作用
func (electionManager *ElectionManager) watchMaster() error {
	//主挂掉的情况下：机器咔嚓了，zk节点删掉，也认为主挂了，然后重新选举
	for {
		//监听zk根znode下的子znode
		//如果连接断开或对应的znode被删除，则触发重新选举
		//监听目录下所有子节点
		children,state,childCh,err := electionManager.ZKClientConn.ChildrenW(electionManager.ZKConfig.RootPath + electionManager.ZKConfig.MasterPath)
		if err != nil {
			fmt.Println("监听失败！",err)
		}
		fmt.Println("监听到子节点",children,state)
		select {
		case childEvent := <- childCh:
			//监听节点的删除事件
			if childEvent.Type == zk.EventNodeDeleted{
				fmt.Println("接收到znode的删除事件",childEvent)
				//重新选举
				fmt.Println("开始选举新的master...")
				err = electionManager.electMaster()
				if err != nil{
					fmt.Println("选举新的master失败",err)
				}
			}
		}

}
}

















