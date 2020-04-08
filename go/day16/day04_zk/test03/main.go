package main

import (
	"github.com/samuel/go-zookeeper/zk"
	"time"
	"fmt"
	"encoding/json"
)

//模拟服务注册和消费
//对服务的管理

//模拟服务地址信息的结构体
type ServiceNode struct {
	Name string `json:"name"`
	Host string `json:"host"`
	Port int    `json:"port"`
}

//模拟客户端的结构体
type SdClient struct {
	//服务列表
	zkServers []string
	//服务的根目录
	zkRoot string
	//zk 的客户端连接
	conn *zk.Conn
}

//创建客户端
func NewClient(zkServers []string, zkRoot string) (*SdClient, error) {
	//创建客户端
	client := new(SdClient)
	//main()中去定义
	client.zkServers = zkServers
	client.zkRoot = zkRoot

	//连接服务器，指定超时时间
	conn, _, err := zk.Connect(zkServers, time.Second*5)
	if err != nil {
		return nil, err
	}

	//拿到连接
	client.conn = conn
	//创建服务根节点
	if err := client.createRoot(); err != nil {
		//关闭连接
		client.Close()
		return nil, err
	}
	return client, nil
}

// 创建服务根节点
func (s *SdClient) createRoot() error {
	//判断根节点是否存在，返回布尔值
	exists, _, err := s.conn.Exists(s.zkRoot)
	if err != nil {
		return err
	}
	//若根节点不存在，那么创建根节点
	if !exists {
		//创建持久化节点为根节点
		_, err := s.conn.Create(s.zkRoot, []byte(""), 0, zk.WorldACL(zk.PermAll))
		//集群多线程可能导致create返回已存在的错误
		if err != nil && err != zk.ErrNodeExists {
			return err
		}
	}
	return nil
}

//关闭连接，释放临时节点
func (s *SdClient) Close() {
	s.conn.Close()
}

func main() {
	//指定服务列表
	servers := []string{"node01:2181", "node02:2181", "node03:2181"}
	//创建客户端，创建根节点
	client, err := NewClient(servers, "/api")
	if err != nil {
		fmt.Println(err)
	}

	//模拟zk注册服务
	node1 := &ServiceNode{"user", "127.0.0.1", 4000}
	node2 := &ServiceNode{"user", "127.0.0.1", 4001}
	node3 := &ServiceNode{"user", "127.0.0.1", 4002}
	//将三个模拟的节点，注册到zk
	if err := client.Register(node1); err != nil {
		fmt.Println(err)
	}
	if err := client.Register(node2); err != nil {
		fmt.Println(err)
	}
	if err := client.Register(node3); err != nil {
		fmt.Println(err)
	}

	//模拟消费者
	//将三个注册节点的数据打印
	nodes, err := client.GetNodes("/api", "/user")
	if err != nil {
		fmt.Println(err)
	}
	for _, node := range nodes {
		fmt.Println(node.Host, node.Port)
	}
}

//模拟服务注册的方法
func (s *SdClient) Register(node *ServiceNode) error {
	//创建/api/user 目录
	if err := s.ensureName(node.Name); err != nil {
		return err
	}

	path := s.zkRoot + "/" + node.Name + "/n"
	//模拟给节点设置数据，将node转为数组
	data, err := json.Marshal(node)
	if err != nil {
		return err
	}

	//在/api/user下面，创建临时有序子节点
	_, err = s.conn.CreateProtectedEphemeralSequential(path, data, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}
	return nil
}

func (s *SdClient) ensureName(name string) error {
	//定义检查的目录 /api/user
	path := s.zkRoot + "/" + name
	//验证目录是否存在
	exists, _, err := s.conn.Exists(path)
	if err != nil && err != zk.ErrNodeExists {
		return err
	}
	//若目录不存在，则创建
	if !exists {
		//创建持久化节点 /api/user
		_, err := s.conn.Create(path, []byte(""), 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			return err
		}
	}
	return nil
}

//创建消费者的方法
func (s *SdClient) GetNodes(zkRoot string, name string) ([]*ServiceNode, error) {
	//定义路径
	// 目录：/api/user
	path := zkRoot + name
	//获取/api/user目录下，所有子节点名称
	childs, _, err := s.conn.Children(path)
	if err != nil {
		if err == zk.ErrNoNode {
			return []*ServiceNode{}, nil
		}
		return nil, err
	}
	nodes := []*ServiceNode{}
	for _, child := range childs {
		// path:/api/user
		fullPath := path + "/" + child
		//得到具体遍历到的子节点
		data, _, err := s.conn.Get(fullPath)
		if err != nil {
			if err == zk.ErrNoNode {
				continue
			}
			return nil, err
		}
		node := new(ServiceNode)
		err = json.Unmarshal(data, node)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}
