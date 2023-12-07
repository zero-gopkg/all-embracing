package apolloconfig

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/apolloconfig/agollo/v4"
	"github.com/apolloconfig/agollo/v4/env/config"
	"github.com/apolloconfig/agollo/v4/storage"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
)

type Conf struct {
	mux       *sync.Mutex
	client    agollo.Client
	namespace string
	key       string
	rs        interface{}
	value     interface{}
}

type ApolloConf struct {
	AppID     string
	IP        string
	Secret    string
	Cluster   string
	Namespace string
	Key       string
	// 重试
	MaxRetries       int
	RetryIntervalSec int
}

var backFilePath string

/*
 * ConfApply 配置apply
 *
 * Request
 * @rs 指针类型变量，用于实时同步配置
 * @ac Apollo连接信息
 */
func ConfApply(rs interface{}, ac *ApolloConf) error {
	var (
		retries int
		err     error
	)
	backFilePath = fmt.Sprintf("./%s_%s.txt", ac.Namespace, ac.Key)

	for retries = 0; retries < ac.MaxRetries; retries++ {
		// 获取配置
		if err = tryFetchConfigFromApollo(rs, ac); err == nil {
			break
		}

		// 失败日志
		logx.Errorf("Failed to fetch config from Apollo. Err is %v,  Retry #%d\n", err, retries+1)

		// 重试间隔
		time.Sleep(time.Second * time.Duration(ac.RetryIntervalSec))
	}

	// 达到最大重试次数，执行兜底逻辑
	if retries == ac.MaxRetries {
		// 1、优先读取本地文件，上一次获取成功最新配置
		if err = restoreConfigFromFile(rs); err == nil {
			return nil
		}

		logx.Errorf("failed restored config from file. err:%s", err)
	}
	return err
}

//----------fetch config----------

// 尝试获取配置信息
func tryFetchConfigFromApollo(rs interface{}, ac *ApolloConf) error {
	cfg, err := newConf(rs, ac)
	if err != nil {
		return err
	}
	if err := cfg.Apply(); err != nil {
		return err
	}
	return nil
}

// NewCfg 配置解析
func newConf(rs interface{}, ac *ApolloConf) (*Conf, error) {
	client, err := agollo.StartWithConfig(func() (*config.AppConfig, error) {
		return &config.AppConfig{
			AppID:         ac.AppID,
			Cluster:       ac.Cluster,
			IP:            ac.IP,
			NamespaceName: ac.Namespace,
			Secret:        ac.Secret,
		}, nil
	})
	if err != nil {
		return nil, err
	}

	c := &Conf{
		mux:       &sync.Mutex{},
		client:    client,
		rs:        rs,
		namespace: ac.Namespace,
		key:       ac.Key,
	}
	return c, nil
}

func (z *Conf) Apply() error {
	if z.rs == nil {
		return errors.New("[conf] param rs is nil")
	}
	typ := reflect.TypeOf(z.rs)
	if typ.Kind() != reflect.Ptr {
		return errors.New("[conf] cannot apply to non-pointer struct")
	}

	if err := z.get(); err != nil {
		return err
	}

	if err := z.parse(); err != nil {
		return err
	}

	// 解析正确，本地文件备份
	if err := saveConfigToFile(z.value); err != nil {
		logx.Errorf("[conf] wsave config to file, err: %v", err)
	}

	// 动态监听
	z.client.AddChangeListener(z)
	return nil
}

func (z *Conf) get() error {
	z.mux.Lock()
	defer z.mux.Unlock()
	store := z.client.GetConfigCache(z.namespace)
	if store == nil {
		logx.Errorf("[conf] namespace [%s] not exist, please check it", z.namespace)
		return fmt.Errorf("[conf] namespace [%s] not exist, please check it", z.namespace)
	}

	v, err := store.Get(z.key)
	if err != nil {
		return err
	}

	z.value = v
	return nil
}

// --------本地文件持久化---------
func saveConfigToFile(value interface{}) error {
	// 写入文件
	logx.Info("writting config to file...")
	data := []byte(fmt.Sprintf("%v", value))
	return os.WriteFile(backFilePath, data, 0644)
}

func restoreConfigFromFile(rs interface{}) error {
	logx.Info("Restored config from file.")
	// 从文件中读取配置
	data, err := os.ReadFile(backFilePath)
	if err != nil {
		return err
	}

	return Unmarshal(data, rs)
}

func (z *Conf) parse() error {
	return Unmarshal([]byte(fmt.Sprintf("%v", z.value)), z.rs)
}

//----------Unmarshal----------

// Unmarshal 解析字符串
// 自动检测字符串类型
// 非json则进入yaml解析
func Unmarshal(data []byte, rs interface{}) error {
	if json.Valid(data) {
		return conf.LoadFromJsonBytes(data, rs)
	}
	return conf.LoadFromYamlBytes(data, rs)
}

// ----------- 实现监听

// OnChange 增加变更监控
func (z *Conf) OnChange(event *storage.ChangeEvent) {
	//fmt.Printf("OnChange: %+v \n", event)
}

// OnNewestChange 监控最新变更
func (z *Conf) OnNewestChange(event *storage.FullChangeEvent) {
	z.mux.Lock()
	defer z.mux.Unlock()

	if value, ok := event.Changes[z.key]; ok {
		z.value = value
	}

	if err := z.parse(); err != nil {
		logx.Infof("[conf] %s conf data parse fail, err [%s] please check it", z.namespace, err)
		return
	}

	// 解析正确，本地文件备份
	if err := saveConfigToFile(z.value); err != nil {
		logx.Errorf("[conf] %s [OnNewestChange] save config to file failed, err: %v", z.namespace, err)
	}

	logx.Infof("[conf] %s OnNewestChange parsed", z.namespace)
}
