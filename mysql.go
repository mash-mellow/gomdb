package mdb

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"strings"
	"time"
)

type Mode int8

const (
	ReadOnly Mode = 1 << iota // 1 << 0 which is 00000001
	Write                         // 1 << 1 which is 00000010
)

const (
	Pending = iota
	Fail
	Success
)

// 全局对象，只在该文件出现
var db *sql.DB

var currConf *Config
type DealSession func(sess *Session) error
type DealWrite func(sess *sqlx.Tx) error

type Config struct {
	DbName string
	Host string
	UserName string
	Password string
	MaxOpenConns int
	MaxIdleConns int
}

// Session 核心操作类
type Session struct {
	Read *sqlx.DB
	Write *sqlx.Tx
}

// TransModel 微事务数据库同步
type TransModel struct {
	TransId string
	TimeoutCxt context.Context
}

// ModelValidator models中如果实现了这个接口，就会自动调；常见判断 status type 等是否是给定的值
type ModelValidator interface {
	Validate() bool
}

// DefaultDbValidate 所有的models 都会包含该结构体 为了校验
type DefaultDbValidate struct {}

func (dbv DefaultDbValidate) Validate() bool {
	// 等待其model对象实现，不实现就什么也不做
	return true
}

// In 工具方法 给db models使用的
func (dbv DefaultDbValidate) In(target interface{}, sources ...interface{}) bool  {
	for _, s := range sources {
		if s == target {
			return true
		}
	}
	return false
}

// NotIn 工具方法 给db models使用的
func (dbv DefaultDbValidate) NotIn(target interface{}, sources ...interface{}) bool  {
	for _, s := range sources {
		if s == target {
			return false
		}
	}
	return true
}

// InitDB 连接数据库
func InitDB(conf Config) {
	logFields := log.Fields{
		"$userName": conf.UserName,
		"$password": conf.Password,
		"$dbName": conf.DbName,
		"$host": conf.Host,
		"MaxOpenConns": conf.MaxOpenConns,
		"MaxIdleConns": conf.MaxIdleConns,
	}
	if currConf != nil || db != nil {
		log.WithFields(logFields).Panic("mdb has been init...")
		return
	}
	dsn := "$userName:$password@tcp($host)/$dbName?charset=utf8mb4&parseTime=True"
	dsn = strings.Replace(dsn, "$userName", conf.UserName, 1)
	dsn = strings.Replace(dsn, "$password", conf.Password, 1)
	dsn = strings.Replace(dsn, "$dbName", conf.DbName, 1)
	dsn = strings.Replace(dsn, "$host", conf.Host, 1)
	var err error
	// 连接数据库 open + ping
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		log.WithFields(logFields).Panicf("connect DB failed, err:%v\n", err)
		return
	}
	// 最大连接数
	db.SetMaxOpenConns(conf.MaxOpenConns)
	// 空闲链接数
	db.SetMaxIdleConns(conf.MaxIdleConns)
	currConf = &conf
}

//func ReadOnlyDb()  {
//
//}

// ObtainSession 获取数据库 session；微事务同步 在这个方法中实现
func ObtainSession(mode Mode, deal DealSession, model *TransModel) error {
	var err error
	//if model != nil && model.TransId != "" && mode == ReadOnly {
	//	log.Panic("mode error! should be Write...")
	//}
	//if mode == Write {
	//	var tx *sql.Tx
	//	tx, err = db.Begin()
	//	if err != nil {
	//		log.Panicf("begin trans failed, err:%v\n", err)
	//	}
	//	defer func() {
	//		// 捕获panic
	//		if p := recover(); p != nil {  // 回滚
	//			err = tx.Rollback()
	//			log.Error(err.Error() + "\n" + string(util.PanicTrace(4))) // re-throw panic after Rollback
	//		} else if err != nil {
	//			log.Warning("---rollback")
	//			err = tx.Rollback() // err is non-nil; don't change it
	//		} else {
	//			err = transCommit(tx, model)
	//		}
	//		if err != nil {
	//			log.Warningf("end trans failed, err:%v\n", err)
	//		}
	//	}()
	//	err = deal(&Session{Write: tx})
	//	return err
	//}
	//err = deal(&Session{Read: db})
	return err
}

// getTransResult 从redis中获取微事务结果
func getTransResult(transId string) int8 {
	fmt.Println(transId, "TODO 从redis中获取")
	return Pending
}

// transCommit  等待事务成功或失败，失败回滚
// 内部启动协程，不阻塞
func transCommit(tx *sqlx.Tx, model *TransModel) error {
	if model == nil || model.TransId == "" {
		return tx.Commit()
	}
	// 启动协程，在协程中等待
	go func () {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		// 等待结果，每50ms读取redis获取
		for {
			select {
			case <-model.TimeoutCxt.Done():  // 超时回滚
				err := tx.Rollback()
				if err != nil {
					log.Warningf("transCommit -end- trans failed, err:%v\n", err)
				}
				return
			case <-ticker.C:
				// 判断等待结果
				switch status := getTransResult(model.TransId); status {
				case Pending:
					// do nothing...
				case Success:
					err := tx.Commit()
					if err != nil {
						log.Warningf("transCommit end trans failed, err:%v\n", err)
					}
					return
				case Fail:
					err := tx.Rollback()
					if err != nil {
						log.Warningf("transCommit end- trans failed, err:%v\n", err)
					}
					return
				}
			}
		}
	}()
	return nil
}

func GetDb() *sql.DB {
	return db
}


