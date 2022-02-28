// 总体思路：在本地 local dev这两个环境可以直接使用auto 模式。修改字段直接删除再新建不保留原来数据，让开发人员简单些，架构师多做一些。
// 在测试和生产 prod  pre-prod 环境上要严格遵循sql 语句来进行数据库的迁移（up down）具体操作步骤如下：
// 当从dev 到 pre-prod的时候，需要在dev 环境上编写 migrate sql 语句。并且这个语句是不被git 忽略的，以供测试环境和生产环境上使用
// 本系统提供了一个差异化工具，来对比变化在哪里，需要手动编辑来甄选具体的删除项。该文件是和软件系统版本有关系的，每次都覆盖最新的
// 换而言之，系统每次上线都有一个版本号，而该版本号包含两部分内容：1.代码；2.数据库迁移脚本；该版本号在.env 中配置，Config中全局可引用
// 如果同一个版本在测试环境上重复的修改数据库迁移脚本，每次都 执行 down 操作，在up 新的文件（这部分操作可自行通过代码完成）。
// 这样可以保证数据库迁移脚本的最小维护成本

package mdb

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

// ForceSync 简短粗暴，不兼容修改的情况。不一样就直接删除重建，自动更新
func ForceSync(charset string, localModels ...interface{}) error {
	// 将本地和远程翻译成 TableStruct
	var locals []TableStruct
	var remotes []TableStruct
	for _, model := range localModels {
		locals = append(locals, Model2Struct(model))
	}
	err, tables := getAllTables2Sql()
	if err != nil {
		return err
	}
	for tableName, _sql := range tables {
		log.WithFields(log.Fields{
			"tableName": tableName,
		}).Info(_sql)
		remotes = append(remotes, Sql2Struct(_sql[0]))
	}
	tableCompare := Compare(locals, remotes)
	//util.PrettyLog(tableCompare, false)
	err = ExecTableCompare(tableCompare, charset)
	return err
}

// ExecTableCompare 执行 差异化
func ExecTableCompare(tableCompare TableCompare, charset string) error {
	// 执行新表 创建
	for _, tableStruct := range tableCompare.CreateTables {
		log.Info("创建新表：" + tableStruct.TableName)
		_sql := tableStruct.GenCreateTableSql(charset)
		//util.PrettyLog(_sql, false)
		_, err := db.Exec(_sql)
		if err != nil {
			return err
		}
	}
	// 删除旧表
	for _, dropTable := range tableCompare.DropTables {
		log.Info("删除旧表：" + dropTable)
		_sql := "DROP TABLE " + dropTable
		if _, err := db.Exec(_sql); err != nil {
			return err
		}
	}
	// 处理相同表，字段列的不同变化
	for _, columnCompare := range tableCompare.Details {
		sqlBuilder := columnCompare.GenColumnSql()
		if len(sqlBuilder) == 0 {
			continue
		}
		log.Infof("start....---处理表 %s colmn 差异", columnCompare.TableName)
		for _, _sql := range sqlBuilder {
			log.Info(_sql)
			if _, err := db.Exec(_sql); err != nil {
				return err
			}
		}
		log.Infof("%s colmn 差异 ---....end", columnCompare.TableName)
	}
	return nil
}

// getAllTables2Sql 获取当前数据库所有表
func getAllTables2Sql() (err error, tableSqlMap map[string][]string) {
	tableSqlMap = make(map[string][]string)
	var rows *sql.Rows
	rows, err = db.Query("SHOW TABLES")
	if err != nil {
		return
	}
	for rows.Next() {
		var tableName, tableSql string
		err = rows.Scan(&tableName)
		if err != nil {
			return
		}
		err, tableSql = getTableSql(tableName)
		if err != nil {
			return
		}
		tableSqlMap[tableName] = append(tableSqlMap[tableName], tableSql)
	}
	return
}

// getTableSql 获取创建表的SQL语句
func getTableSql(tableName string) (error, string) {
	rows, err := db.Query("SHOW CREATE TABLE " + tableName)
	if err != nil {
		return err, ""
	}
	rows.Next()
	var tableSql  struct {
		TableN string
		TableS string
	}
	err = rows.Scan(&tableSql.TableN, &tableSql.TableS)
	if err != nil {
		return err, ""
	}
	return nil, tableSql.TableS
}
