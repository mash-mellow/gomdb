package mdb

import (
	"fmt"
	mapset "github.com/deckarep/golang-set"
	log "github.com/sirupsen/logrus"
	"reflect"
	"sort"
	"strings"
)

// TableStruct 表结构描述
type TableStruct struct {
	TableName   string
	Columns     []string
	PrimaryKeys []string
	Indexes     []string
	ColumnTypes map[string]string
	Constraints map[string]string
}

// TableCompare 对比结构
type TableCompare struct {
	CreateTables []TableStruct
	DropTables   []string
	Details      []ColumnCompare
}

// ColumnCompare 依赖 TableCompare，具体的某一个表的变化信息
// 如果字段从 varchar 修改为 int 这个处理为删除重建，只能同一类型间修改
type ColumnCompare struct {
	TableName  string
	AddColumn  []ColumnDetail
	DropColumn []string
	// mysql drop 只能全部删除，这里只要有不一样就新建了
	AddPrimary    []string
	NeedDropPrimary    bool  // 是否需要drop（比如一个表中原来没有 primary 后来加了一个）
	AddIndex      []string
	DropIndex     []string
	ColumnChanged []RecordCompare
}

// ColumnDetail 对一个列的描述，用于生成一个列的添加语句
type ColumnDetail struct {
	Column      string
	Type        string
	Constraints string
}

// RecordCompare 前后对比
type RecordCompare struct {
	Column      string
	Before      string
	Now         string
	LocalColumn ColumnDetail
}

// Sql2Struct 将数据库的create table sql 语句转换成 TableStruct
func Sql2Struct(createTableSql string) (tableStruct TableStruct) {
	tableStruct.ColumnTypes = make(map[string]string)
	tableStruct.Constraints = make(map[string]string)
	for _, stmt := range strings.Split(createTableSql, "\n") {
		stmt = strings.TrimLeft(stmt, " ")
		if strings.HasPrefix(stmt, "CREATE TABLE ") {
			start := strings.Index(stmt, "`")
			end := strings.LastIndex(stmt, "`")
			tableStruct.TableName = stmt[start+1 : end]
		} else if strings.HasPrefix(stmt, "`") {
			start := strings.Index(stmt, "`")
			end := strings.LastIndex(stmt, "`")
			column := stmt[start+1 : end]
			tableStruct.Columns = append(tableStruct.Columns, column)
			columnType := strings.Split(stmt, " ")[1]
			if column == "db_decimal" {
				print("jjj")
			}
			if strings.HasSuffix(columnType, ",") {
				columnType = strings.Replace(columnType, ",", "", 1)
				tableStruct.ColumnTypes[column] = columnType
				continue
			}
			tableStruct.ColumnTypes[column] = columnType
			typeStart := strings.Index(stmt, columnType)
			_constrain := strings.TrimLeft(stmt[typeStart+len(columnType):], " ")
			_constrain = strings.Replace(_constrain, ",", "", 1)
			tableStruct.Constraints[column] = strings.ToLower(_constrain)
		} else if strings.HasPrefix(stmt, "PRIMARY KEY ") {
			stmt = strings.Replace(stmt, "`", "", -1)
			start := strings.Index(stmt, "(")
			end := strings.LastIndex(stmt, ")")
			tableStruct.PrimaryKeys = strings.Split(stmt[start+1:end], ",")
		} else if strings.HasPrefix(stmt, "KEY ") {
			stmt = strings.Replace(stmt, "`", "", -1)
			start := strings.Index(stmt, "(")
			end := strings.LastIndex(stmt, ")")
			tableStruct.Indexes = append(tableStruct.Indexes, stmt[start+1:end])
		}
	}
	// 只有 varchar 和 Decimal 才有实际意义的长度，其他的没有
	_ctMap := make(map[string]string)
	for column, cType := range tableStruct.ColumnTypes {
		if strings.HasPrefix(cType, "varchar") || strings.HasPrefix(cType, "decimal") {
			_ctMap[column] = cType
			continue
		}
		start := strings.Index(cType, "(")
		if start == -1 {
			_ctMap[column] = cType
			continue
		}
		_ctMap[column] = cType[:start]
	}
	tableStruct.ColumnTypes = _ctMap
	return
}

// Model2Struct 将Model 语句转换成 TableStruct
func Model2Struct(model interface{}) (tableStruct TableStruct) {
	tableStruct.ColumnTypes = make(map[string]string)
	tableStruct.Constraints = make(map[string]string)
	rt := reflect.TypeOf(model)
	if rt.Kind() != reflect.Ptr {
		log.Panic("table must a pointer!")
	}
	// 获取表明并替换
	_array := strings.Split(rt.String(), ".")
	tableStruct.TableName = UnMarshal4Camel(_array[len(_array)-1])
	rte := reflect.TypeOf(model).Elem() //通过反射获取type定义
	for i := 0; i < rte.NumField(); i++ {
		var sparedDefault bool
		columnName := UnMarshal4Camel(rte.Field(i).Name)
		tableStruct.Columns = append(tableStruct.Columns, columnName)
		constraint := strings.ToLower(rte.Field(i).Tag.Get("mdb"))
		if strings.Index(constraint, "index") != -1 {
			tableStruct.Indexes = append(tableStruct.Indexes, columnName)
		}
		if strings.Index(constraint, "primary key") != -1 {
			tableStruct.PrimaryKeys = append(tableStruct.PrimaryKeys, columnName)
			sparedDefault = true
		}
		_array = strings.Split(rte.Field(i).Type.String(), ".")
		dbType := strings.Replace(_array[len(_array)-1], "mdb", "", 1)
		if dbType == "Bool" {
			dbType = "Tinyint"
		} else if dbType == "Varchar" || dbType == "Decimal" {
			lengthStart := strings.Index(constraint, "length:")
			if lengthStart == -1 {
				log.WithFields(log.Fields{
					"table": tableStruct.TableName, "column": columnName, "type": dbType,
				}).Panic("对应类型请通过length字段描述！")
			}
			length := strings.Split(strings.Split(constraint, "length:")[1], " ")[0]
			if strings.Index(length, "_") != -1 {
				_l := strings.Split(length, "_")
				dbType = fmt.Sprintf("%s(%s,%s)", dbType, _l[0], _l[1])
			} else {
				dbType = fmt.Sprintf("%s(%s)", dbType, length)
			}
			constraint = strings.Replace(constraint, "length:", "", 1)
			constraint = strings.Replace(constraint, length, "", 1)
		}
		if dbType == "Text" || dbType == "Blob" {
			sparedDefault = true
		}
		tableStruct.ColumnTypes[columnName] = strings.ToLower(dbType)
		constraint = strings.Replace(constraint, "index", "", 1)
		constraint = strings.Replace(constraint, "primary key", "", 1)
		if strings.Index(constraint, "default") == -1 &&
			strings.Index(constraint, "null") == -1 && !sparedDefault {
			constraint += "default null"
		}
		tableStruct.Constraints[columnName] = strings.TrimRight(strings.TrimLeft(constraint, " "), " ")
	}
	return
}

// Compare 本地 和 远程相比较，最终得到差异
func Compare(locals, remotes []TableStruct) (tableCompare TableCompare) {
	var localMap = make(map[string]TableStruct)
	var remoteMap = make(map[string]TableStruct)
	localSet := mapset.NewSet()
	remoteSet := mapset.NewSet()
	for _, lt := range locals {
		localMap[lt.TableName] = lt
		localSet.Add(lt.TableName)
	}
	for _, rt := range remotes {
		remoteMap[rt.TableName] = rt
		remoteSet.Add(rt.TableName)
	}
	difference := localSet.Difference(remoteSet)
	for v := range difference.Iter() {
		if localSet.Contains(v) { // 说明是新增表
			tableCompare.CreateTables = append(tableCompare.CreateTables, localMap[v.(string)])
		} else { // 说明本地已删除
			tableCompare.DropTables = append(tableCompare.DropTables, v.(string))
		}
	}
	same := localSet.Intersect(remoteSet)
	for v := range same.Iter() {
		sCompare := localMap[v.(string)].Compare(remoteMap[v.(string)])
		tableCompare.Details = append(tableCompare.Details, sCompare)
	}
	return
}

// Compare TableStruct 两个内部细节比较
func (local TableStruct) Compare(remote TableStruct) (singleCompare ColumnCompare) {
	if local.TableName != remote.TableName {
		log.WithFields(log.Fields{
			"oneTableName":   local.TableName,
			"otherTableName": remote.TableName,
		}).Panic("not the same tableName, meaningless...")
	}
	singleCompare.TableName = local.TableName
	// 比较大块 column index primary key
	singleCompare.AddIndex, singleCompare.DropIndex = findDiff(local.Indexes, remote.Indexes)
	addArray, dropArray := findDiff(local.PrimaryKeys, remote.PrimaryKeys)
	if len(dropArray) != 0 { // 只要不一样就重新建，mysql 主键 drop 机制决定的
		singleCompare.AddPrimary = local.PrimaryKeys
		singleCompare.NeedDropPrimary = true
	}
	if len(addArray) != 0 {
		singleCompare.AddPrimary = local.PrimaryKeys
	}
	var addColumns []string
	addColumns, singleCompare.DropColumn = findDiff(local.Columns, remote.Columns)
	for _, column := range addColumns {
		singleCompare.AddColumn = append(singleCompare.AddColumn, ColumnDetail{
			Column: column, Type: local.ColumnTypes[column], Constraints: local.Constraints[column],
		})
	}
	// 相同的 column 来检查 type 和 constraint 有咩有变化
	lColumns := mapset.NewSet()
	rColumns := mapset.NewSet()
	for _, v := range local.Columns {
		lColumns.Add(v)
	}
	for _, v := range remote.Columns {
		rColumns.Add(v)
	}
	sameColumns := lColumns.Intersect(rColumns)
	for column := range sameColumns.Iter() {
		_add, _drop := findDiff(strings.Split(local.Constraints[column.(string)], " "),
			strings.Split(local.Constraints[column.(string)], " "))
		if local.ColumnTypes[column.(string)] != remote.ColumnTypes[column.(string)] ||
			len(_add) != 0 || len(_drop) != 0 {
			singleCompare.ColumnChanged = append(singleCompare.ColumnChanged, RecordCompare{
				Column: column.(string),
				Before: remote.ColumnTypes[column.(string)] + " " + remote.Constraints[column.(string)],
				Now:    local.ColumnTypes[column.(string)] + " " + local.Constraints[column.(string)],
				LocalColumn: ColumnDetail{
					Column: column.(string), Type: local.ColumnTypes[column.(string)],
					Constraints: local.Constraints[column.(string)],
				},
			})
		}
	}
	return
}

// findDiff 具体比较两个数组的差异
func findDiff(lStings, rStrings []string) (add, drop []string) {
	sort.Strings(lStings)
	sort.Strings(rStrings)
	lArray := mapset.NewSet()
	rArray := mapset.NewSet()
	for _, v := range lStings {
		lArray.Add(v)
	}
	for _, v := range rStrings {
		rArray.Add(v)
	}
	diffArray := lArray.Difference(rArray)
	_d := rArray.Difference(lArray)  // 做一次反向比较，必要。远端有无效字段包含了 本地，diffArray 为空
	for v := range _d.Iter() {
		diffArray.Add(v)
	}
	for v := range diffArray.Iter() {
		if lArray.Contains(v) {
			add = append(add, v.(string))
		} else {
			drop = append(drop, v.(string))
		}
	}
	return
}

// GenCreateTableSql 生成建表语句
func (local TableStruct) GenCreateTableSql(charset string) string {
	var sqlBuilder []string
	for _, column := range local.Columns {
		sqlBuilder = append(sqlBuilder, fmt.Sprintf("\t`%s` %s %s",
			column, local.ColumnTypes[column], local.Constraints[column]))
	}
	var primaryKeys []string
	for _, primaryKey := range local.PrimaryKeys {
		primaryKeys = append(primaryKeys, fmt.Sprintf("`%s`", primaryKey))
	}
	if len(primaryKeys) != 0 {
		sqlBuilder = append(sqlBuilder, fmt.Sprintf("\tPRIMARY KEY (%s)", strings.Join(primaryKeys, ",")))
	}
	for _, index := range local.Indexes {
		sqlBuilder = append(sqlBuilder, fmt.Sprintf("\tKEY `idx_%s_%s` (`%s`)",
			local.TableName, index, index))
	}
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (\n%s\n) ENGINE=InnoDB DEFAULT CHARSET=%s",
		local.TableName, strings.Join(sqlBuilder, ",\n"), charset)
}

// GenColumnSql remote 和 local 同一个表下的字段差异处理
// 先处理索引删除，在处理列删除，处理列增加，处理索引增加。严格按照这个顺序
func (columnCompare ColumnCompare) GenColumnSql() (sqlBuilder []string) {
	// 处理 删除索引
	for _, index := range columnCompare.DropIndex {
		sqlBuilder = append(sqlBuilder, fmt.Sprintf("ALTER TABLE %s DROP INDEX idx_%s_%s",
			columnCompare.TableName, columnCompare.TableName, index))
	}
	if len(columnCompare.AddPrimary) != 0 && columnCompare.NeedDropPrimary {  // 删除主键，这里mysql 只要主键有差异 就都drop了
		sqlBuilder = append(sqlBuilder, fmt.Sprintf("ALTER TABLE %s DROP PRIMARY KEY", columnCompare.TableName))
	}
	// 处理删除列
	for _, column := range columnCompare.DropColumn {
		sqlBuilder = append(sqlBuilder, fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s",
			columnCompare.TableName, column))
	}
	// 增加列
	for _, colDetail := range columnCompare.AddColumn {
		sql := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s %s",
			columnCompare.TableName, colDetail.Column, colDetail.Type, colDetail.Constraints)
		// Constraints 可能为空
		sqlBuilder = append(sqlBuilder, strings.TrimRight(sql, " "))
	}
	// 处理增加索引
	for _, primaryKey := range columnCompare.AddPrimary {
		sqlBuilder = append(sqlBuilder, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT PK_%s PRIMARY KEY (%s)",
			columnCompare.TableName, columnCompare.TableName, primaryKey))
	}
	for _, index := range columnCompare.AddIndex {
		sqlBuilder = append(sqlBuilder, fmt.Sprintf("ALTER TABLE %s ADD INDEX idx_%s_%s (`%s`)",
			columnCompare.TableName, columnCompare.TableName, index, index))
	}
	// 处理变化的，这里直接简单粗暴，变化的就直接删除了。测试环境和生产环境上是给出差异化提示，手动编写migrate 文件
	for _, columnRecord := range columnCompare.ColumnChanged {
		sqlBuilder = append(sqlBuilder, fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s",
			columnCompare.TableName, columnRecord.Column))
		sql := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s %s",
			columnCompare.TableName, columnRecord.Column, columnRecord.LocalColumn.Type, columnRecord.LocalColumn.Constraints)
		// Constraints 可能为空
		sqlBuilder = append(sqlBuilder, strings.TrimRight(sql, " "))
	}
	return
}
