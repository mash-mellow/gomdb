package mdb

import (
	"fmt"
	"github.com/shopspring/decimal"
	log "github.com/sirupsen/logrus"
	"reflect"
	"strings"
)


type SqlBuilder struct {
	Type int8 //类型 增删改查
	MainTable string
	// 引用 + 表名
	Models       map[interface{}]string
	SelectFields []selectField // 解析rows使用
	// left join table : on xxx and yyy
	JoinOns   []joinOnCell // 可以有多个
	whereCons string       // 可以有多个
	// insert
	InsertFields []insertField
	SqlStmt string  // 最后执行的sql语句
	Values []interface{}  // 替换sql 语句中的？ 防止sql注入
}

type joinOnCell struct {
	Join string
	On string
}

type selectField struct {
	tableName string
	columnName string
	orgColumnName string
}

type insertField struct {
	columnName string
	value interface{}
}

type JoinMode string
const Left JoinMode = "LEFT"
const Right JoinMode = "RIGHT"
const Inner JoinMode = "INNER"

// Model 规定model 的范围
func Model(models ...interface{}) *SqlBuilder {
	modelsMap := make(map[interface{}]string)
	var sqlBuilder SqlBuilder
	for _, model := range models {
		tableName, insertFields := dealModel(model)
		modelsMap[model] = tableName
		if len(insertFields) != 0 {  // insert 的时候只会有 一个 model
			sqlBuilder.InsertFields = insertFields
			break
		}
	}
	sqlBuilder.Models = modelsMap
	return &sqlBuilder
}

// dealModel 初始化值 dbVarchar 等等的 初始值
func dealModel(obj interface{}) (tableName string, insertFields []insertField) {
	refType := reflect.TypeOf(obj).Elem()
	refValue := reflect.ValueOf(obj).Elem()
	_array := strings.Split(refType.String(), ".")
	tableName = UnMarshal4Camel(_array[len(_array)-1])
	if refValue.Kind() == reflect.Struct {
		// 结构体字段获取
		for i := 0; i < refType.NumField(); i++ {
			f := refType.Field(i)
			if f.Name[0] < "A"[0] || f.Name[0] > "Z"[0]  {
				continue
			}
			//fmt.Printf("name=%s , type=%v , value=%v \n", f.Name, f.Type, refValue.Field(i).Interface())
			_columnName := refType.Field(i).Name
			columnName := UnMarshal4Camel(_columnName)
			dbVar, value := setOptValue(refValue.Field(i).Interface(), tableName, columnName, _columnName)
			if dbVar != nil {
				refValue.FieldByName(refType.Field(i).Name).Set(reflect.ValueOf(dbVar))
			}
			if value != nil {  // 用于 insert 更新取值
				insertFields = append(insertFields, insertField{
					columnName: columnName,
					value:      value,
				})
			}
		}
	}
	return
}

// setOptValue 核心方法，给结构体赋值 tableName columnName，方便后期直接使用
func setOptValue(value interface{}, tableName,
	dbColumnName, orgColumnName string) (interface{}, interface{}) {
	var v interface{}
	if varchar, ok := value.(Varchar); ok {
		varchar.tableName =  tableName
		varchar.dbColumnName = dbColumnName
		varchar.orgColumnName = orgColumnName
		v = varchar.V
		if varchar.V == "" && !varchar.NotNul {
			v = nil
		} else {
			v = fmt.Sprintf("%s", v)
		}
		return varchar, v
	} else if text, ok := value.(Text); ok {
		text.tableName =  tableName
		text.dbColumnName = dbColumnName
		text.orgColumnName = orgColumnName
		v = text.V
		if text.V == "" && !text.NotNul {
			v = nil
		} else {
			v = fmt.Sprintf("%s", v)
		}
		return text, v
	} else if blob, ok := value.(Blob); ok {
		blob.tableName =  tableName
		blob.dbColumnName = dbColumnName
		blob.orgColumnName = orgColumnName
		v = blob.V
		if len(blob.V) == 0 && !blob.NotNul {
			v = nil
		}
		return blob, v
	} else if tinyint, ok := value.(Tinyint); ok {
		tinyint.tableName =  tableName
		tinyint.dbColumnName = dbColumnName
		tinyint.orgColumnName = orgColumnName
		v = tinyint.V
		if tinyint.V == 0 && !tinyint.NotNul {
			v = nil
		}
		return tinyint, v
	} else if smallint, ok := value.(Smallint); ok {
		smallint.tableName =  tableName
		smallint.dbColumnName = dbColumnName
		smallint.orgColumnName = orgColumnName
		v = smallint.V
		if smallint.V == 0 && !smallint.NotNul {
			v = nil
		}
		return smallint, v
	} else if integer, ok := value.(Int); ok {
		integer.tableName =  tableName
		integer.dbColumnName = dbColumnName
		integer.orgColumnName = orgColumnName
		v = integer.V
		if integer.V == 0 && !integer.NotNul {
			v = nil
		}
		return integer, nil
	} else if bigint, ok := value.(Bigint); ok {
		bigint.tableName =  tableName
		bigint.dbColumnName = dbColumnName
		bigint.orgColumnName = orgColumnName
		v = bigint.V
		if bigint.V == 0 && !bigint.NotNul {
			v = nil
		}
		return bigint, v
	} else if float, ok := value.(Float); ok {
		float.tableName =  tableName
		float.dbColumnName = dbColumnName
		float.orgColumnName = orgColumnName
		v = float.V
		if float.V == 0 && !float.NotNul {
			v = nil
		}
		return float, v
	} else if double, ok := value.(Double); ok {
		double.tableName =  tableName
		double.dbColumnName = dbColumnName
		double.orgColumnName = orgColumnName
		v = double.V
		if double.V == 0 && !double.NotNul {
			v = nil
		}
		return double, v
	} else if deci, ok := value.(Decimal); ok {
		deci.tableName =  tableName
		deci.dbColumnName = dbColumnName
		deci.orgColumnName = orgColumnName
		v = deci.V
		if deci.V.Equal(decimal.Zero) && !deci.NotNul {
			v = nil
		}
		return deci, v
	} else if datetime, ok := value.(Datetime); ok {
		datetime.tableName =  tableName
		datetime.dbColumnName = dbColumnName
		datetime.orgColumnName = orgColumnName
		v = datetime.V
		if datetime.V.IsZero() && !deci.NotNul {
			v = nil
		}
		return datetime, v
	} else if _bool, ok := value.(Bool); ok {
		_bool.tableName =  tableName
		_bool.dbColumnName = dbColumnName
		_bool.orgColumnName = orgColumnName
		v = _bool.V
		if !_bool.V && !_bool.NotNul {
			v = nil
		}
		return _bool, v
	}
	return nil, v
}

func getOpt(value interface{}) *Opt {
	if varchar, ok := value.(Varchar); ok {
		return &varchar.Opt
	} else if text, ok := value.(Text); ok {
		return &text.Opt
	} else if blob, ok := value.(Blob); ok {
		return &blob.Opt
	} else if tinyint, ok := value.(Tinyint); ok {
		return &tinyint.Opt
	} else if smallint, ok := value.(Smallint); ok {
		return &smallint.Opt
	} else if integer, ok := value.(Int); ok {
		return &integer.Opt
	} else if bigint, ok := value.(Bigint); ok {
		return &bigint.Opt
	} else if float, ok := value.(Float); ok {
		return &float.Opt
	} else if double, ok := value.(Double); ok {
		return &double.Opt
	} else if deci, ok := value.(Decimal); ok {
		return &deci.Opt
	} else if datetime, ok := value.(Datetime); ok {
		return &datetime.Opt
	} else if _bool, ok := value.(Bool); ok {
		return &_bool.Opt
	}
	return nil
}

func (sqlBuilder *SqlBuilder) Select(opts ...interface{}) *SqlBuilder {
	tableName, fields := selectMarkInfo(opts...)
	sqlBuilder.MainTable = tableName
	sqlBuilder.SelectFields = fields
	return sqlBuilder
}

func (sqlBuilder *SqlBuilder) Insert() error {
	if len(sqlBuilder.Models) != 1 {
		log.Panic("Insert option has one table a time!")
	}
	var tableName string
	//var model interface{}
	for _, tableName = range sqlBuilder.Models {}
	sqlBuilder.MainTable = tableName
	parseInsertSql(sqlBuilder)
	log.Info(sqlBuilder.SqlStmt, sqlBuilder.Values)
	_, err := db.Exec(sqlBuilder.SqlStmt, sqlBuilder.Values...)
	if err != nil {
		return err
	}
	return nil
}


func (sqlBuilder *SqlBuilder) Update() error {
	if len(sqlBuilder.Models) != 1 {
		log.Panic("update option has one table a time!")
	}
	var tableName string
	for _, tableName = range sqlBuilder.Models {}
	sqlBuilder.MainTable = tableName
	parseUpdateSql(sqlBuilder)
	_, err := db.Exec(sqlBuilder.SqlStmt, sqlBuilder.Values...)
	if err != nil {
		return err
	}
	return nil
}


func (sqlBuilder *SqlBuilder) Delete() error {
	if len(sqlBuilder.Models) != 1 {
		log.Panic("update option has one table a time!")
	}
	var tableName string
	for _, tableName = range sqlBuilder.Models {}
	sqlBuilder.MainTable = tableName
	parseDeleteSql(sqlBuilder)
	_, err := db.Exec(sqlBuilder.SqlStmt, sqlBuilder.Values...)
	if err != nil {
		return err
	}
	return nil
}


func (sqlBuilder *SqlBuilder) LeftJoin(model interface{}, onTerms ...Term) *SqlBuilder {
	return sqlBuilder.join(Left, model, onTerms...)
}

func (sqlBuilder *SqlBuilder) InnerJoin(model interface{}, onTerms ...Term) *SqlBuilder {
	return sqlBuilder.join(Inner, model, onTerms...)
}

func (sqlBuilder *SqlBuilder) RightJoin(model interface{}, onTerms ...Term) *SqlBuilder {
	return sqlBuilder.join(Right, model, onTerms...)
}

// Where cond 只有两个可能，string []string
func (sqlBuilder *SqlBuilder) Where(terms ...Term) *SqlBuilder {
	sqlCons, values := assemble(terms...)
	sqlBuilder.whereCons = strings.Join(sqlCons, " ")
	for _, value := range values{
		sqlBuilder.Values = append(sqlBuilder.Values, value)
	}
	sqlBuilder.SqlStmt = fmt.Sprintf("%s where %s", sqlBuilder.SqlStmt, sqlBuilder.whereCons)
	return sqlBuilder
}

// Map 将sql返回的row，dest 是一个结构体或者结构体的数组
func (sqlBuilder *SqlBuilder) Map(dests ...interface{}) error {
	// 组装sql 语句
	parseSelectSql(sqlBuilder)
	log.Info(sqlBuilder.SqlStmt, sqlBuilder.Values)
	err := scanAll(sqlBuilder, dests...)
	if err != nil {
		return err
	}
	return nil
}

func And(terms ...Term) (term Term) {
	for _, _term := range terms {
		_term.GroupOp = OpGroupAnd
		term.CatchTerms = append(term.CatchTerms, _term)
	}
	return
}

func Or(terms ...Term) (term Term) {
	for _, _term := range terms {
		_term.GroupOp = OpGroupOr
		term.CatchTerms = append(term.CatchTerms, _term)
	}
	return
}

// join 核心方法
func (sqlBuilder *SqlBuilder) join(mode JoinMode, model interface{}, onTerms ...Term) *SqlBuilder {
	tableName := sqlBuilder.Models[model]
	var aJoinOnCell joinOnCell
	aJoinOnCell.Join = fmt.Sprintf(" %s JOIN %s ", mode, tableName)
	// 组装On 条件
	sqlCons, values := assemble(onTerms...)
	sqlBuilder.Values = values
	aJoinOnCell.On = strings.Join(sqlCons, " ")
	sqlBuilder.JoinOns = append(sqlBuilder.JoinOns, aJoinOnCell)
	return sqlBuilder
}

// translate 核心方法，翻译sql 逻辑语句
func (term Term) translate(_condStr string, _v []interface{},
	isStart, isEnd bool) (condStr string, v []interface{}) {
	if isStart {
		condStr = _condStr + "(" + makeOneTerm(term, false)
	} else if isEnd {
		condStr = _condStr + makeOneTerm(term, true) + ")"
	} else {
		condStr = _condStr + makeOneTerm(term, false)
	}
	if term.Value != nil {
		v = append(_v, term.Value)
	}
	for i, t := range term.CatchTerms {
		if i == 0 && t.CatchTerms == nil {
			condStr, v = t.translate(condStr, v, true, false)
		} else if i == len(term.CatchTerms) - 1 && t.CatchTerms == nil {
			condStr, v = t.translate(condStr, v, false, true)
			condStr += ")"
		} else {
			condStr, v = t.translate(condStr, v, false, false)
		}
	}
	return
}

// selectMarkInfo 取第一个table 为主表select
func selectMarkInfo(dbVs ...interface{}) (string, []selectField) {
	var mainTable string
	fields := make([]selectField, len(dbVs))
	for i, dbv := range dbVs {
		opt := getOpt(dbv)
		if mainTable == "" {
			mainTable = opt.tableName
		}
		fields[i] = selectField{tableName: opt.tableName, columnName: opt.dbColumnName,
			orgColumnName: opt.orgColumnName}
	}
	return mainTable, fields
}

// assemble  组装where 和 join on 逻辑条件
func assemble(terms ...Term) (sqlCons []string, values []interface{}) {
	for i, _term := range terms {
		_sql, _value := _term.translate("", nil, false, false)
		if i == len(terms) - 1 {
			if strings.HasSuffix(_sql, "and ") {
				_sql = _sql[:len(_sql) - 5]
			} else if strings.HasSuffix(_sql, "or ") {
				_sql = _sql[:len(_sql) - 4]
			}
		}
		sqlCons = append(sqlCons, _sql)
		for _, _v := range _value {
			values = append(values, _v)
		}
	}
	return
}

// makeOneTerm 根据 term.GroupOp 返回类似  and xx >= yy
func makeOneTerm(term Term, isEnd bool) (condStr string) {
	var opStr string
	switch term.Op {
	case OpEq:
		opStr = "="
	case OpGreater:
		opStr = ">"
	case OpGreaterEq:
		opStr = ">="
	case OpLess:
		opStr = "<"
	case OpLessEq:
		opStr = "<="
	case OpIn:
		opStr = "in"
	case OpNotIn:
		opStr = "not in"
	default:
		return ""
	}
	condStr = fmt.Sprintf("%s %s %s", term.One, opStr, term.Other)
	if !isEnd {
		condStr += getGroupOpStr(term.GroupOp)
	}
	return
}

func getGroupOpStr(op int8) (groupOp string) {
	if op == OpGroupAnd {
		groupOp = " and "
	} else if op == OpGroupOr {
		groupOp = " or "
	} else {  // 默认是 and
		groupOp = " and "
	}
	return
}

// parseSelectSql 通过sqlBuilder的元素组装 select sql 语句
func parseSelectSql(sqlBuilder *SqlBuilder) {
	var selectFields []string
	for _, _field := range sqlBuilder.SelectFields {
		selectFields = append(selectFields, fmt.Sprintf("%s.%s As %s_%s",
			_field.tableName, _field.columnName, _field.tableName, _field.columnName))
	}
	sqlStmt := fmt.Sprintf("SELECT %s FROM %s ", strings.Join(selectFields, ", "), sqlBuilder.MainTable)
	for _, joinOn := range sqlBuilder.JoinOns {
		joinSql := fmt.Sprintf("%s On %s ", joinOn.Join, joinOn.On)
		sqlStmt += joinSql
	}
	if sqlBuilder.whereCons != "" {
		sqlStmt += " Where " + sqlBuilder.whereCons
	}
	sqlBuilder.SqlStmt = sqlStmt
}

// parseInsertSql 通过sqlBuilder的元素组装 insert sql 语句
func parseInsertSql(sqlBuilder *SqlBuilder) {
	columns := make([]string, len(sqlBuilder.InsertFields))
	signs := make([]string, len(sqlBuilder.InsertFields))
	sqlBuilder.Values = make([]interface{}, len(sqlBuilder.InsertFields))
	for i, field := range sqlBuilder.InsertFields {
		columns[i] = field.columnName
		sqlBuilder.Values[i] = field.value
		signs[i] = "?"
	}
	sqlStmt := fmt.Sprintf("INSERT INTO %s(%s) VALUES(%s)",
		sqlBuilder.MainTable, strings.Join(columns, ","), strings.Join(signs, ","))
	sqlBuilder.SqlStmt = sqlStmt
}

func parseUpdateSql(sqlBuilder *SqlBuilder) {
	signs := make([]string, len(sqlBuilder.InsertFields))
	insertValues := make([]interface{}, len(sqlBuilder.InsertFields))
	for i, field := range sqlBuilder.InsertFields {
		insertValues[i] = field.value
		signs[i] = fmt.Sprintf("%s.%s=?", sqlBuilder.MainTable,  field.columnName)

	}
	for _, value := range sqlBuilder.Values{
		insertValues = append(insertValues, value)
	}
	sqlBuilder.Values = insertValues
	sql := fmt.Sprintf("UPDATE %s SET %s", sqlBuilder.MainTable, strings.Join(signs, ","))
	sqlBuilder.SqlStmt = sql + sqlBuilder.SqlStmt

}

func parseDeleteSql(sqlBuilder *SqlBuilder) {
	sql :=  fmt.Sprintf("DELETE FROM %s", sqlBuilder.MainTable)
	sqlBuilder.SqlStmt = sql + sqlBuilder.SqlStmt
}