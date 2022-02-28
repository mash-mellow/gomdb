package mdb

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

//var _scannerInterface = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

// 将db查询结构返回，被赋值给结构体或结构体数组
func scanAll(sqlBuilder *SqlBuilder, dests ...interface{}) error {
	// 判断 baseStruct 是否包含返回的字段信息，如果没有报错；调用方要很清楚自己想要什么数据，从而节省资源
	err, destCatch := checkDest(sqlBuilder.SelectFields, dests...)
	if err != nil {
		return err
	}
	// 执行sql 语句
	var rows *sql.Rows
	rows, err = db.Query(sqlBuilder.SqlStmt, sqlBuilder.Values...)
	if err != nil {
		return err
	}
	defer func() {
		_ = rows.Close()
	}()
	// 解析rows
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	values := make([]interface{}, len(columns)) // 和 fieldset 一致
	for rows.Next() {
		sliceMap := locateScanValues(destCatch, values, sqlBuilder.SelectFields)
		err = rows.Scan(values...)
		if err != nil {
			return err
		}
		for slice, obj := range sliceMap {
			slice.Set(reflect.Append(slice, obj))
		}
	}
	return err
}

// locateScanValues 获取scan 的参数地址
func locateScanValues(destCatch map[string]destCell, values []interface{},
	fields []selectField) map[reflect.Value]reflect.Value {
	instMap := make(map[string]reflect.Value)
	sliceMap := make(map[reflect.Value]reflect.Value)
	for tableName, cell := range destCatch {
		vp := reflect.New(cell.baseStruct)
		v := reflect.Indirect(vp)
		instMap[tableName] = v
		sliceMap[cell.slice] = v
	}
	// 这里存在不同的表，将上面对应的 obj 缓存起来了。一个row.next 只有一组obj生成
	for i, field := range fields {
		obj := instMap[field.tableName]
		// 这里不用校验，checkDest 开始就校验了
		attr := obj.FieldByName(field.orgColumnName)
		//attr = attr.FieldByName("V")
		alloc := reflect.New(Deref(attr.Type()))
		alloc = reflect.Indirect(alloc)
		attr.Set(alloc)
		values[i] = attr.Addr().Interface()
	}
	return sliceMap
}

type destCell struct {
	slice      reflect.Value
	baseStruct reflect.Type
}

// checkDest 检查SQLBuilder中的field 和 dest是否一致；他们是包含关系
// 获取dest必须使用变量缓存
func checkDest(fields []selectField, dests ...interface{}) (error, map[string]destCell) {
	destCatch := make(map[string]destCell)
	for i, dest := range dests {
		var cell destCell
		value := reflect.ValueOf(dest)
		// json.Unmarshal returns errors for these
		if value.Kind() != reflect.Ptr {
			return fmt.Errorf("%T must pass a pointer, not a value, to StructScan destination", dest), nil
		}
		if value.IsNil() {
			return fmt.Errorf("index-%d is nil pointer", i), nil
		}
		_slice, err := baseType(value.Type(), reflect.Slice)
		if err != nil {
			return err, nil
		}
		cell.baseStruct = Deref(_slice.Elem())
		if cell.baseStruct.Kind() != reflect.Struct {
			return errors.New("must be a struct"), nil
		}
		// 这就是返回的slice
		cell.slice = reflect.Indirect(value)
		_array := strings.Split(cell.baseStruct.String(), ".")
		tableName := UnMarshal4Camel(_array[len(_array)-1])
		destCatch[tableName] = cell
	}
	// 解析dest，判断是否包含对应的field字段
	for _, field := range fields {
		baseStruct := destCatch[field.tableName].baseStruct
		if _, found := baseStruct.FieldByName(Marshal2Camel(field.columnName)); !found {
			return fmt.Errorf("dest %T do not has the feild %s", baseStruct, field.columnName), nil
		}
	}
	return nil, destCatch
}

// Deref is Indirect for reflect.Types
func Deref(t reflect.Type) reflect.Type {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t
}

// baseType 基本类型
func baseType(t reflect.Type, expected reflect.Kind) (reflect.Type, error) {
	t = Deref(t)
	if t.Kind() != expected {
		return nil, fmt.Errorf("expected %s but got %s", expected, t.Kind())
	}
	return t, nil
}
