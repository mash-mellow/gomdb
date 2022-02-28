package mdb

import (
	"fmt"
	"github.com/shopspring/decimal"
	"strconv"
	"time"
)

const (
	_ = iota
	OpEq
	OpGreater
	OpLess
	OpGreaterEq
	OpLessEq
	OpIn
	OpNotIn
	OpGroupAnd
	OpGroupOr
)

type Opt struct {
	// 就是具体表的struct 引用，在db.Model时候赋值，承载所有的Mark操作
	tableName     string
	dbColumnName  string
	orgColumnName string // 原始struct的表名，首字母大写
}

type Varchar struct {
	V      string
	NotNul bool
	Raw    interface{}  // 针对 select-map 有效。不用重复判读 NotNul；Insert Update 忽略
	Opt
}

func (dbVarchar *Varchar) Scan(src interface{}) error {
	if src == nil {
		dbVarchar.V = ""
		dbVarchar.NotNul = false
		dbVarchar.Raw = nil
		return nil
	}
	b, _ := src.([]byte)
	valStr := string(b)
	dbVarchar.NotNul = true
	dbVarchar.V = valStr
	dbVarchar.Raw = dbVarchar.V
	return nil
}

type Text struct {
	V      string
	NotNul bool
	Raw    interface{}   // 针对 select-map 有效。不用重复判读 NotNul；Insert Update 忽略
	Opt
}

func (dbText *Text) Scan(src interface{}) error {
	if src == nil {
		dbText.V = ""
		dbText.NotNul = false
		dbText.Raw = nil
		return nil
	}
	b, _ := src.([]byte)
	valStr := string(b)
	dbText.NotNul = true
	dbText.V = valStr
	dbText.Raw = dbText.V
	return nil
}

type Blob struct {
	V      []byte
	NotNul bool
	Raw    interface{}   // 针对 select-map 有效。不用重复判读 NotNul；Insert Update 忽略
	Opt
}

func (dbBlob *Blob) Scan(src interface{}) error {
	if src == nil {
		dbBlob.V = nil
		dbBlob.NotNul = false
		dbBlob.Raw = nil
		return nil
	}
	b, _ := src.([]byte)
	dbBlob.NotNul = true
	dbBlob.V = b
	dbBlob.Raw = dbBlob.V
	return nil
}

type Tinyint struct {
	V      int8
	NotNul bool
	Raw    interface{}  // 针对 select-map 有效。不用重复判读 NotNul；Insert Update 忽略
	Opt
}

func (dbTinyint *Tinyint) Scan(src interface{}) error {
	if src == nil {
		dbTinyint.V = 0
		dbTinyint.NotNul = false
		dbTinyint.Raw = nil
		return nil
	}
	b, _ := src.([]byte)
	valStr := string(b)
	dbTinyint.NotNul = true
	var v int
	v, err := strconv.Atoi(valStr)
	if err != nil {
		return err
	}
	dbTinyint.V = int8(v)
	dbTinyint.Raw = dbTinyint.V
	return nil
}

type Smallint struct {
	V      int16
	NotNul bool
	Raw    interface{}  // 针对 select-map 有效。不用重复判读 NotNul；Insert Update 忽略
	Opt
}

func (dbSmallint *Smallint) Scan(src interface{}) error {
	if src == nil {
		dbSmallint.V = 0
		dbSmallint.NotNul = false
		dbSmallint.Raw = nil
		return nil
	}
	b, _ := src.([]byte)
	valStr := string(b)
	dbSmallint.NotNul = true
	var v int
	v, err := strconv.Atoi(valStr)
	if err != nil {
		return err
	}
	dbSmallint.V = int16(v)
	dbSmallint.Raw = nil
	return nil
}

type Int struct {
	V      int32
	NotNul bool
	Raw    interface{}  // 针对 select-map 有效。不用重复判读 NotNul；Insert Update 忽略
	Opt
}

func (dbInt *Int) Scan(src interface{}) error {
	if src == nil {
		dbInt.V = 0
		dbInt.NotNul = false
		dbInt.Raw = nil
		return nil
	}
	b, _ := src.([]byte)
	valStr := string(b)
	dbInt.NotNul = true
	var v int
	v, err := strconv.Atoi(valStr)
	if err != nil {
		return err
	}
	dbInt.V = int32(v)
	dbInt.Raw = dbInt.V
	return nil
}

type Bigint struct {
	V      int64
	NotNul bool
	Raw    interface{}  // 针对 select-map 有效。不用重复判读 NotNul；Insert Update 忽略
	Opt
}

func (dbBigint *Bigint) Scan(src interface{}) error {
	if src == nil {
		dbBigint.V = 0
		dbBigint.NotNul = false
		dbBigint.Raw = nil
		return nil
	}
	b, _ := src.([]byte)
	valStr := string(b)
	dbBigint.NotNul = true
	var v int64
	v, err := strconv.ParseInt(valStr, 10, 64)
	if err != nil {
		return err
	}
	dbBigint.V = v
	dbBigint.Raw = dbBigint.V
	return nil
}

type Float struct {
	V      float32
	NotNul bool
	Raw    interface{}  // 针对 select-map 有效。不用重复判读 NotNul；Insert Update 忽略
	Opt
}

func (dbFloat *Float) Scan(src interface{}) error {
	if src == nil {
		dbFloat.V = 0
		dbFloat.NotNul = false
		dbFloat.Raw = nil
		return nil
	}
	b, _ := src.([]byte)
	valStr := string(b)
	dbFloat.NotNul = true
	var v float64
	v, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return err
	}
	dbFloat.V = float32(v)
	dbFloat.Raw = dbFloat.V
	return nil
}

type Double struct {
	V      float64
	NotNul bool
	Raw    interface{}  // 针对 select-map 有效。不用重复判读 NotNul；Insert Update 忽略
	Opt
}

func (dbDouble *Double) Scan(src interface{}) error {
	if src == nil {
		dbDouble.V = 0
		dbDouble.NotNul = false
		dbDouble.Raw = nil
		return nil
	}
	b, _ := src.([]byte)
	valStr := string(b)
	dbDouble.NotNul = true
	var v float64
	v, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return err
	}
	dbDouble.V = v
	dbDouble.Raw = dbDouble.V
	return nil
}

type Decimal struct {
	V      decimal.Decimal
	NotNul bool
	Raw    interface{}  // 针对 select-map 有效。不用重复判读 NotNul；Insert Update 忽略
	Opt
}

func (dbDecimal *Decimal) Scan(src interface{}) error {
	if src == nil {
		dbDecimal.V = decimal.Zero
		dbDecimal.NotNul = false
		dbDecimal.Raw = nil
		return nil
	}
	b, _ := src.([]byte)
	valStr := string(b)
	dbDecimal.NotNul = true
	dbDecimal.V = decimal.RequireFromString(valStr)
	dbDecimal.Raw = valStr
	return nil
}

type Datetime struct {
	V      time.Time
	NotNul bool
	Raw    interface{}   // 针对 select-map 有效。不用重复判读 NotNul；Insert Update 忽略
	Opt
}

func (dbDatetime *Datetime) Scan(src interface{}) error {
	if src == nil {
		dbDatetime.NotNul = false
		dbDatetime.Raw = nil
		return nil
	}
	dbDatetime.NotNul = true
	dbDatetime.V = src.(time.Time)
	dbDatetime.Raw = dbDatetime.V
	return nil
}

type Bool struct {
	V      bool
	NotNul bool
	Raw    interface{}  // 针对 select-map 有效。不用重复判读 NotNul；Insert Update 忽略
	Opt
}

func (dbBool *Bool) Scan(src interface{}) error {
	if src == nil {
		dbBool.V = false
		dbBool.NotNul = false
		dbBool.Raw = nil
		return nil
	}
	b, _ := src.([]byte)
	valStr := string(b)
	dbBool.NotNul = true
	var v int
	v, err := strconv.Atoi(valStr)
	if err != nil {
		return err
	}
	dbBool.V = !(v == 0)
	dbBool.Raw = dbBool.V
	return nil
}

// Term On 或者 where 每个条件  中间过渡
type Term struct {
	// 一个and 里边有多个or 或者and，这时候会将他们分别缓存到 CatchTerms 中
	CatchTerms []Term
	One        string
	Op         int8
	GroupOp    int8
	Other      string
	Value      interface{}
}

func (o Opt) Eq(v interface{}) Term {
	return op(o, OpEq, v)
}

func (o Opt) Greater(v interface{}) Term {
	return op(o, OpGreater, v)
}

func (o Opt) GreaterEq(v interface{}) Term {
	return op(o, OpGreaterEq, v)
}

func (o Opt) Less(v interface{}) Term {
	return op(o, OpLess, v)
}

func (o Opt) LessEq(v interface{}) Term {
	return op(o, OpLessEq, v)
}

//func (o Opt) Between(start, end interface{}) Term {
//	return op(o, OpLessEq, v)
//}

//func (o Opt) In(vs ...interface{}) Term {
//	return op(o, OpIn, v)
//}
//
//func (o Opt) NotIn(vs ...interface{}) Term {
//	o.Chs = true
//	return o
//}

func op(o Opt, opFlag int8, value interface{}) (term Term) {
	term.One = fmt.Sprintf("`%s`.%s", o.tableName, o.dbColumnName)
	term.Op = opFlag
	if opt, ok := value.(Varchar); ok {
		term.Other = fmt.Sprintf("`%s`.%s", opt.tableName, opt.dbColumnName)
	} else if opt, ok := value.(Text); ok {
		term.Other = fmt.Sprintf("`%s`.%s", opt.tableName, opt.dbColumnName)
	} else if opt, ok := value.(Blob); ok {
		term.Other = fmt.Sprintf("`%s`.%s", opt.tableName, opt.dbColumnName)
	} else if opt, ok := value.(Tinyint); ok {
		term.Other = fmt.Sprintf("`%s`.%s", opt.tableName, opt.dbColumnName)
	} else if opt, ok := value.(Smallint); ok {
		term.Other = fmt.Sprintf("`%s`.%s", opt.tableName, opt.dbColumnName)
	} else if opt, ok := value.(Int); ok {
		term.Other = fmt.Sprintf("`%s`.%s", opt.tableName, opt.dbColumnName)
	} else if opt, ok := value.(Bigint); ok {
		term.Other = fmt.Sprintf("`%s`.%s", opt.tableName, opt.dbColumnName)
	} else if opt, ok := value.(Float); ok {
		term.Other = fmt.Sprintf("`%s`.%s", opt.tableName, opt.dbColumnName)
	} else if opt, ok := value.(Double); ok {
		term.Other = fmt.Sprintf("`%s`.%s", opt.tableName, opt.dbColumnName)
	} else if opt, ok := value.(Decimal); ok {
		term.Other = fmt.Sprintf("`%s`.%s", opt.tableName, opt.dbColumnName)
	} else if opt, ok := value.(Datetime); ok {
		term.Other = fmt.Sprintf("`%s`.%s", opt.tableName, opt.dbColumnName)
	} else if opt, ok := value.(Bool); ok {
		term.Other = fmt.Sprintf("`%s`.%s", opt.tableName, opt.dbColumnName)
	} else {
		term.Other = "?"
		term.Value = value
	}
	return
}
