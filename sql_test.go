package mdb

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/shopspring/decimal"
	log "github.com/sirupsen/logrus"
	"os"
	"testing"
	"time"
)


func init() {
	dbName := os.Getenv("DB_NAME")
	dbHose := os.Getenv("DB_HOST")
	username := os.Getenv("USERNAME")
	password := os.Getenv("PASSWORD")
	InitDB(Config{
		DbName: dbName, Host: dbHose,
		UserName: username,
		Password: password,
	})
}

func TestScope(t *testing.T)  {
	stu := &Student{}
	var stus []Student
	err := Model(stu).Select(stu.ID, stu.Name).Where().Map(&stus)
	if err != nil {
		t.Error(err)
	}
}

func TestSelectSql(t *testing.T)  {
	ma := &TestModelA{}
	mb := &TestModelB{}
	sqlBuilder := Model(ma, mb).
		Select(ma.ID, ma.OwnerID, ma.CreatedTime, mb.ID, mb.Status1).
		LeftJoin(mb, And(ma.ID.Eq(mb.OwnerID), Or(ma.ID.Less(1), mb.State.Greater(1)))).
		Where(mb.Status1.LessEq(4), ma.ID.Eq(1), mb.State.GreaterEq(1))
	t.Logf("%#v", sqlBuilder)

}

func TestSqlMap(t *testing.T)  {
	stu := &Student{}
	var stus []Student
	err := Model(stu).Select(stu.ID, stu.Name, stu.CreateTime, stu.Score).Map(&stus)
	if err != nil {
		t.Error(err)
	}
	resList := make([]map[string]interface{}, len(stus))
	for i, v := range stus {
		result := make(map[string]interface{})
		result["id"] = v.ID.Raw
		result["name"] = v.Name.Raw
		result["createTime"] = v.CreateTime.Raw
		result["score"] = v.Score.Raw
		resList[i] = result
	}
	t.Logf("%v", resList)
	//
	//
	//stus = []Student{}
	//var classes []Class
	//var schools []School
	//stu = &Student{}
	//class := &Class{}
	//school := &School{}
	//err = Model(stu, class, school).Select(stu.ID, stu.Name, class.Number, school.Title).
	//	InnerJoin(class, stu.ClassId.Eq(class.ID)).
	//	InnerJoin(school, class.SchoolId.Eq(school.ID)).
	//	Where(stu.State.Greater(0), stu.Name.Eq("厚林"), school.Title.Eq("沈阳航空航天大学")).
	//	Map(&stus, &classes, &schools)
	//if err != nil {
	//	t.Error(err)
	//}
	//result = make(map[string]interface{}, len(stus))
	//for i := 0; i < len(stus); i++ {
	//	result["id"] = stus[i].ID.V
	//	result["stuName"] = stus[i].Name.V
	//	result["classNumber"] = classes[i].Number.V
	//	result["schoolTitle"] = schools[i].Title.V
	//}
	//t.Log(result)
}

func TestSqlInsert(t *testing.T)  {
	err := Model(&Student{
		ID:    Varchar{V: "112"},
		ClassId: Varchar{V: "222"},
		Name: Varchar{V: "振兴"},
		Score: Decimal{V: decimal.NewFromFloat(3.1415926)},
		CreateTime: Datetime{V: time.Now()},
		State: Bool{V: false, NotNul: true},
	}).Insert()

	//err = Insert(&School{
	//	ID:    Varchar{V: "1"},
	//	Title: Varchar{V: "沈阳航空航天大学"},
	//	State: Bool{V: false, NotNul: true},
	//})
	//err = Insert(&School{
	//	ID:    Varchar{V: "2"},
	//	Title: Varchar{V: "沈阳师范大学"},
	//})
	//err = Insert(&Class{
	//	ID:    Varchar{V: "11"},
	//	SchoolId: Varchar{V: "1"},
	//	Number: Smallint{V: 888},
	//})
	//err = Insert(&Class{
	//	ID:    Varchar{V: "22"},
	//	SchoolId: Varchar{V: "2"},
	//	Number: Smallint{V: 999},
	//})
	//
	//err = Insert(&Student{
	//	ID:    Varchar{V: "111"},
	//	Name: Varchar{V: "厚林"},
	//	ClassId: Varchar{V: "11"},
	//})
	//err = Insert(&Student{
	//	ID:    Varchar{V: "222"},
	//	Name: Varchar{V: "娟"},
	//	ClassId: Varchar{V: "22"},
	//})
	if err != nil {
		t.Error(err)
	}
	//
	//aa := &Student{
	//	Name: Varchar{V: "娟"},
	//	ClassId: Varchar{V: "22"},
	//}
	//err = Update(aa).Where(aa.ID.Eq("1223"))

}

func TestSqlUpdate(t *testing.T)  {
	aa := &School{
		Title: Varchar{V: "辽宁工程技术大学12334567"},
		State: Bool{V: false, NotNul: true},
	}
	err := Model(aa).Where(aa.ID.Eq("1")).Update()
	if err != nil {
		return
	}

}


func TestSqlDelete(t *testing.T)  {
	aa := &School{}
	err := Model(aa).Where(aa.ID.Eq("111111112")).Delete()
	if err != nil {
		log.Error("delete err:", err)
		return
	}

}
