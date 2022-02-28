package mdb

type School struct {
	ID    Varchar `mdb:"length:45 primary key"`
	Title Varchar `mdb:"length:50"`
	State Bool    `mdb:"index default null"`
}

type Class struct {
	ID       Varchar `mdb:"length:45 primary key"`
	SchoolId Varchar `mdb:"length:45 not null"`
	Number   Smallint
	State    Bool `mdb:"index default 1"`
}

type Student struct {
	ID         Varchar `mdb:"length:45 primary key"`
	Name       Varchar `mdb:"length:50"`
	ClassId    Varchar `mdb:"length:45 not null"`
	Score      Decimal `mdb:"length:10_2"`
	CreateTime Datetime
	State      Bool `mdb:"index default 1"`
}

type TestModelA struct {
	ID        Varchar  `mgp:"length:45 primary key"`
	OwnerID   Varchar  `mgp:"index length:45"`
	Status    Smallint `mgp:"index default 1 not null"`
	dbInteger Smallint
	// 统计值-用户人数
	dbBigint Bigint
	// 统计值-该Group 下 Team数量
	dbFloat Float
	// 统计值-该Group 下 Task数量
	dbDouble Double
	// 统计值-该Group 日更新任务数量
	dbDecimal   Decimal `mgp:"length:10_2"`
	dbText      Text
	dbBlob      Blob
	CreatedTime Datetime
	// 禁用组织，当组织禁用后，无法进行创建修改删除等操作，可查看
	State Bool `mgp:"index default 1"`
}

type TestModelB struct {
	ID         Varchar  `mgp:"length:45 "`
	OwnerID    Varchar  `mgp:"index length:45 primary key"`
	Status1    Smallint `mgp:"index default 1 not null"`
	DbTinyint  Tinyint
	DbInteger1 Int
	// 统计值-用户人数
	DbBigint1 Bigint
	// 统计值-该Group 下 Team数量
	DbFloat Float
	// 统计值-该Group 下 Task数量
	DbDouble Double
	// 统计值-该Group 日更新任务数量
	DbDecimal   Decimal `mgp:"length:10_2"`
	DbText      Text
	DbBlob      Blob
	CreatedTime Datetime
	// 禁用组织，当组织禁用后，无法进行创建修改删除等操作，可查看
	State Bool `mgp:"index default 1"`
}
