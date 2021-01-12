// Copyright 2017 The casbin Authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package casbinpgadapter

import (
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"

	"database/sql"
	"github.com/casbin/casbin/v2/model"
	"github.com/casbin/casbin/v2/persist"
	_ "github.com/lib/pq"
)

const (
	defaultDatabaseName = "casbin"
	defaultTableName    = "casbin_rule"
)

type CasbinRule struct {
	ID    uint   `gorm:"primaryKey;autoIncrement"`
	PType string `gorm:"size:40;uniqueIndex:unique_index"`
	V0    string `gorm:"size:40;uniqueIndex:unique_index"`
	V1    string `gorm:"size:40;uniqueIndex:unique_index"`
	V2    string `gorm:"size:40;uniqueIndex:unique_index"`
	V3    string `gorm:"size:40;uniqueIndex:unique_index"`
	V4    string `gorm:"size:40;uniqueIndex:unique_index"`
	V5    string `gorm:"size:40;uniqueIndex:unique_index"`
}

type Filter struct {
	PType []string
	V0    []string
	V1    []string
	V2    []string
	V3    []string
	V4    []string
	V5    []string
}

// Adapter represents the Gorm adapter for policy storage.
type Adapter struct {
	driverName     string
	dataSourceName string
	databaseName   string
	tablePrefix    string
	tableName      string
	dbSpecified    bool
	db             *sql.DB
	isFiltered     bool
}

// finalizer is the destructor for Adapter.
func finalizer(a *Adapter) {
	if err :=a.db.Close();err != nil {
		panic(err)
	}
}

// NewAdapter is the constructor for Adapter.
// Params : databaseName,tableName,dbSpecified
//			databaseName,{tableName/dbSpecified}
//			{database/dbSpecified}
// databaseName and tableName are user defined.
// Their default value are "casbin" and "casbin_rule"
//
// dbSpecified is an optional bool parameter. The default value is false.
// It's up to whether you have specified an existing DB in dataSourceName.
// If dbSpecified == true, you need to make sure the DB in dataSourceName exists.
// If dbSpecified == false, the adapter will automatically create a DB named databaseName.
func NewAdapter(driverName string, dataSourceName string, params ...interface{}) (*Adapter, error) {
	a := &Adapter{}
	a.driverName = driverName
	a.dataSourceName = dataSourceName

	a.tableName = defaultTableName
	a.databaseName = defaultDatabaseName
	a.dbSpecified = true

	if len(params) == 0 {

	} else if len(params) == 1 {
		switch p1 := params[0].(type) {
		case bool:
			a.dbSpecified = p1
		case string:
			a.databaseName = p1
		default:
			return nil, errors.New("wrong format")
		}
	} else if len(params) == 2 {
		switch p2 := params[1].(type) {
		case bool:
			a.dbSpecified = p2
			p1, ok := params[0].(string)
			if !ok {
				return nil, errors.New("wrong format")
			}
			a.databaseName = p1
		case string:
			p1, ok := params[0].(string)
			if !ok {
				return nil, errors.New("wrong format")
			}
			a.databaseName = p1
			a.tableName = p2
		default:
			return nil, errors.New("wrong format")
		}
	} else if len(params) == 3 {
		if p3, ok := params[2].(bool); ok {
			a.dbSpecified = p3
			a.databaseName = params[0].(string)
			a.tableName = params[1].(string)
		} else {
			return nil, errors.New("wrong format")
		}
	} else {
		return nil, errors.New("too many parameters")
	}

	// Open the DB, create it if not existed.
	err := a.open()
	if err != nil {
		return nil, err
	}

	// Call the destructor when the object is released.
	runtime.SetFinalizer(a, finalizer)

	return a, nil
}

// NewAdapterByDBUseTableName creates gorm-adapter by an existing Gorm instance and the specified table prefix and table name
// Example: gormadapter.NewAdapterByDBUseTableName(&db, "cms", "casbin") Automatically generate table name like this "cms_casbin"
func NewAdapterByDBUseTableName(db *sql.DB, prefix string, tableName string) (*Adapter, error) {
	if len(tableName) == 0 {
		tableName = defaultTableName
	}
	a := &Adapter{
		tablePrefix: prefix,
		tableName:   tableName,
		driverName: "postgres",
	}

	a.db = db
	err := a.createTable()
	if err != nil {
		return nil, err
	}
	return a, nil
}

// NewAdapterByDB creates gorm-adapter by an existing Gorm instance
func NewAdapterByDB(db *sql.DB) (*Adapter, error) {
	return NewAdapterByDBUseTableName(db, "", defaultTableName)
}

func openDBConnection(driverName, dataSourceName string) (*sql.DB, error) {
	db, err := sql.Open(driverName,dataSourceName)
	if err != nil {
		return nil, err
	}
	return db, err
}

func (a *Adapter) createDatabase() error {
	var err error
	//db, err := openDBConnection(a.driverName, a.dataSourceName)
	//if err != nil {
	//	return err
	//}
	if a.driverName == "postgres" {
		if _, err = a.db.Exec("CREATE DATABASE IF NOT EXISTS" + a.databaseName); err != nil {
			return err
		}
	}
	return nil
}

func (a *Adapter) open() error {
	var err error
	var db *sql.DB

	if a.dbSpecified {
		db, err = openDBConnection(a.driverName, a.dataSourceName)
		if err != nil {
			return err
		}
	} else {
		if err = a.createDatabase(); err != nil {
			return err
		}
		if a.driverName == "postgres" {
			db, err = openDBConnection(a.driverName, a.dataSourceName+" dbname="+a.databaseName)
		}
		if err != nil {
			return err
		}
	}
	a.db = db
	return a.createTable()
}

func (a *Adapter) close() error {
	return a.db.Close()
}

// getTableInstance return the dynamic table name
func (a *Adapter) getTableInstance() *CasbinRule {
	return &CasbinRule{}
}

func (a *Adapter) casbinRuleTable() func(db *sql.DB) *sql.DB {
	return func(db *sql.DB) *sql.DB {
		if a.tablePrefix != "" {
			a.tableName=(a.tablePrefix + "_" + a.tableName)
		}
		return db
	}
}

func (a *Adapter) createTable() error {
	var err error
	//db, err := openDBConnection(a.driverName, a.dataSourceName)
	//if err != nil {
	//	return err
	//}
	if a.driverName == "postgres" {
		qstr := "CREATE table IF NOT EXISTS " + a.tableName +" (id bigserial NOT NULL,p_type varchar(40) NULL,v0 varchar(40) NULL,v1 varchar(40) NULL,v2 varchar(40) NULL,v3 varchar(40) NULL,v4 varchar(40) NULL,v5 varchar(40) NULL,CONSTRAINT casbin_rule_pkey PRIMARY KEY (id)); CREATE UNIQUE INDEX IF NOT EXISTS unique_index ON "+a.tableName+" USING btree (p_type, v0, v1, v2, v3, v4, v5);"
		if _, err = a.db.Exec(qstr); err != nil {
			return err
		}
	}
	return nil
}

func (a *Adapter) dropTable() error {
	//db, err := openDBConnection(a.driverName, a.dataSourceName)
	//if err != nil {
	//	return err
	//}
	if _, err := a.db.Exec("DROP TABLE " + a.tableName +`;`); err != nil {
		return err
	}
	return nil
}

func loadPolicyLine(line CasbinRule, model model.Model) {
	var p = []string{line.PType,
		line.V0, line.V1, line.V2, line.V3, line.V4, line.V5}

	var lineText string
	if line.V5 != "" {
		lineText = strings.Join(p, ", ")
	} else if line.V4 != "" {
		lineText = strings.Join(p[:6], ", ")
	} else if line.V3 != "" {
		lineText = strings.Join(p[:5], ", ")
	} else if line.V2 != "" {
		lineText = strings.Join(p[:4], ", ")
	} else if line.V1 != "" {
		lineText = strings.Join(p[:3], ", ")
	} else if line.V0 != "" {
		lineText = strings.Join(p[:2], ", ")
	}

	persist.LoadPolicyLine(lineText, model)
}

// LoadPolicy loads policy from database.
func (a *Adapter) LoadPolicy(model model.Model) error {
	//var lines []CasbinRule
	ruleline :=CasbinRule{}
	if rows,err := a.db.Query("select * from "+a.tableName+";"); err != nil {
		return err
	} else {
		for rows.Next(){
			rows.Scan(&ruleline.ID,&ruleline.PType,&ruleline.V0,&ruleline.V1,&ruleline.V2,&ruleline.V3,&ruleline.V4,&ruleline.V5)
			loadPolicyLine(ruleline, model)
		}
	}
	//for _, line := range lines {
	//	loadPolicyLine(line, model)
	//}
	return nil
}

// LoadFilteredPolicy loads only policy rules that match the filter.
func (a *Adapter) LoadFilteredPolicy(model model.Model, filter interface{}) error {
	//var lines []CasbinRule
	//var ruleline CasbinRule
	//var qstr string
	filterValue, ok := filter.(Filter)
	//db :=a.db
	if !ok {
		return errors.New("invalid filter type")
	}
	if len(filterValue.PType) > 0 {
		//qstr = "select * from "+a.tableName+" where p_type in ($1);"
		err :=a.QueryFilter("p_type", model,filterValue.PType)
		if err != nil {
			return err
		}
		//qparstr := make([]interface{},1)
		//for a :=range filterValue.PType{
		//	qparstr = append(qparstr, a)
		//}
		//rows, err := a.db.Query(qstr, qparstr)
		//if err !=nil {
		//	return err
		//}
		//for rows.Next(){
		//	rows.Scan(&ruleline.ID,&ruleline.PType,&ruleline.V0,&ruleline.V1,&ruleline.V2,&ruleline.V3,&ruleline.V4,&ruleline.V5)
		//	loadPolicyLine(ruleline, model)
		//}
	}
	if len(filterValue.V0) > 0 {
		//qstr = "select * from "+a.tableName+" where v0 in ($1);"
		err :=a.QueryFilter( "v0", model,filterValue.V0)
		if err != nil {
			return err
		}
		//rows, err := a.db.Query(qstr, filterValue.V0)
		//if err !=nil {
		//	return err
		//}
		//for rows.Next(){
		//	rows.Scan(&ruleline.ID,&ruleline.PType,&ruleline.V0,&ruleline.V1,&ruleline.V2,&ruleline.V3,&ruleline.V4,&ruleline.V5)
		//	loadPolicyLine(ruleline, model)
		//}
	}
	if len(filterValue.V1) > 0 {
		//qstr = "select * from "+a.tableName+" where v1 in ($1);"
		err :=a.QueryFilter( "v1", model,filterValue.V1)
		if err != nil {
			return err
		}
		//rows, err := a.db.Query(qstr, filterValue.V1)
		//if err !=nil {
		//	return err
		//}
		//for rows.Next(){
		//	rows.Scan(&ruleline.ID,&ruleline.PType,&ruleline.V0,&ruleline.V1,&ruleline.V2,&ruleline.V3,&ruleline.V4,&ruleline.V5)
		//	loadPolicyLine(ruleline, model)
		//}
	}
	if len(filterValue.V2) > 0 {
		//qstr = "select * from "+a.tableName+" where v2 in ($1);"
		err :=a.QueryFilter("v2", model,filterValue.V2)
		if err != nil {
			return err
		}
	}
	if len(filterValue.V3) > 0 {
		//qstr = "select * from "+a.tableName+" where v3 in ($1);"
		err :=a.QueryFilter("v3", model,filterValue.V3)
		if err != nil {
			return err
		}
	}
	if len(filterValue.V4) > 0 {
		//qstr = "select * from "+a.tableName+" where v4 in ($1);"
		err :=a.QueryFilter("v4", model,filterValue.V4)
		if err != nil {
			return err
		}
	}
	if len(filterValue.V5) > 0 {
		//qstr = "select * from "+a.tableName+" where v5 in ($1);"
		err :=a.QueryFilter("v5", model,filterValue.V5)
		if err != nil {
			return err
		}
		//rows, err := a.db.Query(qstr, filterValue.V5)
		//if err !=nil {
		//	return err
		//}
		//for rows.Next(){
		//	rows.Scan(&ruleline.ID,&ruleline.PType,&ruleline.V0,&ruleline.V1,&ruleline.V2,&ruleline.V3,&ruleline.V4,&ruleline.V5)
		//	loadPolicyLine(ruleline, model)
		//}
	}

	///*if err := a.db.Scopes(a.filterQuery(a.db, filterValue)).Order("ID").Find(&lines).Error; err != nil {
	//	return err
	//}*/

	//for _, line := range lines {
	//	loadPolicyLine(line, model)
	//}
	a.isFiltered = true

	return nil
}

func (a *Adapter) QueryFilter(filtersub string,model model.Model,qvalue []string)error{
	var ruleline CasbinRule
	qparstr := make([]interface{},0)
	//i := 1
	var comma string
	for a, value :=range qvalue{
		if a != 0 {
			comma += ","
		}
		comma += " $"+strconv.Itoa(a+1)
		//i++
		qparstr = append(qparstr, value)
	}
	qstr := "select * from "+a.tableName+" where "+filtersub+" in ("+comma+");"
	rows, err := a.db.Query(qstr, qparstr...)
	if err !=nil {
		return err
	}
	for rows.Next(){
		rows.Scan(&ruleline.ID,&ruleline.PType,&ruleline.V0,&ruleline.V1,&ruleline.V2,&ruleline.V3,&ruleline.V4,&ruleline.V5)
		loadPolicyLine(ruleline, model)
	}
	return nil
}

// IsFiltered returns true if the loaded policy has been filtered.
func (a *Adapter) IsFiltered() bool {
	return a.isFiltered
}

// filterQuery builds the gorm query to match the rule filter to use within a scope.
//func (a *Adapter) filterQuery(db *sql.DB, filter Filter) func(db *sql.DB) *sql.DB {
//	return func(db *sql.DB) *sql.DB {
//		if len(filter.PType) > 0 {
//			db = db.Where("p_type in (?)", filter.PType)
//		}
//		if len(filter.V0) > 0 {
//			db = db.Where("v0 in (?)", filter.V0)
//		}
//		if len(filter.V1) > 0 {
//			db = db.Where("v1 in (?)", filter.V1)
//		}
//		if len(filter.V2) > 0 {
//			db = db.Where("v2 in (?)", filter.V2)
//		}
//		if len(filter.V3) > 0 {
//			db = db.Where("v3 in (?)", filter.V3)
//		}
//		if len(filter.V4) > 0 {
//			db = db.Where("v4 in (?)", filter.V4)
//		}
//		if len(filter.V5) > 0 {
//			db = db.Where("v5 in (?)", filter.V5)
//		}
//		return db
//	}
//}

func (a *Adapter) savePolicyLine(ptype string, rule []string) map[string]interface{} {
	qstr := make(map[string]interface{})
	qstr[string("p_type")] = ptype
	if len(rule) > 0 {
		qstr[string("v0")] = rule[0]
	}
	if len(rule) > 1 {
		qstr[string("v1")] = rule[1]
	}
	if len(rule) > 2 {
		qstr[string("v2")] = rule[2]
	}
	if len(rule) > 3 {
		qstr[string("v3")] = rule[3]
	}
	if len(rule) > 4 {
		qstr[string("v4")] = rule[4]
	}
	if len(rule) > 5 {
		qstr[string("v5")] = rule[5]
	}

	return qstr
}

// SavePolicy saves policy to database.
func (a *Adapter) SavePolicy(model model.Model) error {
	err := a.dropTable()
	if err != nil {
		return err
	}
	err = a.createTable()
	if err != nil {
		return err
	}

	for ptype, ast := range model["p"] {
		for _, rule := range ast.Policy {
			line := a.savePolicyLine(ptype, rule)
			err := a.ExecInsertSqlRow(line)
			if err != nil {
				return err
			}
		}
	}

	for ptype, ast := range model["g"] {
		for _, rule := range ast.Policy {
			line := a.savePolicyLine(ptype, rule)
			err := a.ExecInsertSqlRow(line)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// AddPolicy adds a policy rule to the storage.
func (a *Adapter) AddPolicy(sec string, ptype string, rule []string) error {
	line := a.savePolicyLine(ptype, rule)
	err := a.ExecInsertSqlRow(line)
	return err
}

// RemovePolicy removes a policy rule from the storage.
func (a *Adapter) RemovePolicy(sec string, ptype string, rule []string) error {
	//line := a.savePolicyLine(ptype, rule)
	err := a.rawDelete(ptype,a.db, rule) //can't use db.Delete as we're not using primary key http://jinzhu.me/gorm/crud.html#delete
	return err
}

// AddPolicies adds multiple policy rules to the storage.
func (a *Adapter) AddPolicies(sec string, ptype string, rules [][]string) error {
	for _, rule := range rules {
		line := a.savePolicyLine(ptype, rule)
		if err := a.ExecInsertSqlRow(line); err != nil {
			return err
		}
	}
	return nil
}

// RemovePolicies removes multiple policy rules from the storage.
func (a *Adapter) RemovePolicies(sec string, ptype string, rules [][]string) error {
	for _, rule := range rules {
		//line := a.savePolicyLine(ptype, rule)
		if err := a.rawDelete(ptype,a.db, rule); err != nil { //can't use db.Delete as we're not using primary key http://jinzhu.me/gorm/crud.html#delete
				return err
			}
	}
	return nil
}

// RemoveFilteredPolicy removes policy rules that match the filter from the storage.
func (a *Adapter) RemoveFilteredPolicy(sec string, ptype string, fieldIndex int, fieldValues ...string) error {
	//line := a.getTableInstance()
	var line []string
	//line = append(line, ptype)
	if fieldIndex <= 0 && 0 < fieldIndex+len(fieldValues) {
		line = append(line, fieldValues[0-fieldIndex])
	}
	if fieldIndex <= 1 && 1 < fieldIndex+len(fieldValues) {
		line = append(line, fieldValues[1-fieldIndex])
	}
	if fieldIndex <= 2 && 2 < fieldIndex+len(fieldValues) {
		line = append(line, fieldValues[2-fieldIndex])
	}
	if fieldIndex <= 3 && 3 < fieldIndex+len(fieldValues) {
		line = append(line, fieldValues[3-fieldIndex])
	}
	if fieldIndex <= 4 && 4 < fieldIndex+len(fieldValues) {
		line = append(line, fieldValues[4-fieldIndex])
	}
	if fieldIndex <= 5 && 5 < fieldIndex+len(fieldValues) {
		line = append(line, fieldValues[5-fieldIndex])
	}
	err := a.rawDelete(ptype,a.db, line)
	return err
}

func (a *Adapter) rawDelete(ptype string, db *sql.DB, line []string) error {
	//queryArgs := []interface{}{line.PType}
	var queryArgs []interface{}
	queryStr := "p_type = $1"
	queryArgs = append(queryArgs, ptype)
	i :=1
	if len(line) > 0 {
		i++
		queryStr += " and v0 = $"+strconv.Itoa(i)
		queryArgs = append(queryArgs, line[0])
	}
	if len(line) > 1 {
		i++
		queryStr += " and v1 = $"+strconv.Itoa(i)
		queryArgs = append(queryArgs, line[1])
	}
	if len(line) > 2 {
		i++
		queryStr += " and v2 = $"+strconv.Itoa(i)
		queryArgs = append(queryArgs, line[2])
	}
	if len(line) > 3 {
		i++
		queryStr += " and v3 = $"+strconv.Itoa(i)
		queryArgs = append(queryArgs, line[3])
	}
	if len(line) > 4 {
		i++
		queryStr += " and v4 = $"+strconv.Itoa(i)
		queryArgs = append(queryArgs, line[4])
	}
	if len(line) > 5 {
		i++
		queryStr += " and v5 = $"+strconv.Itoa(i)
		queryArgs = append(queryArgs, line[5])
	}
	//args := append([]interface{}{queryStr}, queryArgs...)
	//err := db.Delete(a.getTableInstance(), args...).Error
	dsql := "delete from "+a.tableName+" where "+ queryStr +";"
	_, err := db.Exec(dsql,queryArgs...)
	return err
}

func (a *Adapter) ExecInsertSqlRow(arg map[string]interface{}) error{
	var test []interface{}

	var qstrbt strings.Builder
	qstrbt.WriteString("insert into casbin_rule(")
	var valuestr string
	i :=0
	for k, v := range arg{
		if v == nil || v ==""{

		} else {
			if i != 0 {
				qstrbt.WriteString(",")
				valuestr += ","
			}
			qstrbt.WriteString(k)
			i++
			valuestr += "$"+strconv.Itoa(i)
			test = append(test,v)
		}

	}
	qstrbt.WriteString(") values(")
	qstr := qstrbt.String() + valuestr+ ");"
	fmt.Println(qstr)
	fmt.Println(test)
	_,err :=a.db.Exec(qstr,test...)
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

func (a *Adapter) ExecDeleteSqlRow(arg map[string]interface{}) error{
	var test []interface{}

	var qstrbt strings.Builder
	qstrbt.WriteString("delete from casbin_rule where ")
	var valuestr string
	i :=0
	for k, v := range arg{
		if v == nil || v ==""{

		} else {
			if i != 0 {
				qstrbt.WriteString(",")
				valuestr += ","
			}
			qstrbt.WriteString(k)
			i++
			valuestr += "$"+strconv.Itoa(i)
			test = append(test,v)
		}

	}
	qstrbt.WriteString(") values(")
	qstr := qstrbt.String() + valuestr+ ");"
	fmt.Println(qstr)
	fmt.Println(test)
	_,err :=a.db.Exec(qstr,test...)
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

func (a *Adapter) GenerateQuerySql(filterValue Filter) (qstr string, err error){
	if len(filterValue.PType) > 0 {
		qstr = "select * from "+a.tableName+" where "+a.GetFilterQuery(filterValue.PType)+";"
	}
	if len(filterValue.V0) > 0 {
		qstr = "select * from "+a.tableName+" where "+a.GetFilterQuery(filterValue.PType)+";"
	}
	if len(filterValue.V1) > 0 {
		qstr = "select * from "+a.tableName+" where "+a.GetFilterQuery(filterValue.PType)+";"
	}
	if len(filterValue.V2) > 0 {
		qstr = "select * from "+a.tableName+" where "+a.GetFilterQuery(filterValue.PType)+";"
	}
	if len(filterValue.V3) > 0 {
		qstr = "select * from "+a.tableName+" where "+a.GetFilterQuery(filterValue.PType)+";"
	}
	if len(filterValue.V4) > 0 {
		qstr = "select * from "+a.tableName+" where "+a.GetFilterQuery(filterValue.PType)+";"
	}
	if len(filterValue.V5) > 0 {
		qstr = "select * from "+a.tableName+" where "+a.GetFilterQuery(filterValue.PType)+";"
	}
	return qstr, nil
}

func (a *Adapter)GetFilterQuery(filter []string) (string){
	var stbuilder strings.Builder
	i:=0
	for line := range filter{
		if i == 0 {
			stbuilder.WriteString(",")
		}
		i++
		stbuilder.WriteString(strconv.Itoa(line))
	}
	return stbuilder.String()
}