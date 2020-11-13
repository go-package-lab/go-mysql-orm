package go_mysql_orm

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"log"
	"strings"
)

type DataStruct map[string]interface{}
type Config struct {
	Db           *sql.DB
	DriverName   string
	Addr         string
	User         string
	Passwd       string
	Port         string
	DBName       string
	MaxOpenConns int
	MaxIdleConns int
	Debug        string
	_debug       bool
}
type Client struct {
	*sql.DB
	Config
}

func NewClient(conf Config) *Client {
	c := Client{}
	var err error
	cfg := mysql.NewConfig()
	cfg.User = conf.User
	cfg.Passwd = conf.Passwd
	cfg.Net = "tcp"
	cfg.Addr = conf.Addr
	cfg.DBName = conf.DBName
	dsn := cfg.FormatDSN()
	c.Db, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Println(err)
		return nil
	}
	if err := c.Db.Ping(); err != nil {
		log.Println(err)
		return nil
	}
	if conf.Debug == "true" {
		c._debug = true
	} else {
		c._debug = false
	}
	maxOpenConns := 0
	if c.MaxOpenConns > 0 {
		maxOpenConns = c.MaxOpenConns
	}
	maxIdleConns := 0
	if c.MaxIdleConns > 0 {
		maxIdleConns = c.MaxIdleConns
	}
	c.Db.SetMaxOpenConns(maxOpenConns)
	c.Db.SetMaxIdleConns(maxIdleConns)
	return &c
}

func (S *DataStruct) parseData() (string, []interface{}, error) {
	keys := []string{}
	values := []interface{}{}
	for key, value := range *S {
		keys = append(keys, key)
		values = append(values, value)
	}
	return strings.Join(keys, ","), values, nil
}

//添加或者修改数据
func (d *DataStruct) Set(key string, value interface{}) {
	(*d)[key] = value
}

//获取数据
func (d DataStruct) Get(key string) interface{} {
	return d[key]
}

//配合update使用，生成 field=?
func (S *DataStruct) setData() (string, []interface{}, error) {
	keys := []string{}
	values := []interface{}{}
	for key, value := range *S {
		keys = append(keys, key+"=?")
		values = append(values, value)
	}
	return strings.Join(keys, ","), values, nil
}

//插入数据
func (c *Client) Insert(table string, datas DataStruct) (id int64, err error) {
	s, v, _ := datas.parseData()
	placeString := fmt.Sprintf("%s", strings.Repeat("?,", len(v)))
	placeString = placeString[:len(placeString)-1]
	sqlString := "INSERT INTO `" + table + "` (" + s + ") VALUES (" + placeString + ")"
	if c._debug {
		log.Println("SQL Debug:", sqlString, "\nSQL Param:", v)
	}
	result, err := c.Db.Exec(sqlString, v...)
	if err != nil {
		return
	}
	id, err = result.LastInsertId()
	if err != nil {
		return
	}
	return
}

//更新
func (c *Client) Update(table string, datas DataStruct, where string, args ...interface{}) (num int64, err error) {
	s, v, _ := datas.setData()
	sqlString := "UPDATE `" + table + "` SET " + s
	if where != "" {
		sqlString += " WHERE " + where
	}
	for _, value := range args {
		v = append(v, value)
	}
	if c._debug {
		log.Println("SQL Debug:", sqlString, "\nSQL Param:", v)
	}
	result, err := c.Db.Exec(sqlString, v...)
	if err != nil {
		return
	}
	num, err = result.RowsAffected()
	return
}

//获取一条
func (c *Client) GetOne(table, fields, where string, args ...interface{}) (map[string]interface{}, error) {
	sqlString := "SELECT " + fields + " FROM `" + table + "`"
	if where != "" {
		sqlString += " WHERE " + where
	}
	sqlString += " LIMIT 0,1"
	if c._debug {
		log.Println("SQL Debug:", sqlString, "\nSQL Param:", args)
	}
	rows, err := c.Db.Query(sqlString, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, _ := rows.Columns()
	columnLength := len(columns)
	cache := make([]interface{}, columnLength)
	for index, _ := range cache {
		var a interface{}
		cache[index] = &a
	}
	item := make(map[string]interface{})
	for rows.Next() {
		_ = rows.Scan(cache...)
		for i, data := range cache {
			vData := *(data.(*interface{}))
			switch vData.(type) {
			case []uint8:
				item[columns[i]] = string(vData.([]uint8))
			case int64:
				item[columns[i]] = vData
			default:
				item[columns[i]] = vData
			}
		}
	}
	//data := datas[key].([]uint8)
	//return string(data)
	return item, nil
}

//批量查询，不带分页计算
func (c *Client) Select(table string, fields string, where string, args ...interface{}) ([]map[string]interface{}, error) {
	sqlString := "SELECT " + fields + " FROM `" + table + "`"
	if where != "" {
		sqlString += " WHERE " + where
	}
	if c._debug {
		log.Println("SQL Debug:", sqlString, "\nSQL Param:", args)
	}
	rows, err := c.Db.Query(sqlString, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, _ := rows.Columns()
	columnLength := len(columns)
	cache := make([]interface{}, columnLength)
	for index, _ := range cache {
		var a interface{}
		cache[index] = &a
	}
	var results []map[string]interface{}
	for rows.Next() {
		_ = rows.Scan(cache...)
		item := make(map[string]interface{})
		for i, data := range cache {
			vData := *(data.(*interface{}))
			switch vData.(type) {
			case []uint8:
				item[columns[i]] = string(vData.([]uint8))
			case int64:
				item[columns[i]] = vData
			default:
				item[columns[i]] = vData
			}
		}
		results = append(results, item)
	}
	return results, nil
}
func (c *Client) Delete(table string, where string, args ...interface{}) (num int64, err error) {
	sqlString := "DELETE FROM `" + table + "`"
	if where != "" {
		sqlString += " WHERE " + where
	}
	if c._debug {
		log.Println("SQL Debug:", sqlString, "\nSQL Param:", args)
	}
	stmt, err := c.Db.Prepare(sqlString)
	if err != nil {
		return
	}
	result, err := stmt.Exec(args...)
	num, err = result.RowsAffected()
	return
}

func (c *Client) Count(table string, where string, args ...interface{}) (total int64, err error) {
	sqlString := "SELECT COUNT(*) as total FROM `" + table + "`"
	if where != "" {
		sqlString += " WHERE " + where
	}
	if c._debug {
		log.Println("SQL Debug:", sqlString, "\nSQL Param:", args)
	}
	stmt, err := c.Db.Prepare(sqlString)
	if err != nil {
		return
	}
	row := stmt.QueryRow(args...)
	err = row.Scan(&total)
	return
}

func (c *Client) Close() error {
	err := c.Db.Close()
	return err
}

func Format2String(datas map[string]interface{}, key string) string {
	if datas[key] == nil {
		return ""
	}
	data := datas[key].([]uint8)
	return string(data)
}

func (c *Client) BatchInsert(table string, datas []DataStruct) (num int64, err error) {
	var (
		placeString string
		columnName  []string
		sqlColumn   string
		columnData  []interface{}
	)
	if table == "" || len(datas) == 0 {
		return 0, errors.New("Param ERROR")
	}
	if len(datas) == 1 {
		_, err := c.Insert(table, datas[0])
		if err != nil {
			return 0, err
		}
		return 1, nil
	}
	s := strings.Repeat("?,", len(datas[0]))
	for _, data := range datas {
		placeString += fmt.Sprintf("(%s),", strings.TrimSuffix(s, ","))
		if columnName == nil {
			for k := range data {
				columnName = append(columnName, k)
			}
		}
		for _, key := range columnName {
			columnData = append(columnData, data[key])
		}
		sqlColumn = strings.Join(columnName, ",")
	}
	sqlString := fmt.Sprintf("INSERT INTO `%s`(%s) values %s", table, sqlColumn, strings.TrimSuffix(placeString, ","))
	if c._debug {
		log.Println("SQL Debug:", sqlString, "\nSQL Param:", columnData)
	}
	res, err := c.Db.Exec(sqlString, columnData...)
	if err != nil {
		return
	}
	num, err = res.RowsAffected()
	if err != nil {
		return
	}
	return
}
func (c *Client) Query(sqlString string, args ...interface{}) ([]map[string]interface{}, error) {
	if c._debug {
		log.Println("SQL Debug:", sqlString, "\nSQL Param:", args)
	}
	rows, err := c.Db.Query(sqlString, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, _ := rows.Columns()
	columnLength := len(columns)
	cache := make([]interface{}, columnLength)
	for index, _ := range cache {
		var a interface{}
		cache[index] = &a
	}
	var results []map[string]interface{}
	for rows.Next() {
		_ = rows.Scan(cache...)
		item := make(map[string]interface{})
		for i, data := range cache {
			vData := *(data.(*interface{}))
			switch vData.(type) {
			case []uint8:
				item[columns[i]] = string(vData.([]uint8))
			case int64:
				item[columns[i]] = vData
			default:
				item[columns[i]] = vData
			}
		}
		results = append(results, item)
	}
	return results, nil
}
