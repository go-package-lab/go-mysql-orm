```
package main

import (
	"fmt"
	"github.com/go-package-lab/go-mysql-orm"
)
var DB *go_mysql_orm.Client

func init() {
	config:=go_mysql_orm.Config{
		DriverName: "mysql",
		Addr:       "127.0.0.1",
		User:       "ops",
		Passwd:     "xxxx",
		Port:       "3306",
		DBName:     "ops",
		Debug:      false,
	}
	DB = go_mysql_orm.NewClient(config)

}
func main()  {
	data, err := DB.GetOne("test", "*", "id > ? ORDER BY id DESC", 11)
	fmt.Println(data,err)
	//insert single data
	postData := map[string]interface{}{
		"title": "title",
		"uid":   22,
	}
	DB.Insert("test", postData)
}

```
