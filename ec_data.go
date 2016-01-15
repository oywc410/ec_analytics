package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

func addFile() {

	db, err := sql.Open("postgres", "user=postgres password=root dbname=laundry_my sslmode=disable")

	if err != nil {
		fmt.Println(err)
		return
	}

	rows, err := db.Query("SELECT order_id,customer_id,create_date,customer_create_date,order_rank FROM tmps2")

	if err != nil {
		fmt.Println(err)
		return
	}

	fileName := "test.tmp"

	var file *os.File

	isFile, err := os.Stat(fileName)
	if (err != nil && os.IsNotExist(err)) || isFile.IsDir() {
		file, _ = os.Create(fileName)
	} else {
		file, _ = os.OpenFile(fileName, os.O_WRONLY, 0666)
	}

	defer file.Close()

	i := 0

	for rows.Next() {
		i++
		var order_id, customer_id, order_rank int

		var create_date, customer_create_date time.Time

		err := rows.Scan(&order_id, &customer_id, &create_date, &customer_create_date, &order_rank)
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Fprintln(file, order_id, customer_id, create_date.Unix(), customer_create_date.Unix(), order_rank)

	}
	fmt.Println(i)

}

func getFile() {
	file, _ := os.Open("test.tmp")
	bs := bufio.NewScanner(file)
	i := 0

	var customer_id, order_rank int

	mapA := make(map[int]float32)
	mapB := make(map[int]float32)
	mapC := make(map[int]float32)
	allCustomerId := make([]int, len(mapA))

	for bs.Scan() {
		arrSplit := strings.Split(bs.Text(), " ")
		customer_id, _ = strconv.Atoi(arrSplit[1])
		order_rank, _ = strconv.Atoi(arrSplit[4])

		_, isExA := mapA[customer_id]
		_, isExB := mapB[customer_id]
		_, isExC := mapC[customer_id]

		if !isExA && !isExB && !isExC {
			allCustomerId = append(allCustomerId, customer_id)
		}

		switch order_rank {
		case 1:
			mapA[customer_id]++
		case 2:
			mapB[customer_id]++
		case 3:
			mapC[customer_id]++
		}
		i++
	}

	sort.Ints(allCustomerId)

	for _, customer_id = range allCustomerId {

		var a, b float32

		if _, isEx := mapB[customer_id]; isEx {
			a = mapA[customer_id] / mapB[customer_id]
		}

		if _, isEx := mapC[customer_id]; isEx {
			b = mapB[customer_id] / mapC[customer_id]
		}
		a, b = b, a
		//fmt.Println(customer_id, a, b)
	}

	fmt.Println(i)
}

func sqlTest() {
	db, err := sql.Open("postgres", "user=postgres password=root dbname=laundry_my sslmode=disable")

	if err != nil {
		fmt.Println(err)
		return
	}

	db.Query(`SELECT 
	dtb_customer.customer_id, 
	tmp1.count coun1,
	tmp2.count count2, 
	tmp3.count count3
FROM
	dtb_customer
	LEFT JOIN
	(SELECT 
		COUNT(order_id) count, 
		max(customer_id) customer_id
		FROM tmps2 WHERE order_rank = 1 GROUP BY customer_id) as tmp1
	ON (dtb_customer.customer_id = tmp1.customer_id)
	LEFT JOIN
	(SELECT 
		COUNT(order_id) count, 
		max(customer_id) customer_id
		FROM tmps2 WHERE order_rank = 2 GROUP BY customer_id) as tmp2
	ON (dtb_customer.customer_id = tmp2.customer_id)
	LEFT JOIN
	(SELECT 
		COUNT(order_id) count, 
		max(customer_id) customer_id
		FROM tmps2 WHERE order_rank = 3 GROUP BY customer_id) as tmp3
	ON (dtb_customer.customer_id = tmp3.customer_id)`)
}

func main() {
	t1 := time.Now()
	getFile()
	fmt.Println(time.Now().Sub(t1))
}
