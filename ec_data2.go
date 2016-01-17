package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/lib/pq"
	"strconv"
	"strings"
)

var tb_name = "laundry_my"

func addFile() {

	db, err := sql.Open("postgres", "user=postgres password=root dbname="+tb_name+" sslmode=disable")

	if err != nil {
		fmt.Println(err)
		return
	}

	rows, err := db.Query("SELECT order_id, customer_id, birth, sex, payment_total, customer_create_date, order_create_date FROM tmps3")

	if err != nil {
		fmt.Println(err)
		return
	}

	fileName := "test.tmp2"

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

		var order_id, customer_id, sex, payment_total, birth, customer_create_date, order_create_date interface{}

		err := rows.Scan(&order_id, &customer_id, &birth, &sex, &payment_total, &customer_create_date, &order_create_date)
		if err != nil {
			fmt.Println(err)
			return
		}

		var order_idF, customer_idF, sexF, payment_totalF, birthF, customer_create_dateF, order_create_dateF int64

		if n, ok := order_id.(int64); ok {
			order_idF = n
		}

		if n, ok := customer_id.(int64); ok {
			customer_idF = n
		}

		if n, ok := sex.(int64); ok {
			sexF = n
		}

		if n, ok := payment_total.(int64); ok {
			payment_totalF = n
		}

		if n, ok := birth.(time.Time); ok {
			birthF = n.Unix()
		}

		if n, ok := customer_create_date.(time.Time); ok {
			customer_create_dateF = n.Unix()
		}

		if n, ok := order_create_date.(time.Time); ok {
			order_create_dateF = n.Unix()
		}

		fmt.Fprintf(file, "%v,%v,%v,%v,%v,%v,%v\n", order_idF, customer_idF, sexF, payment_totalF, birthF, customer_create_dateF, order_create_dateF)

	}
	fmt.Println(i)

}

func getLineData(data []byte) []int {
	reData := make([]int, 7, 7)

	tmp := strings.Split(string(data), ",")
	reData[0], _ = strconv.Atoi(tmp[0])
	reData[1], _ = strconv.Atoi(tmp[1])
	reData[3], _ = strconv.Atoi(tmp[3])
	reData[6], _ = strconv.Atoi(tmp[6])
	/*
	startKey := 0
	stopKey := -1

	reKey := 0

	for key, value := range data {
		if value == 44 {
			startKey = stopKey + 1
			stopKey = key

			if reKey != 2 && reKey != 4 && reKey != 5 {
				reData[reKey], _ = strconv.Atoi(string(data[startKey:stopKey]))
			}

			reKey++
		}
	}

	startKey = stopKey + 1
	stopKey = len(data)

	reData[reKey], _ = strconv.Atoi(string(data[startKey:stopKey]))
	*/
	return reData
}

func getFile() {
	file, _ := os.Open("test.tmp2")
	bs := bufio.NewScanner(file)
	i := 0


	for bs.Scan() {
		getLineData(bs.Bytes())

		i++
	}

	fmt.Println(i)
}

func addTestSql() {
	db, err := sql.Open("postgres", "user=postgres password=root dbname="+tb_name+" sslmode=disable")

	if err != nil {
		fmt.Println(err)
		return
	}

	db.Exec(`INSERT INTO dtb_order VALUE (
order_id
,order_temp_id
,customer_id
,message
,order_name01
,order_name02
,order_kana01
,order_kana02
,order_email
,order_tel01
,order_tel02
,order_tel03
,order_fax01
,order_fax02
,order_fax03
,order_zip01
,order_zip02
,order_pref
,order_addr01
,order_addr02
,order_sex
,order_birth
,order_job
,subtotal
,discount
,deliv_id
,deliv_fee
,charge
,use_point
,add_point
,birth_point
,tax
,total
,payment_total
,payment_id
,payment_method
,note
,status
,create_date
,update_date
,commit_date
,payment_date
,device_type_id
,del_flg
,memo01
,memo02
,memo03
,memo04
,memo05
,memo06
,memo07
,memo08
,memo09
,memo10
,order_tax_rate
,order_tax_rule
,order_zipcode
,order_country_id
,plg_coupon_manage_coupon_code
,receipt_flg
,plg_volume_discount
,plg_manage_discount
,plg_coupon_manage_coupon_discount
,order_company_name
,sale_subtotal
,user_agent
,customer_rast_point
)
SELECT
order_id + (SELECT max(order_id) FROM dtb_order) order_id
,order_temp_id
,customer_id
,message
,order_name01
,order_name02
,order_kana01
,order_kana02
,order_email
,order_tel01
,order_tel02
,order_tel03
,order_fax01
,order_fax02
,order_fax03
,order_zip01
,order_zip02
,order_pref
,order_addr01
,order_addr02
,order_sex
,order_birth
,order_job
,subtotal
,discount
,deliv_id
,deliv_fee
,charge
,use_point
,add_point
,birth_point
,tax
,total
,payment_total
,payment_id
,payment_method
,note
,status
,create_date
,update_date
,commit_date
,payment_date
,device_type_id
,del_flg
,memo01
,memo02
,memo03
,memo04
,memo05
,memo06
,memo07
,memo08
,memo09	
,memo10
,order_tax_rate
,order_tax_rule
,order_zipcode
,order_country_id
,plg_coupon_manage_coupon_code
,receipt_flg
,plg_volume_discount
,plg_manage_discount
,plg_coupon_manage_coupon_discount
,order_company_name
,sale_subtotal
,user_agent
,customer_rast_point
FROM dtb_order`)

}

func sqlTest() {
	db, err := sql.Open("postgres", "user=postgres password=root dbname="+tb_name+" sslmode=disable")

	if err != nil {
		fmt.Println(err)
		return
	}

	rows, err := db.Query(`
SELECT
	customer_id,
	CASE WHEN A IS NULL THEN 0 ELSE A END,
	CASE WHEN B IS NULL THEN 0 ELSE B END,
	CASE WHEN C IS NULL THEN 0 ELSE C END,
	CASE
		WHEN
			A IS NULL AND B IS NULL AND C IS NULL
		THEN
			'休眠'
		WHEN
			a1 > 1 AND a2> 1
		THEN
			'最優良'
		WHEN
			a1 > 1 AND a2 = 0
		THEN
			'優良'
		WHEN
			a1 > 1 AND a2 < -1
		THEN
			'準優良'
		WHEN
			a1 = 0 AND a2 > 1
		THEN
			'優良傾向'
		WHEN
			a1 = 0 AND a2 = 0
		THEN
			'安定'
		WHEN
			a1 = 0 AND a2 < -1
		THEN
			'休眠傾向'
		WHEN
			a1 < -1 AND a2 > 1
		THEN
			'休眠予備A'
		WHEN
			a1 < -1 AND a2 = 0
		THEN
			'休眠予備B'
		WHEN 
			a1 < -1 AND a2 < -1
		THEN
			'休眠'
	END as p
FROM
	(
		SELECT
			t.customer_id customer_id,
			CASE
				WHEN B IS NULL AND A IS NULL THEN 0
				WHEN B IS NULL THEN 100
				WHEN A IS NULL THEN -100
				ELSE cast(cast (A as float ) / cast (B as float ) * 100 - 100 as integer)
			END a1,
			CASE
				WHEN C IS NULL AND B IS NULL THEN 0
				WHEN C IS NULL THEN 100
				WHEN B IS NULL THEN -100
				ELSE cast(cast (B as float ) / cast (C as float ) * 100 - 100 as integer)
			END a2,
			A,
			B,
			C
		FROM
			dtb_customer t
				LEFT JOIN
			(SELECT
				COUNT(order_id) A, customer_id
			FROM
				tmps3
			WHERE
				order_create_date <= date('2015-05-01 23:59:59') AND  order_create_date > date('2014-05-01 23:59:59') AND
				customer_create_date >= date('2012-05-01 23:59:59') AND customer_id IS NOT null AND order_id IS NOT null
			GROUP BY customer_id) t1
			USING (customer_id)
				LEFT JOIN
			(SELECT
				COUNT(order_id) B, customer_id
			FROM
				tmps3
			WHERE
				order_create_date <= date('2014-05-01 23:59:59') AND  order_create_date > date('2013-05-01 23:59:59') AND
				customer_create_date >= date('2012-05-01 23:59:59') AND customer_id IS NOT null AND order_id IS NOT null
			GROUP BY customer_id) t2
			USING (customer_id)
				LEFT JOIN
			(SELECT
				COUNT(order_id) C, customer_id
			FROM
				tmps3
			WHERE
				order_create_date <= date('2013-05-01 23:59:59') AND  order_create_date > date('2012-05-01 23:59:59') AND
				customer_create_date >= date('2012-05-01 23:59:59') AND customer_id IS NOT null AND order_id IS NOT null
			GROUP BY customer_id) t3
			USING (customer_id)
			WHERE t.del_flg = 0
	) tmp`)

	if err != nil {
		fmt.Println(err)
		return
	}

	i := 0
	for rows.Next() {
		i++
	}
	fmt.Println(i)
}

func main() {
	t1 := time.Now()
	//getFile()
	//sqlTest()
	getFile()
	//addFile()
	fmt.Println(time.Now().Sub(t1))
	//fmt.Println("---------------------")
	//t1 = time.Now()
	//sqlTest()
	//fmt.Println(time.Now().Sub(t1))
}
