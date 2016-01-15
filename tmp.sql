
	DELETE FROM tmps;

INSERT INTO tmps (order_id, customer_id, sa, create_date, customer_create_date, rank) 
	SELECT
		order_id,
		customer_id,
		sa,
		create_date,
		customer_create_date,
		cast(
			CASE WHEN create_date > now - sa THEN 1
				WHEN create_date > now - 2 * sa THEN 2
				ELSE 3
			END as integer
		) rank
	FROM (
		SELECT 
			order_id,
			customer_id,
			now,
			(now - customer_create_date) / 3 sa,
			create_date,
			customer_create_date
		FROM (
			SELECT 
				order_id,
				dtb_order.customer_id customer_id,
				cast(extract(epoch FROM date(dtb_order.create_date)) as integer) create_date, 
				cast(extract(epoch FROM date(dtb_customer.create_date)) as integer) customer_create_date, 
				cast(extract(epoch FROM date(NOW())) as integer) now
			FROM dtb_customer JOIN 
				dtb_order ON (dtb_order.customer_id = dtb_customer.customer_id) 
			WHERE 
				dtb_customer.del_flg = 0 AND dtb_order.del_flg = 0 AND dtb_order.status = 5
		) tmp
	) tmp
;


//-------------------------------------------------------------------------------------------------------------

ALTER TABLE "public"."tmps" ALTER COLUMN "rank" TYPE integer USING rank::integer;


SELECT extract(epoch FROM date('1999-10-10')) - extract(epoch FROM date('1989-02-05'));
SELECT extract(epoch FROM age(date('1999-10-10'), timestamp '1989-02-05'));


SELECT age(date('1999-10-10'), timestamp '1989-02-05') * 2;


//-------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS tmps2;
create table tmps2 (
	order_id integer NOT NULL,
	customer_id integer NOT NULL,
	create_date timestamp without time zone NOT NULL,
	customer_create_date timestamp without time zone NOT NULL,
	sa interval,
	order_rank integer
);
CREATE INDEX tmps2_order_id_customer_id ON tmps2 USING btree (order_id, customer_id);


//最初データ取る
DELETE FROM tmps2;

INSERT INTO tmps2 (order_id, customer_id, create_date, customer_create_date)
	SELECT 
		order_id,
		customer_id,
		create_date,
		customer_create_date
	FROM (
		SELECT 
			order_id,
			dtb_order.customer_id customer_id,
			dtb_order.create_date create_date,
			dtb_customer.create_date customer_create_date
		FROM dtb_customer JOIN 
			dtb_order ON (dtb_order.customer_id = dtb_customer.customer_id) 
		WHERE 
			dtb_customer.del_flg = 0 AND dtb_order.del_flg = 0 AND dtb_order.status = 5
	) tmp

//基準日によりデータ計算
UPDATE tmps2 SET sa = age(now(), customer_create_date) / 3 , order_rank = 
		CASE WHEN create_date > now() - age(now(), customer_create_date)/3 THEN 1
			 WHEN create_date > now() - age(now(), customer_create_date)/3 * 2 THEN 2
			 ELSE 3
		END


SELECT dtb_customer.customer_id,
	cast (tmp1.count as float ) / cast (tmp2.count as float ) a,
	cast (tmp2.count as float ) / cast (tmp3.count as float ) b
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
	ON (dtb_customer.customer_id = tmp3.customer_id)
ORDER BY dtb_customer.customer_id


SELECT 
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
	ON (dtb_customer.customer_id = tmp3.customer_id)




本物に近い
-------------------------------

DROP TABLE IF EXISTS tmps3;
create table tmps3 (
	order_id integer,
	customer_id integer,
	birth timestamp without time zone,
	sex smallint,
	payment_total integer,
	customer_create_date timestamp without time zone ,
	order_create_date timestamp without time zone
);
CREATE INDEX tmps3_order_id_customer_id ON tmps3 USING btree (order_id, customer_id);

--最初データ
DELETE FROM tmps3;

INSERT INTO tmps3 (order_id, customer_id, birth, sex, payment_total, customer_create_date, order_create_date)
	SELECT 
		order_id,
		dtb_customer.customer_id customer_id,
		dtb_customer.birth birth,
		dtb_customer.sex sex,
		payment_total,
		dtb_customer.create_date customer_create_date, 
		dtb_order.create_date order_create_date
	FROM 
		dtb_customer FULL OUTER JOIN dtb_order ON (dtb_customer.customer_id = dtb_order.customer_id) 
	WHERE 
		(dtb_customer.del_flg = 0 OR dtb_customer.del_flg IS NULL) 
			AND 
		(dtb_order.del_flg = 0 OR dtb_order.del_flg IS NULL)
;


--ランク分ける処理
--パターン1のみランク判定を行う 基準日からランクデータを集計する  
--2015-05-01 23:59:59～2014-05-01 23:59:59 
--2014-05-01 23:59:59～2013-05-01 23:59:59
--2013-05-01 23:59:59～2012-05-01 23:59:59

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
	) tmp ORDER BY customer_id ;

















-----------------テストデータ
INSERT INTO dtb_order VALUE (
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
FROM dtb_order