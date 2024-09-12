
CREATE OR REPLACE PROCEDURE report_monthly(vmonth INT DEFAULT NULL)
LANGUAGE plpgsql
AS $$
declare
	sum_du_no_sau_wo NUMERIC := 0;
    sum_du_no_trc_wo NUMERIC := 0;
    sum_du_no_xau_trc_wo NUMERIC := 0;
    sum_du_no_xau_sau_wo NUMERIC := 0;
    vwo_balance NUMERIC := 0;
    vwo_balance_xau NUMERIC := 0;
    vmonth_key INT;
   	area_name_arr varchar[]:=  array['Đông Bắc Bộ','Tây Bắc Bộ','Đồng Bằng Sông Hồng','Bắc Trung Bộ','Nam Trung Bộ','Tây Nam Bộ','Đông Nam Bộ'];
   	area_arr varchar[]:= array['dong_bac_bo','tay_bac_bo','db_song_hong','bac_trung_bo','nam_trung_bo','tay_nam_bo','dong_nam_bo'];
    area_cde_arr varchar[] := array['B','C','D','E','F','G','H'];
   	sort_id_max int :=32;
    query text;
   	i int ;
    j int ;
   	m int;
   	n int;
BEGIN
    -- ---------------------
    -- THÔNG TIN NGƯỜI TẠO
    -- ---------------------
    -- Tên người tạo: Hồ Tấn Lộc
    -- Ngày tạo: 2024-May-22
    -- Mục đích : Tổng hợp các tiêu chí nguồn vốn và sử dụng vốn theo ngày

    -- ---------------------
    -- THÔNG TIN NGƯỜI CẬP NHẬT
    -- ---------------------
    -- Tên người cập nhật: 
    -- Ngày cập nhật: 
    -- Mục đích cập nhật: 

    -- ---------------------
    -- SUMMARY LUỒNG XỬ LÝ
    -------------------------
    -- Bước 1: Kiểm tra nếu tháng truyền vào là null sẽ month = thang hien tai
    -- ngược lại month=month
    -- Bước 2: Xóa dữ liệu bảng fact_report_xh, fact_report_tong_hop_raw tại month_key, fact_temp,fact_report_tong_hop 
    -- Bước 3: insert dữ liệu vào bảng fact_temp
    -- Bước 4: insert dữ liệu vào bảng fact_report_month
	-- Bước 5: insert dữ liệu vào bảng fact_report_tong_hop_raw
	-- Bước 6: insert dữ liệu vào bnảg fact_report_tong_hop
    ---------------------------
    -- CHI TIẾT CÁC BƯỚC
    -- Bước 1: Kiểm tra dữ liệu truyền
    IF vmonth IS NULL THEN 
        vmonth := EXTRACT(MONTH FROM CURRENT_DATE);
    ELSE
        vmonth := vmonth;
    END IF;
    vmonth_key := 2023 * 100 + vmonth;
    -- Bước 2: Xóa dữ liệu 2 bảng fact_report_tong_hop và fact_report_month tại month_key 
   -- DELETE FROM fact_report_tong_hop_raw WHERE month = vmonth_key;
    --DELETE FROM fact_report_xh WHERE month_key = vmonth_key;
   	truncate table fact_temp;
   	truncate table fact_report_tong_hop ;
   	truncate table fact_report_tong_hop_raw ;
   	truncate table fact_report_xh ;
   	truncate table fact_report_tong_hop_detail;
   	truncate table fact_head_value;
   	truncate table du_no_sau_wo ;
    -- Bước 3: insert dữ liệu vào bảng fact_temp
	-- insert fact_temp: bang luu tru ( ti le du no xau / tong du no, )
 	FOR n IN 1..array_length(area_cde_arr, 1) -- run loop in order to insert specific area
    LOOP
        sum_du_no_trc_wo := 0;
        sum_du_no_xau_trc_wo := 0;
        FOR m IN 1..vmonth
        loop
        	--accumulate du_no for each month
            SELECT 
                COALESCE(SUM(outstanding_principal), 0) 
            INTO sum_du_no_sau_wo
            FROM 
                fact_kpi_month_raw_data a
            JOIN 
                dim_tinh b ON a.pos_city = b.tinh
            WHERE 
                kpi_month = 2023 * 100 + m 
                AND ma_tinh = area_cde_arr[n];
            --accumulate wo_balance for each month
            SELECT 
                COALESCE(SUM(write_off_balance_principal), 0)
            INTO vwo_balance
            FROM 
                fact_kpi_month_raw_data a
            JOIN 
                 dim_tinh b ON a.pos_city = b.tinh
            WHERE 
                write_off_month BETWEEN 2023 * 100 + 1 AND 2023 * 100 + m 
                AND ma_tinh = area_cde_arr[n];
            sum_du_no_trc_wo := sum_du_no_trc_wo + sum_du_no_sau_wo + vwo_balance;
           --accumulate du_no_xau for each month
            SELECT 
                COALESCE(SUM(outstanding_principal), 0) 
            INTO sum_du_no_xau_sau_wo
            FROM 
                fact_kpi_month_raw_data a
            JOIN 
                 dim_tinh b ON a.pos_city = b.tinh
            WHERE 
                kpi_month = 2023 * 100 + m 
                AND max_bucket > 2 
                AND ma_tinh = area_cde_arr[n];
            --accumulate wo_balance_xau for each month   
            SELECT 
                COALESCE(SUM(write_off_balance_principal), 0) 
            INTO vwo_balance_xau
            FROM 
                fact_kpi_month_raw_data a
            JOIN 
                dim_tinh b ON a.pos_city = b.tinh
            WHERE 
                write_off_month BETWEEN 2023 * 100 + 1 AND 2023 * 100 + m
                AND ma_tinh = area_cde_arr[n]
                AND max_bucket > 2;   
            sum_du_no_xau_trc_wo := sum_du_no_xau_trc_wo + sum_du_no_xau_sau_wo + vwo_balance_xau;
        END LOOP;
        sum_du_no_trc_wo := sum_du_no_trc_wo / vmonth;
        sum_du_no_xau_trc_wo := sum_du_no_xau_trc_wo / vmonth;
       	INSERT INTO fact_temp (rate, area_code,du_no)
      	VALUES (sum_du_no_xau_trc_wo / sum_du_no_trc_wo, area_cde_arr[n], sum_du_no_trc_wo);
    END LOOP;
-- insert du_no_sau_wo ( bang luu tru du no nhom 1, du no nhom 2 va du no nhom 345)
insert into du_no_sau_wo (area,kpi_month ,du_no_ck,du_no_nhom_1,du_no_nhom_2,du_no_nhom_345)
   	select 
   		y.ma_khu_vuc , 
   		x.kpi_month , 
   		sum(outstanding_principal) as dnck, 
		sum 
		(
			case
				when coalesce(max_bucket,1) = 1 then outstanding_principal
				else 0
			end 
		) as os_after_wo_1,
		sum 
		(
			case
				when coalesce(max_bucket,1) = 2 then outstanding_principal
				else 0
			end 
		) as os_after_wo_2,
		sum 
		(
			case
				when coalesce(max_bucket,1) in (3,4,5) then outstanding_principal
				else 0
			end 
		) as os_after_wo_345
	from fact_kpi_month_raw_data x 
	join dim_tinh y on x.pos_city = y.tinh
	where kpi_month <= 202302
	and kpi_month >= 202301
	group by x.kpi_month,y.ma_khu_vuc  ;
-- tạo table lưu giá trị head cần phân bổ
 insert into fact_head_value(sort_id, value)
-- sort_id 14 ( lãi trong hạn ) với account_code thuộc : 702000030002, 702000030001,702000030102
	select 
		14 as sort_id,
		round(sum(amount),2 )as value
	from 
		fact_txn_raw_data
	where 
		account_code in ( '702000030002', '702000030001','702000030102') and 
		extract(month from transaction_date) <= vmonth  and substring(analysis_code,9,1) ='0'
	union all
-- sort_id 15 ( lãi quá hạn ) account_code thuộc : 702000030012, 702000030112
	(select 
		15 as sort_id,
		round(sum(amount),2 )as value
	from 
		fact_txn_raw_data
	where 
		account_code in ( '702000030012', '702000030112') and 
		extract(month from transaction_date) <= vmonth and substring(analysis_code,9,1) ='0')
	union all
-- sort_id 16 (phí bảo hiểm ) account_code thuộc :  '716000000001
	(select 
		16 as sort_id,
		round(sum(amount),2 )as value
	from 
		fact_txn_raw_data
	where 
		account_code in ( '716000000001') and 
		extract(month from transaction_date) <= vmonth and substring(analysis_code,9,1) ='0'	)
	union all
-- sort_id 17 ( phí tăng hạn mức ) với account_code thuộc : '719000030002
	(select 
		17 as sort_id,
		round(sum(amount),2 )as value
	from 
		fact_txn_raw_data
	where 
		account_code in ( '719000030002') and 
		extract(month from transaction_date) <= vmonth and substring(analysis_code,9,1) ='0'	)
	union all
-- sort_id 18 ( phí thanh toán chậm, thu từ ngoại bảng ) với account_code thuộc :  719000030003,719000030103,790000030003,790000030103,790000030004,790000030104
	(select 
		18 as sort_id,
		round(sum(amount),2 )as value
	from 
		fact_txn_raw_data
	where 
		account_code in ( '719000030003','719000030103','790000030003','790000030103','790000030004','790000030104') and 
		extract(month from transaction_date) <=  vmonth and substring(analysis_code,9,1) ='0'	);
 insert into fact_head_value(sort_id, value)
-- sort_id 4 ( Tổng thu nhập từ hoạt động thẻ ) = 14->18 
	select 
		4 as sort_id,
		round(sum(value),2) as value
	from 
		fact_head_value
	where sort_id between 14 and 18;
 insert into fact_head_value(sort_id, value)
 --sort_id 22 ( CP vốn CCTG ) với account_code 803000000001
 	select 
		22 as sort_id,
		round(sum(amount),2 )as value
	from 
		fact_txn_raw_data
	where 
		account_code in ( '803000000001') and 
		extract(month from transaction_date) <= vmonth and substring(analysis_code,9,1) ='0'
-- sort_id 20  ( CP vốn TT2 ) với account_code  '801000000001','802000000001'
	union all 
	(select 
		20 as sort_id,
		round(sum(amount),2 )as value
	from 
		fact_txn_raw_data
	where 
		account_code in ( '801000000001','802000000001') and 
		extract(month from transaction_date) <= vmonth and substring(analysis_code,9,1) ='0');
  insert into fact_head_value(sort_id, value)
 -- sort_id 5 ( Chi phí thuần KDV )
	select 
		5 as sort_id,
		round(sum(value),2) as value
	from 
		fact_head_value
	where sort_id between 20 and 22;
insert into fact_head_value(sort_id, value)
 --sort_id 26 ( CP hoa hồng ) account_code thuộc :  816000000001,816000000002,816000000003
 	select 
		26 as sort_id,
		round(sum(amount),2 )as value
	from 
		fact_txn_raw_data
	where 
		account_code in (  '816000000001','816000000002','816000000003') and 
		extract(month from transaction_date) <= vmonth and substring(analysis_code,9,1) ='0'
--sort_id 27 ( CP thuần KD khác )  với account_code thuộc :  809000000002,809000000001,811000000001,811000000102,811000000002,811014000001,811037000001,811039000001,811041000001,815000000001,819000000002,819000000003,819000000001,790000000003,790000050101,790000000101,790037000001,849000000001,899000000003,899000000002,811000000101,819000060001
	union all 
	(select 
		27 as sort_id,
		round(sum(amount),2 )as value
	from 
		fact_txn_raw_data
	where 
		account_code in ('809000000002','809000000001','811000000001','811000000102','811000000002','811014000001','811037000001','811039000001','811041000001','815000000001','819000000002','819000000003','819000000001','790000000003','790000050101','790000000101','790037000001','849000000001','899000000003','899000000002','811000000101','819000060001') and 
		extract(month from transaction_date) <= vmonth and substring(analysis_code,9,1) ='0')
	union all
-- sort_id 25 ( DT kinh doanh ) với account_code thuộc :  702000010001,702000010002,704000000001,705000000001,709000000001,714000000002,714000000003,714037000001,714000000004,714014000001,715000000001,715037000001,719000000001,709000000101,719000000101
	(select 
		25 as sort_id,
		round(sum(amount),2 )as value
	from 
		fact_txn_raw_data
	where 
		account_code in ('702000010001','702000010002','704000000001','705000000001','709000000001','714000000002','714000000003','714037000001','714000000004','714014000001','715000000001','715037000001','719000000001','709000000101','719000000101') and 
		extract(month from transaction_date) <=vmonth and substring(analysis_code,9,1) ='0');
 insert into fact_head_value(sort_id, value)
 -- sort_id 6 ( Chi phí thuần hoạt động khác )
	select 
		6 as sort_id,
		round(sum(value),2) as value
	from 
		fact_head_value
	where sort_id between 25 and 27;
insert into fact_head_value(sort_id, value)
 -- sort_id 7 ( Tổng thu nhập hoạt động )
	select 
		7 as sort_id,
		round(sum(value),2) as value
	from 
		fact_head_value
	where sort_id between 4 and 6;
insert into fact_head_value(sort_id, value)
--sort_id 30 ( CP nhân viên ) với account_code thuộc :  85x
 	select 
		30 as sort_id,
		round(sum(amount),2 )as value
		from fact_txn_raw_data
	where 
		CAST(account_code AS TEXT) LIKE '85%' and 
		extract(month from transaction_date) <= vmonth and substring(analysis_code,9,1) ='0'
--sort_id 31 ( CP quản lý ) với account_code thuộc :  86x
	union all
 	(select 
		31 as sort_id,
		round(sum(amount),2 )as value
	from 
		fact_txn_raw_data
	where 
		CAST(account_code AS TEXT) LIKE '86%' and 
		extract(month from transaction_date) <= vmonth and substring(analysis_code,9,1) ='0')
--sort_id 32 ( CP tài sản ) với account_code thuộc :  87x
	union all
 	(select 
		32 as sort_id,
		round(sum(amount),2 )as value
	from 
		fact_txn_raw_data
	where 
		CAST(account_code AS TEXT) LIKE '87%' and 
		extract(month from transaction_date) <= vmonth and substring(analysis_code,9,1) ='0');
insert into fact_ head_value(sort_id, value)
 -- sort_id 8 (Tổng chi phí hoạt động )
	select 
		8 as sort_id,
		round(sum(value),2) as value
	from 
		fact_head_value
	where sort_id between 30 and 32;
insert into fact_head_value(sort_id, value)
-- sort_id 9 ( Chi phí dự phòng ) với account_code thược 790000050001, 882200050001, 790000030001, 882200030001, 790000000001, 790000020101, 882200000001, 882200050101, 882200020101, 882200060001,790000050101 882200030101
 select 
		9 as sort_id,
		round(sum(amount),2 )as value
	from 
		fact_txn_raw_data
	where 
		account_code in ('790000050001', '882200050001', '790000030001', '882200030001', '790000000001', '790000020101', '882200000001', '882200050101', '882200020101', '882200060001','790000050101', '882200030101') and 
		extract(month from transaction_date) <= vmonth and substring(analysis_code,9,1) ='0';
insert into fact_head_value(sort_id, value)
 -- sort_id 1 (Lợi nhuận trước thuế)
	select 
		1 as sort_id,
		round(sum(value),2) as value
	from 
		fact_head_value
	where sort_id between 7 and 9;
--insert fact_report_tong_hop_raw
--insert sort_id 14->18
	insert into fact_report_tong_hop_raw (month,sort_id,area_name,value)
	select 
		vmonth_key as month,
		14 as sort_id,
		a.area_name as area_name ,
		round(a.amount,2) as value
	from
		((with amount_area as (
		-- Tính amount chưa phân bổ của từng khu vực theo tiêu chí sort_id =14
			select 
				sum(amount) as amount,
				substring(analysis_code,9,1)  as area_code
			from fact_txn_raw_data ftrd 
			where account_code in ('702000030002', '702000030001','702000030102') and extract(month from transaction_date) <vmonth+1 and substring(analysis_code,9,1) <> '0'
			group by substring(analysis_code,9,1)
			),
			amount_head as (
		-- Tính amount của head để phân bổ về khu vực 
			select 
				sum(amount) as amount,
				substring(analysis_code,9,1)  as area_code
			from fact_txn_raw_data ftrd 
			where account_code in ('702000030002', '702000030001','702000030102') and extract(month from transaction_date) <vmonth+1 and substring(analysis_code,9,1) = '0'
			group by substring(analysis_code,9,1)
			),
			sum_amount as (
			select sum( amount) as sum_amount
			from amount_area 
			),
			rate_area as (
			select round(amount / sum_amount,2) as rate,area_code
			from sum_amount,amount_area 
			)
		select 	
			(a.rate*b.amount) + c.amount as amount,d.area_name
		from 
		rate_area a
		join amount_head b on 1=1
		join amount_area c on c.area_code=a.area_code
		join area_code d on d.area_code=a.area_code)) a
	union all 
	(select 
		vmonth_key as month,
		15 as sort_id,
		a.area_name as area_name ,
		round(a.amount,2) as value
	from
	((with amount_area as (
		-- Tính amount chưa phân bổ của từng khu vực theo tiêu chí sort_id =15
		select 
			sum(amount) as amount,
			substring(analysis_code,9,1)  as area_code
		from fact_txn_raw_data ftrd 
		where account_code in ('702000030012', '702000030112') and extract(month from transaction_date) <vmonth+1 and substring(analysis_code,9,1) <> '0'
		group by substring(analysis_code,9,1)
		),
		-- Tính amount của head để phân bổ về khu vực 
		amount_head as (
		select 
			sum(amount) as amount,
			substring(analysis_code,9,1)  as area_code
		from fact_txn_raw_data ftrd 
		where account_code in ('702000030012', '702000030112') and extract(month from transaction_date) <vmonth +1 and substring(analysis_code,9,1) = '0'
		group by substring(analysis_code,9,1)
		),
		sum_amount as (
		select sum( amount) as sum_amount
		from amount_area 
		),
		rate_area as (
		select round(amount / sum_amount,2) as rate,area_code
		from sum_amount,amount_area 
		)
	select 	
		(a.rate*b.amount) + c.amount as amount,d.area_name 
	from 
	rate_area a
	join amount_head b on 1=1
	join amount_area c on c.area_code=a.area_code
	join area_code d on d.area_code=a.area_code)) a)
	union all 
	(select 
		-- Tính amount chưa phân bổ của từng khu vực theo tiêu chí sort_id =16
		vmonth_key as month,
		16 as sort_id,
		a.area_name as area_name ,
		round(a.amount,2) as value
	from
	((with amount_area as (
		select 
			sum(amount) as amount,
			substring(analysis_code,9,1)  as area_code
		from fact_txn_raw_data ftrd 
		where account_code in ('716000000001') and extract(month from transaction_date) <vmonth +1 and substring(analysis_code,9,1) <> '0'
		group by substring(analysis_code,9,1)
		),
		amount_head as (
		-- Tính amount của head để phân bổ về khu vực 
		select 
			sum(amount) as amount,
			substring(analysis_code,9,1)  as area_code
		from fact_txn_raw_data ftrd 
		where account_code in ('716000000001') and extract(month from transaction_date) <vmonth +1 and substring(analysis_code,9,1) = '0'
		group by substring(analysis_code,9,1)
		),
		sum_amount as (
		select sum( amount) as sum_amount
		from amount_area 
		),
		rate_area as (
		select round(amount / sum_amount,2) as rate,area_code
		from sum_amount,amount_area 
		)
	select 	
		(a.rate*b.amount) + c.amount as amount,d.area_name
	from 
	rate_area a
	join amount_head b on 1=1
	join amount_area c on c.area_code=a.area_code
	join area_code d on d.area_code=a.area_code)) a)
	union all 
	(-- Tính amount chưa phân bổ của từng khu vực theo tiêu chí sort_id =17
	select 
		vmonth_key as month,
		17 as sort_id,
		a.area_name as area_name ,
		round(a.amount,2) as value
	from
	((with amount_area as (
		-- Tính amount chưa phân bổ của từng khu vực theo tiêu chí sort_id =17
		select 
			sum(amount) as amount,
			substring(analysis_code,9,1)  as area_code
		from fact_txn_raw_data ftrd 
		where account_code in ('719000030002') and extract(month from transaction_date) <vmonth +1 and substring(analysis_code,9,1) <> '0'
		group by substring(analysis_code,9,1)
		),
		amount_head as (
		-- Tính amount của head để phân bổ về khu vực 
		select 
			sum(amount) as amount,
			substring(analysis_code,9,1)  as area_code
		from fact_txn_raw_data ftrd 
		where account_code in ('719000030002') and extract(month from transaction_date) <vmonth + 1 and substring(analysis_code,9,1) = '0'
		group by substring(analysis_code,9,1)
		),
		sum_amount as (
		select sum( amount) as sum_amount
		from amount_area 
		),
		rate_area as (
		select round(amount / sum_amount,2) as rate,area_code
		from sum_amount,amount_area 
		)
	select 	
		(a.rate*b.amount) + c.amount as amount,d.area_name
	from 
	rate_area a
	join amount_head b on 1=1
	join amount_area c on c.area_code=a.area_code
	join area_code d on d.area_code=a.area_code)) a)
	union all 
	(select 
	
		vmonth_key as month,
		18 as sort_id,
		a.area_name as area_name ,
		round(a.amount,2) as value
	from 
	((with amount_area as (
		-- Tính amount chưa phân bổ của từng khu vực theo tiêu chí sort_id =18
		select 
			sum(amount) as amount,
			substring(analysis_code,9,1)  as area_code
		from fact_txn_raw_data ftrd 
		where account_code in ('719000030003','719000030103','790000030003','790000030103','790000030004','790000030104') and extract(month from transaction_date) < vmonth+1 and substring(analysis_code,9,1) <> '0'
		group by substring(analysis_code,9,1)
		),
		amount_head as (
		-- Tính amount của head để phân bổ về khu vực 
		select 
			sum(amount) as amount,
			substring(analysis_code,9,1)  as area_code
		from fact_txn_raw_data ftrd 
		where account_code in ('719000030003','719000030103','790000030003','790000030103','790000030004','790000030104') and extract(month from transaction_date) <vmonth+1 and substring(analysis_code,9,1) = '0'
		group by substring(analysis_code,9,1)
		),
		sum_amount as (
		select sum( amount) as sum_amount
		from amount_area 
		),
		rate_area as (
		select round(amount / sum_amount,2) as rate,area_code
		from sum_amount,amount_area 
		)
	select 	
		(a.rate*b.amount) + c.amount as amount,d.area_name
	from 
	rate_area a
	join amount_head b on 1=1
	join amount_area c on c.area_code=a.area_code
	join area_code d on d.area_code=a.area_code)) a);
	insert into fact_report_tong_hop_raw (month,sort_id,area_name,value)
	(select
		vmonth_key as month,
		4 as sort_id ,
		area_name,
		round(sum(value),2) as value
	from fact_report_tong_hop_raw
	where sort_id between 14 and 18
	group by area_name);
	insert into fact_report_tong_hop_raw(month,sort_id,area_name,value)
	(select 
		vmonth_key as month,
		26 as sort_id,
		a.area_name as area_name ,
		round(a.amount,2) as value
	from
	((with amount_area as (
		-- Tính amount chưa phân bổ của từng khu vực theo tiêu chí sort_id =26
		select 
			sum(amount) as amount,
			substring(analysis_code,9,1)  as area_code
		from fact_txn_raw_data ftrd 
		where account_code in ('816000000001','816000000002','816000000003') and extract(month from transaction_date) <vmonth+1 and substring(analysis_code,9,1) <> '0'
		group by substring(analysis_code,9,1)
		),
		-- Tính amount của head để phân bổ về khu vực 
		amount_head as (
		select 
			sum(amount) as amount,
			substring(analysis_code,9,1)  as area_code
		from fact_txn_raw_data ftrd 
		where account_code in ('816000000001','816000000002','816000000003') and extract(month from transaction_date) <vmonth +1 and substring(analysis_code,9,1) = '0'
		group by substring(analysis_code,9,1)
		),
		sum_amount as (
		select sum( amount) as sum_amount
		from amount_area 
		),
		rate_area as (
		select round(amount / sum_amount,2) as rate,area_code
		from sum_amount,amount_area 
		)
	select 	
		(a.rate*b.amount) + c.amount as amount,d.area_name
	from 
	rate_area a
	join amount_head b on 1=1
	join amount_area c on c.area_code=a.area_code
	join area_code d on d.area_code=a.area_code)) a)
	union all
	(select 
		vmonth_key as month,
		27 as sort_id,
		a.area_name as area_name ,
		round(a.amount,2) as value
	from
	((with amount_area as (
	-- Tính amount chưa phân bổ của từng khu vực theo tiêu chí sort_id =27
		select 
			sum(amount) as amount,
			substring(analysis_code,9,1)  as area_code
		from fact_txn_raw_data ftrd 
		where account_code in ('809000000002','809000000001','811000000001','811000000102','811000000002','811014000001','811037000001','811039000001','811041000001','815000000001','819000000002','819000000003','819000000001','790000000003','790000050101','790000000101','790037000001','849000000001','899000000003','899000000002','811000000101','819000060001') and extract(month from transaction_date) <vmonth+1 and substring(analysis_code,9,1) <> '0'
		group by substring(analysis_code,9,1)
		),
		---- Tính amount của head để phân bổ về khu vực 
		amount_head as (
		select 
			sum(amount) as amount,
			substring(analysis_code,9,1)  as area_code
		from fact_txn_raw_data ftrd 
		where account_code in ('809000000002','809000000001','811000000001','811000000102','811000000002','811014000001','811037000001','811039000001','811041000001','815000000001','819000000002','819000000003','819000000001','790000000003','790000050101','790000000101','790037000001','849000000001','899000000003','899000000002','811000000101','819000060001') and extract(month from transaction_date) <vmonth +1 and substring(analysis_code,9,1) = '0'
		group by substring(analysis_code,9,1)
		),
		sum_amount as (
		select sum( amount) as sum_amount
		from amount_area 
		),
		rate_area as (
		select round(amount / sum_amount,2) as rate,area_code
		from sum_amount,amount_area 
		)
	select 	
		(a.rate*b.amount) + c.amount as amount,d.area_name
	from 
	rate_area a
	join amount_head b on 1=1
	join amount_area c on c.area_code=a.area_code
	join area_code d on d.area_code=a.area_code)) a);
	insert into fact_report_tong_hop_raw(month,sort_id,area_name,value)
	(select 
		vmonth_key as month,
		25 as sort_id,
		a.area_name as area_name ,
		round(a.amount,2) as value
	from
	((with amount_area as (
		---- Tính amount chưa phân bổ của từng khu vực theo tiêu chí sort_id =25
		select 
			sum(amount) as amount,
			substring(analysis_code,9,1)  as area_code
		from fact_txn_raw_data ftrd 
		where account_code in ('702000010001','702000010002','704000000001','705000000001','709000000001','714000000002','714000000003','714037000001','714000000004','714014000001','715000000001','715037000001','719000000001','709000000101','719000000101') and extract(month from transaction_date) <vmonth+1 and substring(analysis_code,9,1) <> '0'
		group by substring(analysis_code,9,1)
		),
		---- Tính amount của head để phân bổ về khu vực 
		amount_head as (
		select 
			sum(amount) as amount,
			substring(analysis_code,9,1)  as area_code
		from fact_txn_raw_data ftrd 
		where account_code in  ('702000010001','702000010002','704000000001','705000000001','709000000001','714000000002','714000000003','714037000001','714000000004','714014000001','715000000001','715037000001','719000000001','709000000101','719000000101')  and extract(month from transaction_date) <vmonth+1 and substring(analysis_code,9,1) = '0'
		group by substring(analysis_code,9,1)
		),
		sum_amount as (
		select sum( amount) as sum_amount
		from amount_area 
		),
		rate_area as (
		select round(amount / sum_amount,2) as rate,area_code
		from sum_amount,amount_area 
		)
	select 	
		(a.rate*b.amount) + c.amount as amount,d.area_name
	from 
	rate_area a
	join amount_head b on 1=1
	join amount_area c on c.area_code=a.area_code
	join area_code d on d.area_code=a.area_code)) a);
	insert into fact_report_tong_hop_raw (month,sort_id,area_name,value)
	(select
		vmonth_key as month,
		6 as sort_id ,
		area_name,
		round(sum(value),2) as value
	from fact_report_tong_hop_raw
	where sort_id between 25 and 27
	group by area_name);
	insert into fact_report_tong_hop_raw (month,sort_id,area_name,value)
	select 
		vmonth_key as month,
		20 as sort_id,
		area_name as area_name ,
		round(amount,2) as value
	from 
	(with amount_head as (
	---- Tính amount chưa phân bổ của từng khu vực theo tiêu chí sort_id =20
		select 
			sum(amount) as amount,
			substring(analysis_code,9,1)  as area_code
		from fact_txn_raw_data ftrd 
		where account_code in ('801000000001','802000000001') and extract(month from transaction_date) < 2 +1 and substring(analysis_code,9,1) = '0'
		group by substring(analysis_code,9,1)
	),
		rate as (
		select area_code, du_no/sum_du_no as rate
		from fact_temp 
		join 
			(select sum(du_no) as sum_du_no
			 from fact_temp ) on 1=1 
	)
	select b.amount* a.rate as amount , c.area_name
	from rate a
	join amount_head b on 1=1
	join area_code c on c.area_code =a.area_code)
--
	union all 
	(select 
		vmonth_key as month,
		22 as sort_id,
		area_name as area_name ,
		round(amount,2) as value
	from 
	(with amount_head as (
		select 
			sum(amount) as amount,
			substring(analysis_code,9,1)  as area_code
		from fact_txn_raw_data ftrd 
		where account_code in ( '803000000001') and extract(month from transaction_date) <vmonth +1 and substring(analysis_code,9,1) = '0'
		group by substring(analysis_code,9,1)
	),
		rate as (
		select area_code, du_no/sum_du_no as rate
		from fact_temp 
		join 
			(select sum(du_no) as sum_du_no
			 from fact_temp ) on 1=1 
	)
	select b.amount* a.rate as amount , c.area_name
	from rate a
	join amount_head b on 1=1
	join area_code c on c.area_code =a.area_code));
	insert into fact_report_tong_hop_raw (month,sort_id,area_name,value)
	(select
		vmonth_key as month,
		5 as sort_id ,
		area_name,
		round(sum(value),2) as value
	from fact_report_tong_hop_raw
	where sort_id between 20 and 22
	group by area_name);
	insert into fact_report_tong_hop_raw (month,sort_id,area_name,value)
	select 
		vmonth_key as month,
		30 as sort_id,
		area_name as area_name ,
		round(amount,2) as value
	from
	(with amount_area as (
		-- Tính amount chưa phân bổ của từng khu vực theo tiêu chí sort_id =30
		select 
			sum(amount) as amount,
			substring(analysis_code,9,1)  as area_code
		from fact_txn_raw_data ftrd 
		where CAST(account_code AS TEXT) LIKE '85%'  and extract(month from transaction_date) <vmonth +1 and substring(analysis_code,9,1) <> '0'
		group by substring(analysis_code,9,1)
		),
		-- Tính amount của head để phân bổ về khu vực 
		amount_head as (
		select 
			sum(amount) as amount,
			substring(analysis_code,9,1)  as area_code
		from fact_txn_raw_data ftrd 
		where CAST(account_code AS TEXT) LIKE '85%'  and extract(month from transaction_date) <vmonth +1 and substring(analysis_code,9,1) = '0'
		group by substring(analysis_code,9,1)
		),
		count_sale as(
		select count(distinct sale_name) cnt_sale,area_name
		from fact_kpi_data_final
		group by area_name),
		rate as(
			select 
				a.cnt_sale/ b.sum as rate , c.area_code 
			from count_sale a
			join (
				select 
					sum(cnt_sale) 
				from count_sale 
			) b on 1=1
			join area_code c on c.area_name  =a.area_name 
		)	
	select 	
		(a.rate*b.amount) + c.amount as amount,d.area_name
	from 
	rate a
	join amount_head b on 1=1
	join amount_area c on c.area_code=a.area_code
	join area_code d on a.area_code=d.area_code)
	union all
	(select 
		vmonth_key as month,
		31 as sort_id,
		area_name as area_name ,
		round(amount,2) as value
	from
	(with amount_area as (
	-- Tính amount chưa phân bổ của từng khu vực theo tiêu chí sort_id =31
		select 
			sum(amount) as amount,
			substring(analysis_code,9,1)  as area_code
		from fact_txn_raw_data ftrd 
		where CAST(account_code AS TEXT) LIKE '86%'  and extract(month from transaction_date) <vmonth +1 and substring(analysis_code,9,1) <> '0'
		group by substring(analysis_code,9,1)
		),
		-- Tính amount của head để phân bổ về khu vực 
		amount_head as (
		select 
			sum(amount) as amount,
			substring(analysis_code,9,1)  as area_code
		from fact_txn_raw_data ftrd 
		where CAST(account_code AS TEXT) LIKE '86%'  and extract(month from transaction_date) <vmonth +1 and substring(analysis_code,9,1) = '0'
		group by substring(analysis_code,9,1)
		),
		count_sale as(
		select count(distinct sale_name) cnt_sale,area_name
		from fact_kpi_data_final
		group by area_name),
		rate as(
			select 
				a.cnt_sale/ b.sum as rate , c.area_code 
			from count_sale a
			join (
				select 
					sum(cnt_sale) 
				from count_sale 
			) b on 1=1
			join area_code c on c.area_name  =a.area_name 
		)	
	select 	
		(a.rate*b.amount) + c.amount as amount,d.area_name
	from 
	rate a
	join amount_head b on 1=1
	join amount_area c on c.area_code=a.area_code
	join area_code d on d.area_code=a.area_code))
	union all
	(select 
		vmonth_key as month,
		32 as sort_id,
		area_name as area_name ,
		round(amount,2) as value
	from
	(with amount_area as (
	-- Tính amount chưa phân bổ của từng khu vực theo tiêu chí sort_id =31
		select 
			sum(amount) as amount,
			substring(analysis_code,9,1)  as area_code
		from fact_txn_raw_data ftrd 
		where CAST(account_code AS TEXT) LIKE '87%'  and extract(month from transaction_date) <vmonth + 1 and substring(analysis_code,9,1) <> '0'
		group by substring(analysis_code,9,1)
		),
	-- Tính amount của head để phân bổ về khu vực 
		amount_head as (
		select 
			sum(amount) as amount,
			substring(analysis_code,9,1)  as area_code
		from fact_txn_raw_data ftrd 
		where CAST(account_code AS TEXT) LIKE '87%'  and extract(month from transaction_date) < vmonth + 1 and substring(analysis_code,9,1) = '0'
		group by substring(analysis_code,9,1)
		),
		count_sale as(
		select count(distinct sale_name) cnt_sale,area_name
		from fact_kpi_data_final
		group by area_name),
		rate as(
			select 
				a.cnt_sale/ b.sum as rate , c.area_code 
			from count_sale a
			join (
				select 
					sum(cnt_sale) 
				from count_sale 
			) b on 1=1
			join area_code c on c.area_name  =a.area_name 
		)	
	select 	
		(a.rate*b.amount) + c.amount as amount,d.area_name 
	from 
	rate a
	join amount_head b on 1=1
	join amount_area c on c.area_code=a.area_code
	join area_code d on d.area_code=a.area_code));
	insert into fact_report_tong_hop_raw (month,sort_id,area_name,value)
	(select
		vmonth_key as month,
		8 as sort_id ,
		area_name,
		sum(value) as value
	from fact_report_tong_hop_raw
	where sort_id between 30 and 32
	group by area_name);
	insert into fact_report_tong_hop_raw (month,sort_id,area_name,value)
	(select
		vmonth_key as month,
		7 as sort_id ,
		area_name,
		round(sum(value),2) as value
	from fact_report_tong_hop_raw
	where sort_id between 4 and 6
	group by area_name);
	insert into fact_report_tong_hop_raw (month,sort_id,area_name,value)
	select 
		vmonth_key as month,
		9 as sort_id ,
		area_name,
		round(amount,2) as value
	from
	(with amount_area as (
		select 
			sum(amount) as amount,
			substring(analysis_code,9,1)  as area_code
		from fact_txn_raw_data ftrd 
		where account_code in ('790000050001', '882200050001', '790000030001', '882200030001', '790000000001', '790000020101', '882200000001', '882200050101', '882200020101', '882200060001','790000050101', '882200030101') and extract(month from transaction_date) <vmonth +1 and substring(analysis_code,9,1) <> '0'
		group by substring(analysis_code,9,1)
		),
		amount_head as (
		select 
			sum(amount) as amount,
			substring(analysis_code,9,1)  as area_code
		from fact_txn_raw_data ftrd 
		where account_code in ('790000050001', '882200050001', '790000030001', '882200030001', '790000000001', '790000020101', '882200000001', '882200050101', '882200020101', '882200060001','790000050101', '882200030101') and extract(month from transaction_date) <  vmonth +1 and substring(analysis_code,9,1) = '0'
		group by substring(analysis_code,9,1)
		),
		sum_amount as (
		select sum( amount) as sum_amount
		from amount_area 
		),
		rate_area as (
		select round(amount / sum_amount,2) as rate,area_code
		from sum_amount,amount_area 
		)
	select 	
		(a.rate*b.amount) + c.amount as amount,d.area_name
	from 
	rate_area a
	join amount_head b on 1=1
	join amount_area c on c.area_code=a.area_code
	join area_code d on d.area_code=a.area_code);
	insert into fact_report_tong_hop_raw (month,sort_id,area_name,value)
	select 
		vmonth_key as month,
		2 as sort_id ,
		area_name,
		cnt_sale as value
	from 
		(select 
			count(distinct sale_name) cnt_sale,
			b.area_name
		from fact_kpi_data_final a 
		join area_code b on a.area_name=b.area_name
		group by b.area_name);
	insert into fact_report_tong_hop_raw (month,sort_id,area_name,value)
	(select
		vmonth_key as month,
		1 as sort_id ,
		area_name,
		sum(value) as value
	from fact_report_tong_hop_raw
	where sort_id between 7 and 9
	group by area_name);
	insert into fact_report_tong_hop_raw (month,sort_id,area_name,value)
	(select
		vmonth_key as month,
		10 as sort_id,
		a.area_name,
		round(b.amount_id8/a.amount_id7*(-100),2) as value
	from 
		(select 
			value as amount_id7,
			area_name
		from fact_report_tong_hop_raw
		where sort_id = 7) a
	join 
	(select 
			value as amount_id8,
			area_name
		from fact_report_tong_hop_raw
		where sort_id = 8) b  on a.area_name= b.area_name 
	union all 
	(select
		vmonth_key as month,
		11 as sort_id,
		a.area_name,
		round(a.amount_id1/(b.amount_id4+c.amount_id25)*(100),2) as value
	from 
		(select 
			value as amount_id1,
			area_name
		from fact_report_tong_hop_raw
		where sort_id = 1) a
	join 
	(select 
			value as amount_id4,
			area_name
		from fact_report_tong_hop_raw
		where sort_id = 4) b  on a.area_name=b.area_name
	join 
	(select 
			value as amount_id25,
			area_name
		from fact_report_tong_hop_raw
		where sort_id = 25) c  on a.area_name=c.area_name)
	union all 
	(select
		vmonth_key as month,
		12 as sort_id,
		a.area_name,
		round(a.amount_id1/b.amount_id5*(-100),2) as value
	from 
		(select 
			value as amount_id1,area_name
		from fact_report_tong_hop_raw
		where sort_id = 1) a
	join 
	(select 
			value as amount_id5,
			area_name
		from fact_report_tong_hop_raw 
		where sort_id = 5) b  on a.area_name=b.area_name)
	union all 
	(select
		vmonth_key as month,
		13 as sort_id,
		a.area_name,
		round(a.amount_id1/b.amount_id2,2) as value
	from 
		(select 
			value as amount_id1,
			area_name
		from fact_report_tong_hop_raw
		where sort_id = 1) a
	join 
	(select 
			value as amount_id2,
			area_name
		from fact_report_tong_hop_raw
		where sort_id = 2) b  on a.area_name=b.area_name));
------
-- 
	insert into  fact_report_tong_hop (sort_id)
	select 
		distinct sort_id
	from fact_report_tong_hop_raw ;
-- insert bang fact_report_tong_hop
	for i in 1 .. array_length(area_arr,1)
	loop
		for j in 1.. sort_id_max
			loop
				query :=
					format('
						update fact_report_tong_hop b
						set %I=
							(select 
								value
							from fact_report_tong_hop_raw a
							where a.sort_id= %L and a.area_name= %L)
						where b.sort_id = %L
							',area_arr[i],j,area_name_arr[i],j);
				execute query;
			end loop;
	end loop;
	insert into  fact_report_tong_hop_detail
	select 
		b.information,
		c.value as Head,
		a.dong_bac_bo,
		a.tay_bac_bo,
		a.db_song_hong,
		a.bac_trung_bo,
		a.nam_trung_bo,
		a.tay_nam_bo,
		a.dong_nam_bo
	from fact_report_tong_hop a 
	join dim_report_tong_hop b on a.sort_id=b.sort_id 
	left join fact_head_value c on c.sort_id =a.sort_id ;
--insert bang fact_report_xh 
	INSERT INTO fact_report_xh(
    month_key, area_code, area_name, email, tong_diem, rank_final, 
    ltn_avg, rank_ltn_avg, psdn_avg, rank_psdn_avg, approval_rate_avg, 
    rank_approval_rate_avg, npl_truoc_wo_luy_ke, rank_npl_truoc_wo_luy_ke, 
    diem_quy_mo, rank_ptkd, cir, rank_cir, margin, rank_margin, hs_von, 
    rank_hs_von, hsbq_nhan_su, rank_hsbq_nhan_su, diem_fin, rank_fin
	)
	SELECT 
    month_key, area_code, area_name, email, tong_diem, rank_final, 
    ltn_avg, rank_ltn_avg, psdn_avg, rank_psdn_avg, approval_rate_avg, 
    rank_approval_rate_avg, npl_truoc_wo_luy_ke, rank_npl_truoc_wo_luy_ke, 
    diem_quy_mo, rank_ptkd, cir, rank_cir, margin, rank_margin, hs_von, 
    rank_hs_von, hsbq_nhan_su, rank_hsbq_nhan_su, diem_fin, rank_fin
	from 	
	(select 
		a.*,
		diem_quy_mo + diem_fin as tong_diem,
		rank() over(order by diem_quy_mo + diem_fin) as rank_final
	from 
		(select 
				a.* ,
				rank() over(order by diem_quy_mo) as rank_ptkd,
				b.value as cir,
				b.rank_cir as rank_cir,
				c.value as margin,
				c.rank_margin as rank_margin,
				d.value as hs_von,
				d.rank_hs_von as rank_hs_von,
				e.value as hsbq_nhan_su,
				e.rank_hsbq_nhan_su as rank_hsbq_nhan_su ,
				rank_cir + rank_margin + rank_hs_von + rank_hsbq_nhan_su as diem_fin,
				rank() over(order by rank_cir + rank_margin + rank_hs_von + rank_hsbq_nhan_su) as rank_fin
						/*as rank_ptkd
						as cir,
						as rank_cir,
						as margin,
						as rank_margin,
						as hs_von, 
						as rank_hs_von,
						as hsbq_nhan_su,
						as diem_fin,
						as rank_fin*/
				from 
					(select 
						--vmonth_key as month_key,
						vmonth_key as month_key,
						a.area_code as area_code,
						a.area_name as area_name,
						a.email as email,
						--as tong_diem,
						--as rank_final,
						b.ltn_avg as ltn_avg,
						b.rank_ltn_avg as rank_ltn_avg,
						c.psdn_avg as psdn_avg,
						c.rank_psdn_avg as rank_psdn_avg,
						d.approval_rate_avg  as approval_rate_avg,
						d.rank_approval_rate_avg as rank_approval_rate_avg,
						e.rate as npl_truoc_wo_luy_ke,
						rank() over(order by rate ) as rank_npl_truoc_wo_luy_ke, 
						b.rank_ltn_avg + c.rank_psdn_avg + d.rank_approval_rate_avg + rank() over(order by rate ) as diem_quy_mo
					from 
						(select distinct 
							a.email,
							a.area_name,
							b.area_code 
						from fact_kpi_data_final a
						join area_code b on a.area_name = b.area_name ) a
					join 
						(select
							ltn_avg,
							rank() over(order by ltn_avg desc) as rank_ltn_avg,
							email,
							area_name
						from 
							(select 
								avg(loan_to_new) as ltn_avg,
								email,
								area_name
							 from fact_kpi_data_final 
							 where month < vmonth +1
							 group by sale_name, email,area_name
							)
						)  b on a.email=b.email and a.area_name=b.area_name 
					join 
						(select
							psdn_avg,
							rank() over(order by psdn_avg desc) as rank_psdn_avg,
							email,
							area_name
						from 
							(select 
								avg(psdn) as psdn_avg,
								email,
								area_name
							 from fact_kpi_data_final 
							 where month <vmonth +1
							 group by sale_name, email,area_name
							)
						)  c on a.email=c.email and a.area_name=c.area_name 
					join 
						(select
							approval_rate_avg ,
							rank() over(order by approval_rate_avg  desc) as rank_approval_rate_avg ,
							email,
							area_name
						from 
							(select 
								avg(app_rate) as approval_rate_avg,
								email,
								area_name
							 from fact_kpi_data_final 
							 where month < vmonth +1
							 group by sale_name, email,area_name
							)
						)  d on a.email=d.email and a.area_name=d.area_name 
					join 
						fact_temp e on e.area_code =a.area_code) a
				join 
					(select
						value, 
						area_name,
						rank() over(order by value) as rank_cir
					from fact_report_tong_hop_raw
					where sort_id=10) b on b.area_name=a.area_name
				join 
					(select
						value, 
						area_name,
						rank() over(order by value desc) as rank_margin
					from fact_report_tong_hop_raw
					where sort_id=11) c on c.area_name=a.area_name
				join 
					(select
						value, 
						area_name,
						rank() over(order by value desc) as rank_hs_von
					from fact_report_tong_hop_raw
					where sort_id=12) d on d.area_name=a.area_name
					join 
					(select
						value, 
						area_name,
						rank() over(order by value desc) as rank_hsbq_nhan_su
					from fact_report_tong_hop_raw
					where sort_id=13) e on e.area_name=a.area_name) a);
end;
$$;
-- Declare variables and array
DO $$ 
DECLARE
    m int;
    n int;
    sort_id_array integer[];
    area_array varchar[];
begin
	drop table   save_value ;
	create table save_value (	
			month int,
			area varchar
	);
    -- Initialize the arrays with distinct sort_id and area_name values
    sort_id_array := (SELECT ARRAY(SELECT DISTINCT sort_id FROM fact_report_tong_hop_raw ORDER BY sort_id ASC));
    area_array := (SELECT ARRAY(SELECT DISTINCT area_name FROM fact_report_tong_hop_raw ORDER BY area_name ASC));

    -- Insert into save_value table
    INSERT INTO save_value (month, area)
    SELECT DISTINCT month - 202300 AS month,
                    area_name AS area
    FROM fact_report_tong_hop_raw;

    -- Loop through the sort_id_array to alter the table and add columns
    FOR n IN 1 .. array_length(sort_id_array, 1)
    LOOP 
        EXECUTE format('
            ALTER TABLE save_value 
            ADD COLUMN sort_id_%s numeric', sort_id_array[n]);

        -- Loop through the area_array to insert values into the new columns
        FOR m IN 1 .. array_length(area_array, 1)
        LOOP
            EXECUTE format('
                UPDATE save_value
                SET sort_id_%s = (SELECT value 
                                   FROM fact_report_tong_hop_raw 
                                   WHERE sort_id = %L AND area_name = %L)
                WHERE area = %L', 
                sort_id_array[n], 
                sort_id_array[n], 
                area_array[m], 
                area_array[m]);
        END LOOP;
    END LOOP;
END $$;
call report_monthly(2);
