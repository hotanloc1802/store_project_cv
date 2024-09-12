create table fact_dp_customer(
	process_dt date,
	account_id int,
	account_type varchar,
	customer_type varchar,
	interest_rate decimal,
	vof_rate decimal,
	balance bigint,
	open_date date,
	maturity_date date,
	close_dtae date,
	day_key int,
	funding_id smallint
)
update fact_dp_customer 
set day_key= extract(year from process_dt)*1000 + extract(DoY from process_dt)
create table fact_ln_customer(
	process_dt date,
	account_id int,
	account_type varchar,
	customer_id int,
	customer_type varchar,
	interest_rate decimal,
	cof_rate decimal,
	balance bigint,
	open_date date,
	maturity_date date,
	close_date date,
	day_key int,
	funding_id smallint
)
update fact_ln_customer 
set day_key= extract(year from process_dt)*1000 + extract(DoY from process_dt)

create or replace function find_dp_funding_id(open_date date, maturity_date date)
returns int as $$
declare
	diff_day int;
	diff_month int;
begin 
	if maturity_date is null then return 7;
	else 
		diff_day := maturity_date - open_date;
		if diff_day <31 then return 8;
		else 
			diff_month := extract(year from age(maturity_date,open_date))*12 + extract(month from age(maturity_date,open_date));
			case 
				when diff_month between 1 and 5 then return 9;
				when diff_month = 6 then return 10;
				when diff_month = 7 then return 11;
				when diff_month between 8 and 11 then return 12;
				when diff_month = 12 then return 13;
				when diff_month = 13 then return 14;
				when diff_month between 14 and 23 then return 15;
				else return 16;
			end case;
		end if;	
	end if;
end
$$  LANGUAGE plpgsql
update fact_dp_customer 
set funding_id = find_dp_funding_id (open_date, maturity_date)
create or replace function finding_ln_funding_id(open_date date, maturity_date date, account_type varchar)
returns int as $$
declare 
	month_diff int;
begin 
	if account_type = 'CV CCSTK' then return 20;
	elsif account_type = 'CV VND LS USD' then return 21;
	else 
		month_diff := extract(year from age(maturity_date,open_date))*12 + extract( month from age(maturity_date,open_date));
		case 
			when month_diff < 12  then return 22;
			when month_diff <= 60 then return 23;
			when month_diff <= 120 then return 24;
			when month_diff <= 180 then return 25;
			else return 26;
		end case;
	end if;
end
$$  LANGUAGE plpgsql
update fact_ln_customer 
set funding_id = finding_ln_funding_id(open_date, maturity_date, account_type);
create or replace procedure fact_summary_funding_daily_prc(vDate date default null)
LANGUAGE plpgsql
AS $$
declare 
	vProcess_dt date;
	vDaykey int;
	vBeginmonth_Daykey int ;
begin 
	-- ---------------------
    -- THÔNG TIN NGƯỜI TẠO
    -- ---------------------
    -- Tên người tạo: loc_ho
    -- Ngày tạo: 2024-Apr-28
        -- Mục đích : Tổng hợp các tiêu chí nguồn vốn và sử dụng vốn theo ngày

    -- ---------------------
    -- THÔNG TIN NGƯỜI CẬP NHẬT
    -- ---------------------
    -- Tên người cập nhật: 
    -- Ngày cập nhật: 
    -- Mục đích cập nhật: 

    -- ---------------------
    -- SUMMARY LUỒNG XỬ LÝ
    -- ---------------------
    -- Bước 1: Kiểm tra nếu ngày truyền vào là null sẽ lấy vProcess_dt = ngày hiện tại - 1
        -- ngược lại vProcess_dt = vDate
    -- Bước 2: xoá dữ liệu fact_summary_funding_daily tại ngày vProcess_dt
    -- Bước 3: insert dữ liệu tiêu chí nguồn vốn vào bảng
    -- Bước 4: insert dữ liệu tiêu chí nguồn sử dụng vốn vào bảng
    -- Bước 5: Xử lý ngoại lệ và ghi log (nếu cần)

    -- ---------------------
    -- CHI TIẾT CÁC BƯỚC
    -- ---------------------
	-- Bước 1: Kiểm tra nếu ngày truyền vào là null sẽ lấy vProcess_dt = ngày hiện tại - 1
	if vDate is null then 
		vProcess_dt := current_date - 1;
	else 
		vProcess_dt := vDate;	
	end if;
	vDaykey := extract(year from vProcess_dt)*1000 + extract( DoY from vProcess_dt);
	vBeginmonth_Daykey := extract( year from date_trunc('month',vProcess_dt))*1000 + extract(DoY from date_trunc('month',vProcess_dt));
	-- Bước 2: xoá dữ liệu fact_summary_funding_daily tại ngày vProcess_dt
	delete from fact_summary_funding_daily where day_key=vDaykey;
	-- Bước 3: insert dữ liệu tiêu chí nguồn vốn vào bảng
	insert into fact_summary_funding_daily 
		(day_key,process_dt,funding_id,interest_rate,vof_rate,cof_rate,balance,avg_balance_month)
		select
			--vDaykey as day_key,
			--vProcess_dt as process_dt,
			2023349 as day_key,
			to_date('20231215','YYYYMMDD') as process_dt,
			x.funding_id,
			x.interest_rate,
			x.vof_rate,
			0 as cof_rate,
			x.balance,
			y.avg_balance_month 
		from 
			(select 
				funding_id,
				round(cast(avg(interest_rate) as decimal),2) as interest_rate,
				round(cast(avg(vof_rate) as decimal),2) as vof_rate,
				sum(balance) as balance 
				from fact_dp_customer
				where day_key = 2023349
				group by funding_id		
			)x
		left join 
			(select 
				funding_id,
				round(cast(avg(balance) as decimal),2) as avg_balance_month 
			from 
				(select 
					day_key,
					funding_id,
					sum(balance) as balance
				from fact_dp_customer
				where day_key between 2023335 and 2023349
				group by day_key,funding_id)
			group by funding_id) y on x.funding_id =y.funding_id 
		-- Phan loai theo loai hinh
		union all 
			select 
				--vDaykey as day_key,
				--vProcess_dt as process_dt,
				2023349 as day_key,
				to_date('20231215','YYYYMMDD') as process_dt,
				x.funding_id,
				x.interest_rate,
				x.vof_rate,
				0 as cof_rate,
				x.balance,
				y.avg_balance_month 
			from(
				select 
					case 
						when account_type ='Business' then 17
						else 18
					end as funding_id,
					round(cast(avg(interest_rate) as decimal),2) as interest_rate,
					round(cast(avg(vof_rate) as decimal),2) as vof_rate,
					sum(balance) as balance
				from 
					fact_dp_customer 
				where day_key= 2023349
				group by 
					case 
						when account_type ='Business' then 17
						else 18
					end 
			) x 
			left join 
			(
				select 
					funding_id,
					round(cast(avg(balance) as decimal),2) as avg_balance_month
				from 
					(select 
						case 
						when account_type ='Business' then 17
						else 18
						end as funding_id,
						day_key,
						sum(balance) as balance 
						from fact_dp_customer 
						where day_key between 2023335 and 2023349
						group by day_key, 
						case 
							when account_type ='Business' then 17
							else 18
						end 
					)
				group by funding_id
			) y on x.funding_id =y.funding_id 
			-- Du no 
		union all 
			select 
				--vDaykey as day_key,
				--vProcess_dt as process_dt,
				2023349 as day_key,
				to_date('20231215','YYYYMMDD') as process_dt,
				x.funding_id,
				x.interest_rate,
				0 as vof_rate,
				x.cof_rate,
				x.balance,
				y.avg_balance_month 
			from (
				select 
					funding_id,
					round(cast(avg(interest_rate) as decimal),2) as interest_rate,
					round(cast(avg(cof_rate) as decimal),2) as cof_rate,
					sum(balance) as balance 
					from fact_ln_customer
					where day_key = 2023349
					group by funding_id	
			) x
			left join 
			(select 
				funding_id,
				round(cast(avg(balance) as decimal),2) as avg_balance_month 
			from 
				(select 
					day_key,
					funding_id,
					sum(balance) as balance
				from fact_ln_customer
				where day_key between 2023335 and 2023349
				group by day_key,funding_id)
			group by funding_id) y on x.funding_id =y.funding_id 
			union all 
			select 
				--vDaykey as day_key,
				--vProcess_dt as process_dt,
				2023349 as day_key,
				to_date('20231215','YYYYMMDD') as process_dt,
				x.funding_id,
				x.interest_rate,
				0 as vof_rate,
				x.cof_rate,
				x.balance,
				y.avg_balance_month 
			from(
				select 
					case 
						when customer_type ='Business' then 32
						else 33
					end as funding_id,
					round(cast(avg(interest_rate) as decimal),2) as interest_rate,
					round(cast(avg(cof_rate) as decimal),2) as cof_rate,
					sum(balance) as balance
				from 
					fact_ln_customer 
				where day_key= 2023349
				group by 
					case 
						when customer_type ='Business' then 32
						else 33
					end 
			) x 
			left join 
			(
				select 
					funding_id,
					round(cast(avg(balance) as decimal),2) as avg_balance_month
				from 
					(select 
						case 
						when customer_type  ='Business' then 32
						else 33
						end as funding_id,
						day_key,
						sum(balance) as balance 
						from fact_ln_customer 
						where day_key between 2023335 and 2023349
						group by day_key, 
						case 
							when customer_type  ='Business' then 32
							else 33
						end 
					)
				group by funding_id
			) y on x.funding_id =y.funding_id 
			union all 
			select 
				--vDaykey as day_key,
				--vProcess_dt as process_dt,
				2023349 as day_key,
				to_date('20231215','YYYYMMDD') as process_dt,
				1 as funding_id,
				x.interest_rate,
				x.vof_rate,
				0 as cof_rate,
				x.balance ,
				y.avg_balance_month 
			from 
				(select
					round(cast(avg(interest_rate) as decimal),2) as interest_rate,
					round(cast(avg(vof_rate) as decimal),2) as vof_rate,
					sum(balance) as balance 
				from fact_dp_customer 
				where day_key= 2023349
				)	x
			left join 
				(select 
					round(cast(avg(balance) as decimal),2) as avg_balance_month 
				from 
					(select
						sum(balance) as balance,
						day_key
					from 
						fact_dp_customer
					where day_key between 2023335 and 2023349
					group by day_key
					)	
			) y on 1=1
			union all
			select 
				--vDaykey as day_key,
				--vProcess_dt as process_dt,
				2023349 as day_key,
				to_date('20231215','YYYYMMDD') as process_dt,
				2 as funding_id,
				x.interest_rate,
				0 as vof_rate,
				x.cof_rate,
				x.balance ,
				y.avg_balance_month 
			from 
				(select
					round(cast(avg(interest_rate) as decimal),2) as interest_rate,
					round(cast(avg(cof_rate) as decimal),2) as cof_rate,
					sum(balance) as balance 
				from fact_ln_customer 
				where day_key= 2023349
				)	x
			left join 
				(select 
					round(cast(avg(balance) as decimal),2) as avg_balance_month 
				from 
					(select
						sum(balance) as balance,
						day_key
					from 
						fact_ln_customer
					where day_key between 2023335 and 2023349
					group by day_key
					)	
			) y on 1=1;
insert into fact_summary_funding_daily 
		(day_key,process_dt,funding_id,interest_rate,vof_rate,cof_rate,balance,avg_balance_month)
			select 
				2023349 as day_key,
				to_date('20231215','YYYYMMDD') as process_dt,
				3 as funding_id,
				x.interest_rate,
				x.vof_rate,
				0 as cof_rate,
				x.balance,
				x.avg_balance_month 
			from fact_summary_funding_daily x
			where funding_id = 1 		
			union all
			select 
				2023349 as day_key,
				to_date('20231215','YYYYMMDD') as process_dt,
				4 as funding_id,
				x.interest_rate,
				x.vof_rate,
				0 as cof_rate,
				x.balance,
				x.avg_balance_month 
			from fact_summary_funding_daily x
			where funding_id = 1
			union all 
			select 
				2023349 as day_key,
				to_date('20231215','YYYYMMDD') as process_dt,
				5 as funding_id,
				x.interest_rate,
				0 as vof_rate,
				x.cof_rate,
				x.balance,
				x.avg_balance_month 
			from fact_summary_funding_daily x
			where funding_id = 2
			union all 
			select 
				2023349 as day_key,
				to_date('20231215','YYYYMMDD') as process_dt,
				6 as funding_id,
				x.interest_rate,
				0 as vof_rate,
				x.cof_rate,
				x.balance,
				x.avg_balance_month 
			from fact_summary_funding_daily x
			where funding_id = 2;
 -- Bước 6: Xử lý ngoại lệ và ghi log (nếu cần)
   -- EXCEPTION
        --WHEN others THEN
            -- Xử lý ngoại lệ ở đây
            -- Có thể ghi log hoặc xử lý các tình huống đặc biệt
            -- THÊM CÓ THỂ: RAISE; -- Tùy chọn để re-raise ngoại lệ
END;
$$;
call fact_summary_funding_daily_prc(to_date('2`','YYYYMMDD'))   