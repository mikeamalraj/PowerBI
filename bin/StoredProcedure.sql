CREATE PROCEDURE Get()
BEGIN
DECLARE bDone INT DEFAULT FALSE;
DECLARE VAR_MDN BIGINT;
DECLARE VAR_DATA BIGINT;
DECLARE VAR_SMS BIGINT;
DECLARE VAR_VOICE BIGINT;
DECLARE VAR_DATE_T DATE;
DECLARE CURRENT_BILL_CYCLE_DAY INT;
DECLARE CURRENT_BILL_CYCLE_DATE DATE;
DECLARE PREVIOUS_BILL_CYCLE_DATE DATE;
DECLARE NEXT_BILL_CYCLE_DATE DATE;
DECLARE curs CURSOR FOR SELECT MDN,Date FROM temp_usagedata group by MDN,date order by MDN,date;
DECLARE CONTINUE HANDLER FOR NOT FOUND SET bDone = TRUE;
drop table if exists dailyusagedata_final;
create table temp_usagedata as select MDN,STR_TO_DATE(Date,'%m/%d/%Y')as Date,Usage_Type,sum(Usage_Count) as Usage_Count_Sum from dailyusagedata group by MDN,Date,Usage_Type order by MDN,date;
create table if not exists dailyusagedata_final(MDN bigint(20),Previous_Bill_Cycle_Date date,Next_Bill_Cycle_Date date,Data_usage bigint(20),Sms_usage int,Voice_usage bigint(15),Your_Calling_Plan varchar(500));
OPEN curs;
cursor_loop:LOOP
FETCH curs INTO VAR_MDN,VAR_DATE_T;
IF bDone THEN
LEAVE cursor_loop;
END IF;
IF(SELECT EXISTS (SELECT 1 FROM dailyusagedata_final where MDN=VAR_MDN)) THEN
	select Previous_Bill_Cycle_Date from dailyusagedata_final where MDN=VAR_MDN into PREVIOUS_BILL_CYCLE_DATE;
	select Next_Bill_Cycle_Date from dailyusagedata_final where MDN=VAR_MDN into NEXT_BILL_CYCLE_DATE;
	IF (VAR_DATE_T>PREVIOUS_BILL_CYCLE_DATE AND VAR_DATE_T<=NEXT_BILL_CYCLE_DATE) THEN
		select max(case when Usage_Type ='D' then Usage_Count_Sum else 0 end) from temp_usagedata where MDN=VAR_MDN and Date=VAR_DATE_T group by MDN into VAR_DATA;
		select max(case when Usage_Type ='S' then Usage_Count_Sum else 0 end) from temp_usagedata where MDN=VAR_MDN and Date=VAR_DATE_T group by MDN into VAR_SMS;
		select max(case when Usage_Type ='V' then Usage_Count_Sum else 0 end) from temp_usagedata where MDN=VAR_MDN and Date=VAR_DATE_T group by MDN into VAR_VOICE;
		IF (VAR_DATA!=0 and VAR_DATA is Not Null) THEN
			update dailyusagedata_final set Data_usage = (Data_usage + VAR_DATA) where MDN=VAR_MDN;
		END IF;
		IF (VAR_SMS!=0 and VAR_SMS is Not Null) THEN
			update dailyusagedata_final set Sms_usage = (Sms_usage + VAR_SMS) where MDN=VAR_MDN;
		END IF;
		IF (VAR_VOICE!=0 and VAR_VOICE is Not Null) THEN
			update dailyusagedata_final set Voice_usage = (Voice_usage + VAR_VOICE) where MDN=VAR_MDN;
		END IF;
	ELSE
		IF (VAR_DATE_T>NEXT_BILL_CYCLE_DATE) THEN
			DELETE FROM dailyusagedata_final where MDN=VAR_MDN;
			SET PREVIOUS_BILL_CYCLE_DATE = NEXT_BILL_CYCLE_DATE;
			SELECT DATE_ADD(PREVIOUS_BILL_CYCLE_DATE,INTERVAL 1 MONTH) into NEXT_BILL_CYCLE_DATE;
			insert dailyusagedata_final select tbl.MDN,PREVIOUS_BILL_CYCLE_DATE,NEXT_BILL_CYCLE_DATE,
			max(case when tbl.Usage_Type ='D' then tbl.Usage_Count_Sum else 0 end) AS Data_usage,
			max(case when tbl.Usage_Type ='S' then tbl.Usage_Count_Sum else 0 end) AS Sms_usage,
			max(case when tbl.Usage_Type ='V' then tbl.Usage_Count_Sum else 0 end) AS Voice_usage,
			acs.Your_Calling_Plan
			from temp_usagedata tbl join account_wireless_summary acs on tbl.MDN=cast(replace(acs.Wireless_Number,'-','') as unsigned) where acs.Wireless_Number <> 'N/A' and (tbl.MDN=VAR_MDN and tbl.Date=VAR_DATE_T)
			group by tbl.MDN;
		END IF;
	END IF;
ELSE
	select max(acs.Bill_Cycle_Date) from account_wireless_summary acs join temp_usagedata tbl on tbl.MDN=cast(replace(acs.Wireless_Number,'-','') as unsigned) where acs.Wireless_Number <> 'N/A' and tbl.MDN=VAR_MDN into CURRENT_BILL_CYCLE_DATE;
	select DAY(CURRENT_BILL_CYCLE_DATE) into CURRENT_BILL_CYCLE_DAY;
	SELECT DATE_SUB(DATE_ADD(CURDATE(),INTERVAL CURRENT_BILL_CYCLE_DAY DAY),INTERVAL DAY(CURDATE()) DAY) into NEXT_BILL_CYCLE_DATE;
	SELECT DATE_SUB(NEXT_BILL_CYCLE_DATE,INTERVAL 1 MONTH) into PREVIOUS_BILL_CYCLE_DATE;
	IF (VAR_DATE_T>PREVIOUS_BILL_CYCLE_DATE AND VAR_DATE_T<=NEXT_BILL_CYCLE_DATE) THEN
		insert dailyusagedata_final select tbl.MDN,PREVIOUS_BILL_CYCLE_DATE,NEXT_BILL_CYCLE_DATE,
		max(case when tbl.Usage_Type ='D' then tbl.Usage_Count_Sum else 0 end) AS Data_usage,
		max(case when tbl.Usage_Type ='S' then tbl.Usage_Count_Sum else 0 end) AS Sms_usage,
		max(case when tbl.Usage_Type ='V' then tbl.Usage_Count_Sum else 0 end) AS Voice_usage,
		acs.Your_Calling_Plan
		from temp_usagedata tbl join account_wireless_summary acs on tbl.MDN=cast(replace(acs.Wireless_Number,'-','') as unsigned) where acs.Wireless_Number <> 'N/A' and (tbl.MDN=VAR_MDN and tbl.Date=VAR_DATE_T)
		group by tbl.MDN;
	END IF;
END IF;
SET VAR_MDN = 0;
SET VAR_DATA = 0;
SET VAR_SMS = 0;
SET VAR_VOICE = 0;
END LOOP cursor_loop;
CLOSE curs;
drop table temp_usagedata;
END