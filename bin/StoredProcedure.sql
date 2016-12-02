	DROP PROCEDURE IF EXISTS Get;
	CREATE PROCEDURE Get()
	BEGIN
		
		DECLARE VAR_MDN BIGINT;
		DECLARE VAR_DATE VARCHAR(10);
		DECLARE VAR_ACCNO BIGINT;
		
		DECLARE VAR_DATA_DOMESTIC BIGINT;
		DECLARE VAR_DATA_ROAMING BIGINT;
		DECLARE VAR_DATA_INTER BIGINT;
		DECLARE VAR_SMS_DOMESTIC INT;
		DECLARE VAR_SMS_ROAMING INT;
		DECLARE VAR_SMS_INTER INT;
		DECLARE VAR_VOICE_DOMESTIC INT;
		DECLARE VAR_VOICE_ROAMING INT;
		DECLARE VAR_VOICE_INTER INT;
		
		DECLARE BILL_CYCLE_DAY INT;
		DECLARE CURRENT_BILL_CYCLE_DATE DATE;
		DECLARE P_BILL_CYCLE_DATE DATE;
		DECLARE N_BILL_CYCLE_DATE DATE;
		
		DECLARE bDone INT DEFAULT FALSE;
		DECLARE curs CURSOR FOR SELECT MDN,Acct_No,Date FROM dailyusagedata GROUP BY MDN,date ORDER BY MDN,date;
		DECLARE curs2 CURSOR FOR SELECT MDN,Date FROM DailyTemp GROUP BY MDN,date ORDER BY MDN,date;
		DECLARE CONTINUE HANDLER FOR NOT FOUND SET bDone = TRUE;
		
		DROP TABLE IF EXISTS dailytemp;
		DROP TABLE IF EXISTS dailyusagedata_final;
		
		CREATE TABLE IF NOT EXISTS DailyTemp (MDN bigint(20),Acct_No int(10),Date date,Data_usage_D bigint(20),Data_usage_R bigint(20),Data_usage_I bigint(20),Sms_usage_D int,Sms_usage_R int,Sms_usage_I int,Voice_usage_D bigint(15),Voice_usage_R bigint(15),Voice_usage_I bigint(15));
		CREATE TABLE IF NOT EXISTS dailyusagedata_final (MDN bigint(20),Acct_No int(10),Date date,Previous_Bill_Cycle_Date date,Next_Bill_Cycle_Date date,Data_usage_D bigint(20),Data_usage_R bigint(20),Data_usage_I bigint(20),Sms_usage_D int,Sms_usage_R int,Sms_usage_I int,Voice_usage_D bigint(15),Voice_usage_R bigint(15),Voice_usage_I bigint(15),Your_Calling_Plan varchar(500));
		
		OPEN curs;
		CURSOR_LOOP:LOOP
		
			FETCH curs INTO VAR_MDN,VAR_ACCNO,VAR_DATE;
			
			IF bDone THEN
				LEAVE CURSOR_LOOP;
			END IF;
			
			INSERT INTO dailytemp (MDN,Acct_No,Date,Data_usage_D,Data_usage_R,Data_usage_I,Sms_usage_D,Sms_usage_R,Sms_usage_I,Voice_usage_D,Voice_usage_R,Voice_usage_I) values
			(VAR_MDN,VAR_ACCNO,STR_TO_DATE(VAR_DATE,'%m/%d/%Y'),
			(SELECT SUM(Usage_Count) FROM dailyusagedata WHERE (Overseas_Ind <> 'Y' AND Roaming_Indicator<>'Y' AND Usage_Type ='D' AND MDN=VAR_MDN AND Date=VAR_DATE)),
			(SELECT SUM(Usage_Count) FROM dailyusagedata WHERE (Roaming_Indicator='Y' AND Usage_Type ='D' AND MDN=VAR_MDN AND Date=VAR_DATE)),
			(SELECT SUM(Usage_Count) FROM dailyusagedata WHERE (Overseas_Ind = 'Y' AND Usage_Type ='D' AND MDN=VAR_MDN AND Date=VAR_DATE)),
			(SELECT SUM(Usage_Count) FROM dailyusagedata WHERE (Overseas_Ind <> 'Y' AND Roaming_Indicator<>'Y' AND Usage_Type ='S' AND MDN=VAR_MDN AND Date=VAR_DATE)),
			(SELECT SUM(Usage_Count) FROM dailyusagedata WHERE (Roaming_Indicator='Y' AND Usage_Type ='S' AND MDN=VAR_MDN AND Date=VAR_DATE)),
			(SELECT SUM(Usage_Count) FROM dailyusagedata WHERE (Overseas_Ind = 'Y' AND Usage_Type ='S' AND MDN=VAR_MDN AND Date=VAR_DATE)),
			(SELECT SUM(Usage_Count) FROM dailyusagedata WHERE (Overseas_Ind <> 'Y' AND Roaming_Indicator<>'Y' AND Usage_Type ='V' AND MDN=VAR_MDN AND Date=VAR_DATE)),
			(SELECT SUM(Usage_Count) FROM dailyusagedata WHERE (Roaming_Indicator='Y' AND Usage_Type ='V' AND MDN=VAR_MDN AND Date=VAR_DATE)),
			(SELECT SUM(Usage_Count) FROM dailyusagedata WHERE (Overseas_Ind = 'Y' AND Usage_Type ='V' AND MDN=VAR_MDN AND Date=VAR_DATE)));
			
		END LOOP CURSOR_LOOP;
		CLOSE curs;
		
		SET bDone = FALSE;
		
		OPEN curs2;
		CURSOR_LOOP2:LOOP
		
			FETCH curs2 INTO VAR_MDN,VAR_DATE;
			
			IF bDone THEN
				LEAVE CURSOR_LOOP2;
			END IF;
			
			SELECT CASE WHEN Data_usage_D <> 'NULL' THEN Data_usage_D ELSE 0 END FROM dailytemp WHERE MDN=VAR_MDN AND Date=VAR_DATE INTO VAR_DATA_DOMESTIC;
			SELECT CASE WHEN Data_usage_R <> 'NULL' THEN Data_usage_R ELSE 0 END FROM dailytemp WHERE MDN=VAR_MDN AND Date=VAR_DATE INTO VAR_DATA_ROAMING;
			SELECT CASE WHEN Data_usage_I <> 'NULL' THEN Data_usage_I ELSE 0 END FROM dailytemp WHERE MDN=VAR_MDN AND Date=VAR_DATE INTO VAR_DATA_INTER;
			SELECT CASE WHEN Sms_usage_D <> 'NULL' THEN Sms_usage_D ELSE 0 END FROM dailytemp WHERE MDN=VAR_MDN AND Date=VAR_DATE INTO VAR_SMS_DOMESTIC;
			SELECT CASE WHEN Sms_usage_R <> 'NULL' THEN Sms_usage_R ELSE 0 END FROM dailytemp WHERE MDN=VAR_MDN AND Date=VAR_DATE INTO VAR_SMS_ROAMING;
			SELECT CASE WHEN Sms_usage_I <> 'NULL' THEN Sms_usage_I ELSE 0 END FROM dailytemp WHERE MDN=VAR_MDN AND Date=VAR_DATE INTO VAR_SMS_INTER;
			SELECT CASE WHEN Voice_usage_D <> 'NULL' THEN Voice_usage_D ELSE 0 END FROM dailytemp WHERE MDN=VAR_MDN AND Date=VAR_DATE INTO VAR_VOICE_DOMESTIC;
			SELECT CASE WHEN Voice_usage_R <> 'NULL' THEN Voice_usage_R ELSE 0 END FROM dailytemp WHERE MDN=VAR_MDN AND Date=VAR_DATE INTO VAR_VOICE_ROAMING;
			SELECT CASE WHEN Voice_usage_I <> 'NULL' THEN Voice_usage_I ELSE 0 END FROM dailytemp WHERE MDN=VAR_MDN AND Date=VAR_DATE INTO VAR_VOICE_INTER;
			
			IF(SELECT EXISTS (SELECT 1 FROM dailyusagedata_final WHERE MDN=VAR_MDN)) THEN
				SELECT Previous_Bill_Cycle_Date FROM dailyusagedata_final WHERE MDN=VAR_MDN INTO P_BILL_CYCLE_DATE;
				SELECT Next_Bill_Cycle_Date FROM dailyusagedata_final WHERE MDN=VAR_MDN INTO N_BILL_CYCLE_DATE;
				
				IF (VAR_DATE>P_BILL_CYCLE_DATE AND VAR_DATE<=N_BILL_CYCLE_DATE) THEN
					UPDATE dailyusagedata_final SET Date=VAR_DATE WHERE MDN=VAR_MDN;
					UPDATE dailyusagedata_final SET Data_usage_D=Data_usage_D + VAR_DATA_DOMESTIC  WHERE MDN=VAR_MDN; 
					UPDATE dailyusagedata_final SET Data_usage_R=Data_usage_R + VAR_DATA_ROAMING WHERE MDN=VAR_MDN; 
					UPDATE dailyusagedata_final SET Data_usage_I=Data_usage_I + VAR_DATA_INTER WHERE MDN=VAR_MDN; 
					UPDATE dailyusagedata_final SET Sms_usage_D=Sms_usage_D + VAR_SMS_DOMESTIC WHERE MDN=VAR_MDN; 
					UPDATE dailyusagedata_final SET Sms_usage_R=Sms_usage_R + VAR_SMS_ROAMING WHERE MDN=VAR_MDN; 
					UPDATE dailyusagedata_final SET Sms_usage_I=Sms_usage_I + VAR_SMS_INTER WHERE MDN=VAR_MDN; 
					UPDATE dailyusagedata_final SET Voice_usage_D=Voice_usage_D + VAR_VOICE_DOMESTIC WHERE MDN=VAR_MDN; 
					UPDATE dailyusagedata_final SET Voice_usage_R=Voice_usage_R + VAR_VOICE_ROAMING WHERE MDN=VAR_MDN; 
					UPDATE dailyusagedata_final SET Voice_usage_I=Voice_usage_I + VAR_VOICE_INTER WHERE MDN=VAR_MDN; 					
				ELSE
					IF (VAR_DATE>N_BILL_CYCLE_DATE) THEN
						DELETE FROM dailyusagedata_final WHERE MDN=VAR_MDN;
						SET P_BILL_CYCLE_DATE = N_BILL_CYCLE_DATE;
						SELECT DATE_ADD(P_BILL_CYCLE_DATE,INTERVAL 1 MONTH) INTO N_BILL_CYCLE_DATE;
						INSERT dailyusagedata_final SELECT tbl.MDN,tbl.Acct_No,tbl.Date,P_BILL_CYCLE_DATE,N_BILL_CYCLE_DATE,
						VAR_DATA_DOMESTIC ,VAR_DATA_ROAMING ,VAR_DATA_INTER ,VAR_SMS_DOMESTIC,VAR_SMS_ROAMING,VAR_SMS_INTER,VAR_VOICE_DOMESTIC,VAR_VOICE_ROAMING,VAR_VOICE_INTER,acs.Your_Calling_Plan
						FROM DailyTemp tbl JOIN account_wireless_summary acs on tbl.MDN=cast(replace(acs.Wireless_Number,'-','') as unsigned) WHERE acs.Wireless_Number <> 'N/A' AND (tbl.MDN=VAR_MDN AND tbl.Date=VAR_DATE) ORDER BY acs.Bill_Cycle_Date DESC LIMIT 1;
					END IF;
				END IF;
			ELSE
				SELECT MAX(Bill_Cycle_Date) FROM account_wireless_summary WHERE CAST(REPLACE(Wireless_Number,'-','') AS unsigned)=VAR_MDN AND Wireless_Number <> 'N/A' INTO CURRENT_BILL_CYCLE_DATE;
				SELECT DAY(CURRENT_BILL_CYCLE_DATE) INTO BILL_CYCLE_DAY;
				SELECT DATE_SUB(DATE_ADD(CURDATE(),INTERVAL BILL_CYCLE_DAY DAY),INTERVAL DAY(CURDATE()) DAY) INTO N_BILL_CYCLE_DATE;
				SELECT DATE_SUB(N_BILL_CYCLE_DATE,INTERVAL 1 MONTH) INTO P_BILL_CYCLE_DATE;
				IF (VAR_DATE>P_BILL_CYCLE_DATE AND VAR_DATE<=N_BILL_CYCLE_DATE) THEN
					INSERT dailyusagedata_final SELECT tbl.MDN,tbl.Acct_No,tbl.Date,P_BILL_CYCLE_DATE,N_BILL_CYCLE_DATE,
					VAR_DATA_DOMESTIC ,VAR_DATA_ROAMING ,VAR_DATA_INTER ,VAR_SMS_DOMESTIC,VAR_SMS_ROAMING,VAR_SMS_INTER,VAR_VOICE_DOMESTIC,VAR_VOICE_ROAMING,VAR_VOICE_INTER,acs.Your_Calling_Plan
					FROM DailyTemp tbl JOIN account_wireless_summary acs on tbl.MDN=cast(replace(acs.Wireless_Number,'-','') as unsigned) WHERE acs.Wireless_Number <> 'N/A' AND (tbl.MDN=VAR_MDN AND tbl.Date=VAR_DATE) ORDER BY acs.Bill_Cycle_Date DESC LIMIT 1;
				END IF;	
			END IF;
			
		END LOOP CURSOR_LOOP2;
		CLOSE curs2;
		
	END