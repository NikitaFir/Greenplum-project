DROP FUNCTION IF EXISTS training.run_etl(text);
CREATE OR REPLACE FUNCTION training.run_etl(date_of_load text)
RETURNS VOID
AS $$

	INSERT INTO training.stg_clients
	SELECT client,
		   last_name,
		   first_name,
		   patronymic,
		   date_of_birth,
		   passport,
		   passport_valid_to,
		   phone,
		   date
	FROM training.srs_load
	WHERE date::timestamp::date = $1
	ORDER BY date;

	INSERT INTO training.stg_accounts
	SELECT account,
		   account_valid_to,
		   client,
		   date
	FROM training.srs_load
	WHERE date::timestamp::date = $1
	ORDER BY date;

	INSERT INTO training.stg_cards
	SELECT card,
		   account,
		   date
	FROM training.srs_load
	WHERE date::timestamp::date = $1
	ORDER BY date;

	INSERT INTO training.stg_terminals
	SELECT terminal,
		   terminal_type,
		   city,
		   address,
		   date
	FROM training.srs_load
	WHERE date::timestamp::date = $1
	ORDER BY date;

	INSERT INTO training.stg_transactions
	SELECT trans_id,
		   date,
		   card,
		   oper_type,
		   amount,
		   oper_result,
		   terminal
	FROM training.srs_load
	WHERE date::timestamp::date = $1
	ORDER BY date;

	/* Вставка данных в таблицы измерений и таблицы метаданных*/

	/* Вставка данных в таблицу измерений dim_clients*/

	UPDATE training.dim_clients dim
	SET 
	last_name         = stg.last_name,
	first_name        = stg.first_name,
	patronymic        = stg.patronymic,
	date_of_birth     = stg.date_of_birth,
	passport_num      = stg.passport_num,
	passport_valid_to = stg.passport_valid_to,
	phone             = stg.phone,
	update_dt         = stg.create_dt

	FROM (
			SELECT client_id,
				   last_name, 
				   first_name, 
				   patronymic, 
				   date_of_birth, 
				   passport_num, 
				   passport_valid_to, 
				   phone,
				   MIN(create_dt) AS create_dt
			FROM training.stg_clients
			GROUP BY client_id, last_name, first_name, patronymic, 
					 date_of_birth, passport_num, passport_valid_to, 
					 phone
			ORDER BY create_dt
		 ) stg
	WHERE dim.client_id = stg.client_id AND (dim.last_name <> stg.last_name 
	OR dim.first_name        <> stg.first_name        OR dim.patronymic   <> stg.patronymic 
	OR dim.date_of_birth     <> stg.date_of_birth     OR dim.passport_num <> stg.passport_num 
	OR dim.passport_valid_to <> stg.passport_valid_to OR dim.phone        <> stg.phone);
	
	
	INSERT INTO training.dim_clients (client_id, last_name, first_name, patronymic, date_of_birth, 
										passport_num, passport_valid_to, phone, create_dt, update_dt)
		SELECT client_id,
			   last_name, 
			   first_name, 
			   patronymic, 
			   date_of_birth, 
			   passport_num, 
			   passport_valid_to, 
			   phone, 
			   create_dt, 
			   NULL 
		FROM (
				SELECT client_id,
					   last_name, 
					   first_name, 
					   patronymic, 
					   date_of_birth, 
					   passport_num, 
					   passport_valid_to, 
					   phone,
					   MIN(create_dt) AS create_dt
				from training.stg_clients
				group by client_id, last_name, first_name, patronymic, 
						 date_of_birth, passport_num, passport_valid_to, 
						 phone
				order by create_dt
			 ) stg
		WHERE NOT EXISTS (SELECT 1 
						  FROM training.dim_clients 
						  WHERE client_id IN (SELECT client_id 
											  FROM training.stg_clients 
											  GROUP BY client_id
											  )
						  );

	/* Вставка метаднных в таблицу meta_clients */

	INSERT INTO training.meta_clients
	SELECT 'INSERTED', 
		   COUNT(*),
		   NOW() 
	FROM dim_clients dc
	WHERE dc.create_dt::timestamp::date = $1
	UNION ALL
	SELECT 'UPDATED', 
		   COUNT(*),
		   NOW() 
	FROM dim_clients dc
	WHERE dc.update_dt::timestamp::date = $1;


	/* Вставка данных в таблицу измерений dim_accounts */

	UPDATE training.dim_accounts dim
	SET 
	valid_to  = stg.valid_to,
	client    = stg.client,
	update_dt = stg.create_dt
	
	FROM (
			SELECT account_num,
				   valid_to,
				   client,
				   MIN(create_dt) AS create_dt
			FROM training.stg_accounts
			GROUP BY account_num, valid_to, client
			ORDER BY create_dt
		 ) stg 
	WHERE dim.account_num = stg.account_num AND (dim.valid_to <> stg.valid_to
	OR dim.client <> stg.client);
	
	
	INSERT INTO training.dim_accounts (account_num, valid_to, client, create_dt, 
										update_dt)
	SELECT account_num,
		   valid_to,
		   client,
		   create_dt,
		   NULL
	FROM (
			SELECT account_num,
				   valid_to,
				   client,
				   MIN(create_dt) AS create_dt
			FROM training.stg_accounts
			GROUP BY account_num, valid_to, client
			ORDER BY create_dt
		 ) stg
	WHERE NOT EXISTS (SELECT 1 
						  FROM training.dim_accounts 
						  WHERE account_num IN (SELECT account_num 
												FROM training.stg_accounts
												GROUP BY account_num
												)
					 );

	/* Вставка метаднных в таблицу meta_accounts */

	INSERT INTO training.meta_accounts
	SELECT 'INSERTED', 
		   COUNT(*),
		   NOW() 
	FROM dim_accounts da
	WHERE da.create_dt::timestamp::date = $1
	UNION ALL
	SELECT 'UPDATED', 
		   COUNT(*),
		   NOW() 
	FROM dim_accounts da
	WHERE da.update_dt::timestamp::date = $1;


	/* Вставка данных в таблицу измерений dim_cards */

	UPDATE training.dim_cards dim
	SET 
	account_num = stg.account_num,
	update_dt   = stg.create_dt
	
	FROM (
			SELECT card_num,
				   account_num,
				   MIN(create_dt) AS create_dt
			FROM training.stg_cards
			GROUP BY card_num, account_num
			ORDER BY create_dt
		 ) stg 
	WHERE dim.card_num = stg.card_num AND dim.account_num <> stg.account_num;
	
	
	INSERT INTO training.dim_cards (card_num, account_num, 
										create_dt, update_dt)
	SELECT card_num,
		   account_num,
		   create_dt,
		   NULL
	FROM (
			SELECT card_num,
				   account_num,
				   MIN(create_dt) AS create_dt
			FROM training.stg_cards
			GROUP BY card_num, account_num
			ORDER BY create_dt
		 ) stg
	WHERE NOT EXISTS (SELECT 1 
					  FROM training.dim_cards 
					  WHERE card_num IN (SELECT card_num 
										 FROM training.stg_cards
										 GROUP BY card_num
										)
					 );

	/* Вставка метаднных в таблицу meta_cards */

	INSERT INTO training.meta_cards
	SELECT 'INSERTED', 
		   COUNT(*),
		   NOW() 
	FROM dim_cards dc
	WHERE dc.create_dt::timestamp::date = $1
	UNION ALL
	SELECT 'UPDATED', 
		   COUNT(*),
		   NOW() 
	FROM dim_cards dc
	WHERE dc.update_dt::timestamp::date = $1;


	/* Вставка данных в таблицу измерений dim_terminals */

	UPDATE training.dim_terminals dim
	SET 
	terminal_type    = stg.terminal_type,
	terminal_city    = stg.terminal_city,
	terminal_address = stg.terminal_address,
	update_dt        = stg.create_dt
	
	FROM (
			SELECT terminal_id,
				   terminal_type,
				   terminal_city,
				   terminal_address,
				   MIN(create_dt) AS create_dt
			FROM training.stg_terminals
			GROUP BY terminal_id, terminal_type,
					 terminal_city, terminal_address
			ORDER BY create_dt
		 ) stg 
	WHERE dim.terminal_id = stg.terminal_id AND (dim.terminal_type <> stg.terminal_type 
	OR dim.terminal_city    <> stg.terminal_city 
	OR dim.terminal_address <> stg.terminal_address);
	
	
	INSERT INTO training.dim_terminals (terminal_id, terminal_type, terminal_city,
										terminal_address, create_dt, update_dt)
	SELECT terminal_id,
		   terminal_type,
		   terminal_city,
		   terminal_address,
		   create_dt,
		   NULL
	FROM (
			SELECT terminal_id,
				   terminal_type,
				   terminal_city,
				   terminal_address,
				   MIN(create_dt) AS create_dt
			FROM training.stg_terminals
			GROUP BY terminal_id, terminal_type,
					 terminal_city, terminal_address
			ORDER BY create_dt
		 ) stg
	WHERE NOT EXISTS (SELECT 1 
					  FROM training.dim_terminals 
					  WHERE terminal_id IN (SELECT terminal_id 
											FROM training.stg_terminals
											GROUP BY terminal_id
											)
					 );

	/* Вставка метаднных в таблицу meta_terminals */

	INSERT INTO training.meta_terminals
	SELECT 'INSERTED', 
		   COUNT(*),
		   NOW() 
	FROM dim_terminals dt
	WHERE dt.create_dt::timestamp::date = $1
	UNION ALL
	SELECT 'UPDATED', 
		   COUNT(*),
		   NOW() 
	FROM dim_terminals dt
	WHERE dt.update_dt::timestamp::date = $1;


	/* Вставка данных в таблицу фактов fact_transactions */

	INSERT INTO training.fact_transactions (trans_id, trans_date, card_num,
											oper_type, amt, oper_result, terminal)
	SELECT trans_id,
		   trans_date,
		   card_num,
		   oper_type,
		   amt,
		   oper_result,
		   terminal
	FROM training.stg_transactions;

	/* Вставка метаднных в таблицу meta_terminals */

	INSERT INTO training.meta_transactions
	SELECT 'INSERTED', 
		   COUNT(*),
		   NOW() 
	FROM fact_transactions dt
	WHERE dt.trans_date::timestamp::date = $1;


	/* Очистка промежуточных таблиц */

	TRUNCATE TABLE training.srs_load;

	TRUNCATE TABLE training.stg_clients;
	TRUNCATE TABLE training.stg_accounts;
	TRUNCATE TABLE training.stg_cards;
	TRUNCATE TABLE training.stg_terminals;
	TRUNCATE TABLE training.stg_transactions;
$$
LANGUAGE SQL;
