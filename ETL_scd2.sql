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

	/* Вставка данных в таблицу измерений dim_clients_hist*/

	UPDATE training.dim_clients_hist dim
	SET 
	end_dt = stg.create_dt

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
	WHERE dim.client_id = stg.client_id AND dim.end_dt IS NULL AND (dim.last_name <> stg.last_name 
	OR dim.first_name        <> stg.first_name        OR dim.patronymic   <> stg.patronymic 
	OR dim.date_of_birth     <> stg.date_of_birth     OR dim.passport_num <> stg.passport_num 
	OR dim.passport_valid_to <> stg.passport_valid_to OR dim.phone        <> stg.phone);


	INSERT INTO training.dim_clients_hist (client_id, last_name, first_name, patronymic, date_of_birth, 
								passport_num, passport_valid_to, phone, start_dt, end_dt)
	SELECT stg.client_id,
		   stg.last_name,
		   stg.first_name,
		   stg.patronymic,
		   stg.date_of_birth,
		   stg.passport_num,
		   stg.passport_valid_to,
		   stg.phone,
		   stg.create_dt,
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
			FROM training.stg_clients
			GROUP BY client_id, last_name, first_name, patronymic, 
					 date_of_birth, passport_num, passport_valid_to, 
					 phone
			ORDER BY create_dt
		 ) stg, training.dim_clients_hist dim
	WHERE dim.client_id = stg.client_id AND dim.end_dt = stg.create_dt AND (dim.last_name <> stg.last_name 
	OR dim.first_name        <> stg.first_name        OR dim.patronymic   <> stg.patronymic 
	OR dim.date_of_birth     <> stg.date_of_birth     OR dim.passport_num <> stg.passport_num 
	OR dim.passport_valid_to <> stg.passport_valid_to OR dim.phone        <> stg.phone);


	INSERT INTO training.dim_clients_hist (client_id, last_name, first_name, patronymic, date_of_birth, 
									passport_num, passport_valid_to, phone, start_dt, end_dt)
	SELECT client_id,
		   last_name, 
		   first_name, 
		   patronymic, 
		   date_of_birth, 
		   passport_num, 
		   passport_valid_to, 
		   phone,
		   MIN(create_dt) AS create_dt,
		   NULL
	FROM training.stg_clients
	WHERE client_id NOT IN (SELECT client_id 
							FROM training.dim_clients_hist 
							WHERE client_id IS NOT NULL
						   )
	GROUP BY client_id, last_name, first_name, patronymic, 
			 date_of_birth, passport_num, passport_valid_to, 
			 phone
	ORDER BY create_dt;
	

	/* Вставка метаднных в таблицу meta_clients */

	INSERT INTO training.meta_clients
	SELECT 'INSERTED', 
		   COUNT(*),
		   NOW() 
	FROM dim_clients_hist dc
	WHERE dc.start_dt::timestamp::date = $1
	UNION ALL
	SELECT 'UPDATED', 
		   COUNT(*),
		   NOW() 
	FROM dim_clients_hist dc
	WHERE dc.end_dt::timestamp::date = $1;


	/* Вставка данных в таблицу измерений dim_accounts_hist */

	UPDATE training.dim_accounts_hist dim
	SET 
	end_dt = stg.create_dt
	
	FROM (
			SELECT account_num,
				   valid_to,
				   client,
				   MIN(create_dt) AS create_dt
			FROM training.stg_accounts
			GROUP BY account_num, valid_to, client
			ORDER BY create_dt
		 ) stg 
	WHERE dim.account_num = stg.account_num AND dim.end_dt IS NULL AND (dim.valid_to <> stg.valid_to
	OR dim.client <> stg.client);
	
	
					 
	INSERT INTO training.dim_accounts_hist (account_num, valid_to, client, start_dt, 
										end_dt)
	SELECT stg.account_num,
		   stg.valid_to,
		   stg.client,
		   stg.create_dt,
		   NULL
	FROM (
			SELECT account_num,
				   valid_to,
				   client,
				   MIN(create_dt) AS create_dt
			FROM training.stg_accounts
			GROUP BY account_num, valid_to, client
			ORDER BY create_dt
		 ) stg, training.dim_accounts_hist dim
	WHERE dim.account_num = stg.account_num AND dim.end_dt = stg.create_dt AND (dim.valid_to <> stg.valid_to
	OR dim.client <> stg.client);
	


	INSERT INTO training.dim_accounts_hist (account_num, valid_to, client, start_dt, 
										end_dt)
	SELECT account_num,
		   valid_to,
		   client,
		   MIN(create_dt) AS create_dt,
		   NULL
	FROM training.stg_accounts
	WHERE account_num NOT IN (SELECT account_num 
							  FROM training.dim_accounts_hist 
							  WHERE account_num IS NOT NULL
						     )
	GROUP BY account_num, valid_to, client
	ORDER BY create_dt;

	/* Вставка метаднных в таблицу meta_accounts */

	INSERT INTO training.meta_accounts
	SELECT 'INSERTED', 
		   COUNT(*),
		   NOW() 
	FROM dim_accounts_hist da
	WHERE da.start_dt::timestamp::date = $1
	UNION ALL
	SELECT 'UPDATED', 
		   COUNT(*),
		   NOW() 
	FROM dim_accounts_hist da
	WHERE da.end_dt::timestamp::date = $1;


	/* Вставка данных в таблицу измерений dim_cards_hist */

	UPDATE training.dim_cards_hist dim
	SET 
	end_dt = stg.create_dt
	
	FROM (
			SELECT card_num,
				   account_num,
				   MIN(create_dt) AS create_dt
			FROM training.stg_cards
			GROUP BY card_num, account_num
			ORDER BY create_dt
		 ) stg 
	WHERE dim.card_num = stg.card_num AND dim.end_dt IS NULL 
	AND dim.account_num <> stg.account_num;


	INSERT INTO training.dim_cards_hist (card_num, account_num, 
										start_dt, end_dt)
	SELECT stg.card_num,
		   stg.account_num,
		   stg.create_dt,
		   NULL
	FROM (
			SELECT card_num,
				   account_num,
				   MIN(create_dt) AS create_dt
			FROM training.stg_cards
			GROUP BY card_num, account_num
			ORDER BY create_dt
		 ) stg, training.dim_cards_hist dim
	WHERE dim.card_num = stg.card_num AND dim.end_dt = stg.create_dt
	AND dim.account_num <> stg.account_num;


	INSERT INTO training.dim_cards_hist (card_num, account_num, 
									start_dt, end_dt)
	SELECT card_num,
		   account_num,
		   MIN(create_dt) AS create_dt,
		   NULL
	FROM training.stg_cards
	WHERE account_num NOT IN (SELECT card_num 
							  FROM training.dim_cards_hist 
							  WHERE card_num IS NOT NULL
						     )
	GROUP BY card_num, account_num
	ORDER BY create_dt;

	/* Вставка метаднных в таблицу meta_cards */

	INSERT INTO training.meta_cards
	SELECT 'INSERTED', 
		   COUNT(*),
		   NOW() 
	FROM dim_cards_hist dc
	WHERE dc.start_dt::timestamp::date = $1
	UNION ALL
	SELECT 'UPDATED', 
		   COUNT(*),
		   NOW() 
	FROM dim_cards_hist dc
	WHERE dc.end_dt::timestamp::date = $1;


	/* Вставка данных в таблицу измерений dim_terminals_hist */

	UPDATE training.dim_terminals_hist dim
	SET 
	end_dt = stg.create_dt
	
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
	WHERE dim.terminal_id = stg.terminal_id AND dim.end_dt IS NULL AND(dim.terminal_type <> stg.terminal_type 
	OR dim.terminal_city    <> stg.terminal_city 
	OR dim.terminal_address <> stg.terminal_address);


	INSERT INTO training.dim_terminals_hist (terminal_id, terminal_type, terminal_city,
										terminal_address, start_dt, end_dt)
	SELECT stg.terminal_id,
		   stg.terminal_type,
		   stg.terminal_city,
		   stg.terminal_address,
		   stg.create_dt,
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
		 ) stg, training.dim_terminals_hist dim
	WHERE dim.terminal_id = stg.terminal_id AND dim.end_dt = stg.create_dt 
	AND (dim.terminal_type  <> stg.terminal_type 
	OR dim.terminal_city    <> stg.terminal_city 
	OR dim.terminal_address <> stg.terminal_address);
	

	INSERT INTO training.dim_terminals_hist (terminal_id, terminal_type, terminal_city,
										terminal_address, start_dt, end_dt)

	SELECT terminal_id,
		   terminal_type,
		   terminal_city,
		   terminal_address,
		   MIN(create_dt) AS create_dt,
		   NULL
	FROM training.stg_terminals
	WHERE terminal_id NOT IN (SELECT terminal_id 
							  FROM training.dim_terminals_hist
							  WHERE terminal_id IS NOT NULL
						     )
	GROUP BY terminal_id, terminal_type,
			 terminal_city, terminal_address
	ORDER BY create_dt;

	/* Вставка метаднных в таблицу meta_terminals */

	INSERT INTO training.meta_terminals
	SELECT 'INSERTED', 
		   COUNT(*),
		   NOW() 
	FROM dim_terminals_hist dt
	WHERE dt.start_dt::timestamp::date = $1
	UNION ALL
	SELECT 'UPDATED', 
		   COUNT(*),
		   NOW() 
	FROM dim_terminals_hist dt
	WHERE dt.end_dt::timestamp::date = $1;


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
