DROP FUNCTION IF EXISTS training.run_report(text);
CREATE OR REPLACE FUNCTION training.run_report(date_of_load text)
RETURNS VOID
AS $$

	/* 1. Совершение операции при просроченном паспорте. */

	/* Ход решения:
	 * 
	 * 1) В подзапросе tr берем все номера карт, которые встречались в 
	 *    транзакциях сегодня.
	 * 
	 * 2) Формируем отчет, взяв остальную информацию при объединении таблиц.
	 *    Предполагаемые мошеннические операции будут те, где passport_valid_to < tr.trans_date
	 *    (т.е. паспорт был просрочен на момент записи времени транзакции)
	 * 
	 */

	INSERT INTO training.report
	SELECT tr.trans_date AS FRAUD_DT,
		   dc.passport_num AS PASSPORT,
		   dc.last_name || ' ' || dc.first_name || ' ' || dc.patronymic AS FIO,
		   dc.phone AS PHONE,
		   'Совершение операции при просроченном паспорте' AS FRAUD_TYPE,
		   NOW() AS REPORT_DT
	FROM (
		  SELECT card_num,
				 trans_date
		  FROM training.fact_transactions
		  WHERE trans_date::timestamp::date = $1
		  ) tr
	INNER JOIN training.dim_cards_hist cards ON tr.card_num = cards.card_num
	INNER JOIN training.dim_accounts_hist da ON cards.account_num = da.account_num
	INNER JOIN training.dim_clients_hist dc  ON da.client = dc.client_id
	WHERE dc.passport_valid_to < tr.trans_date
	AND cards.end_dt IS NULL AND da.end_dt IS NULL AND dc.end_dt IS NULL;


	/* 2. Совершение операции при недействующем договоре. */

	/* Ход решения:
	 * 
	 * 1) В подзапросе tr берем все номера карт, которые встречались в 
	 *    транзакциях сегодня.
	 * 
	 * 2) Формируем отчет, взяв остальную информацию при объединении таблиц.
	 *    Предполагаемые мошеннические операции будут те, где  da.valid_to < tr.trans_date
	 *    (т.е. договор был просрочен на момент записи времени транзакции)
	 * 
	 */

	INSERT INTO training.report
	SELECT tr.trans_date AS FRAUD_DT,
		   dc.passport_num AS PASSPORT,
		   dc.last_name || ' ' || dc.first_name || ' ' || dc.patronymic AS FIO,
		   dc.phone AS PHONE,
		   'Совершение операции при недействующем договоре' AS FRAUD_TYPE,
		   NOW() AS REPORT_DT
	FROM (
		  SELECT card_num,
				 trans_date
		  FROM training.fact_transactions
		  WHERE trans_date::timestamp::date = $1
		  ) tr
	INNER JOIN training.dim_cards_hist cards ON tr.card_num = cards.card_num
	INNER JOIN training.dim_accounts_hist da ON cards.account_num = da.account_num
	INNER JOIN training.dim_clients_hist dc  ON da.client = dc.client_id
	WHERE da.valid_to < tr.trans_date
	AND cards.end_dt IS NULL AND da.end_dt IS NULL AND dc.end_dt IS NULL;

	/* 3. Совершение операции в разных городах в течение 1 часа. */
	
	/* Ход решения:
	 * 
	 * 1) Берем все номера карт, которые встречались в транзакциях более 1 раза.
	 * 
	 * 2) В подзапросе t2 в каждой строке получаем информацию о каждой карте:
	 *    номер карты; номер терминала; дату операции; разницу во времени между прошлой 
	 *    операцией и текущей; предыдущий город (где была произведена прошлая операция);
	 *    текущий город (где произведена текущая операция).
	 * 
	 * 3) В подзапросе t3 берем номер карты и дату транзакции, отфильтровав те значения,
	 *    где разница во времени между предыдущей и текущей транзакцией меньше 1 
	 *    часа и города, где была произведена предыдущая и текущая операции не совпадают.
	 * 
	 * 4) Формируем отчет. 
	 * 
	 */

	INSERT INTO training.report
	SELECT t3.trans_date AS FRAUD_DT,
		   dc2.passport_num AS PASSPORT,
		   dc2.last_name || ' ' || dc2.first_name || ' ' || dc2.patronymic AS FIO,
		   dc2.phone AS PHONE,
		   'Совершение операции в разных городах в течение 1 часа' AS FRAUD_TYPE,
		   NOW() AS REPORT_DT
	FROM
	(	
		SELECT t2.card_num,
			   t2.trans_date
		FROM
		(	
		   SELECT tr2.card_num,
				   tr2.terminal,
				   tr2.trans_date,
				   tr2.trans_date - LAG(tr2.trans_date) 
					   OVER(PARTITION BY tr2.card_num ORDER BY tr2.trans_date) AS time_delta,
				   LAG(dt1.terminal_city) 
					   OVER(PARTITION BY tr2.card_num ORDER BY tr2.trans_date) AS prev_city,
				   dt1.terminal_city AS cur_city
				   
			FROM training.fact_transactions tr2
			INNER JOIN training.dim_terminals_hist dt1 ON tr2.terminal = dt1.terminal_id 
			WHERE tr2.card_num IN (	
									SELECT tr1.card_num
									FROM training.fact_transactions tr1
									WHERE trans_date::timestamp::date = $1
									GROUP BY tr1.card_num 
									HAVING COUNT(*) > 1
								  ) 
			AND trans_date::timestamp::date = $1
			AND dt1.end_dt IS NULL
		) t2
		INNER JOIN training.dim_terminals_hist dt2 ON t2.terminal = dt2.terminal_id 
		WHERE time_delta <= interval '1 hour' AND prev_city <> cur_city
		AND dt2.end_dt IS NULL
	) t3
	INNER JOIN training.dim_cards_hist dc    ON t3.card_num = dc.card_num 
	INNER JOIN training.dim_accounts_hist da ON dc.account_num = da.account_num 
	INNER JOIN training.dim_clients_hist dc2 ON da.client = dc2.client_id
	WHERE dc.end_dt IS NULL AND da.end_dt IS NULL AND dc2.end_dt IS NULL;


	/* 4) Попытка подбора сумм. */
	
	/* Ход решения:
	 * 
	 * 1) Берем все номера карт, которые встречались в транзакциях более 3 раз. 
	 *
	 * 2) В подзапросе q1 в каждой строке получаем:
	 *  
	 *    card_num - номер карты;
	 * 
	 *    trans_date - дату транзакции;
	 *  
	 *    amt - сумму; 
	 * 
	 *    prev_amt - сумму прошлой операции; 
	 * 
	 *    oper_result_total - результат операции в численном виде (0 - Отказ, 1 - Успешно);
	 *  
	 *    oper_result - результат операции.
	 * 
	 * 3) В подзапросе q2 получаем:
	 * 
	 *    card_num - номер карты;
	 * 
	 *    trans_date - дату транзакции;
	 * 
	 *    oper_result_total - результат операции в численном виде (0 - Отказ, 1 - Успешно);
	 *  
	 *    oper_result - результат операции;
	 *  
	 *    prev_less_than_cur - значение, которое принимает 0, если прошлая сумма была меньше. Если
	 *    прошлая сумма была больше, то значение будет равно 1.
	 * 
	 * 4) В подзапросе q3 получаем
	 * 	  card_num - номер карты;
	 * 
	 *    trans_date - последнюю дату транзакции;
	 * 
	 *    oper_result_total - результат последней операции в численном виде (Просуммировали 
	 *                        численные значения операций, где 0 - Отказ, 1 - Успешно.
	 *                        Таким образом, если значение не превышает 1, то возможна ситуация,
	 *                        при которой все операции были отклонены, кроме последней);
	 * 
	 *    prev_less_than_cur_total - значение, которое показывает, что все суммы для данной карты были меньше предыдущих
	 *                               (Просуммировали все значения, где 0 - сумма была меньше предыдущей, 1 - сумма была 
	 *                               больше предыдущей. Таким образом, если общая сумма равно 0, то все суммы в 
	 *                               последовательности транзакций были меньше предыдущих);
	 * 
	 *    last_oper - результат последней операции в последовательности транзакций.
	 * 
	 *  4) В подзапросе q4 отфильтровываем строки, где:
	 * 
	 *     oper_result_total = 1  --> в последовательности транзакций все операции были отклонены кроме одной
	 *     last_oper = 'Успешно'  --> последняя операция была успешной
	 *     prev_less_than_cur_total = 0  --> все суммы в последовательности транзакций были меньше, чем в 
	 *                                       предыдущих
	 * 
	 *  5) Формируем отчет.
	 * 
	 */

	INSERT INTO training.report
	SELECT q4.trans_date AS FRAUD_DT,
		   dc2.passport_num AS PASSPORT,
		   dc2.last_name || ' ' || dc2.first_name || ' ' || dc2.patronymic AS FIO,
		   dc2.phone AS PHONE,
		   'Попытка подбора сумм' AS FRAUD_TYPE,
		   NOW() AS REPORT_DT
	FROM
	(	
		SELECT q3.card_num,
			   q3.trans_date
		FROM
		(	
		  SELECT q2.card_num,
				   LAST_VALUE(q2.trans_date) 
								OVER(PARTITION BY q2.card_num ORDER BY q2.trans_date
									RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
										AS trans_date,
				   SUM(q2.oper_result_total) 
								OVER(PARTITION BY q2.card_num ORDER BY q2.trans_date
									RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
										AS oper_result_total,
				   SUM(q2.prev_less_than_cur) 
								OVER(PARTITION BY q2.card_num ORDER BY q2.trans_date
									RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
										AS prev_less_than_cur_total,
				   LAST_VALUE(q2.oper_result) 
								OVER(PARTITION BY q2.card_num ORDER BY q2.trans_date
									RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
										AS last_oper
			FROM
			(	
			   SELECT q1.card_num,
					  q1.trans_date,
					  q1.oper_result_total,
					  q1.oper_result,
					  CASE
						  WHEN q1.prev_amt IS NULL THEN
							0
						  WHEN q1.amt < q1.prev_amt THEN
							0
						  ELSE 
							1
					  END AS prev_less_than_cur
				FROM
				(	
					SELECT tr2.card_num,
						   tr2.trans_date,
						   tr2.amt,
						   LAG(tr2.amt) OVER (PARTITION BY tr2.card_num ORDER BY tr2.trans_date) AS prev_amt,
						   CASE
							   WHEN tr2.oper_result = 'Отказ' THEN 
								0
							   WHEN tr2.oper_result = 'Успешно' THEN
								1
						   END AS oper_result_total,
						   tr2.oper_result
					FROM training.fact_transactions tr2
					WHERE tr2.card_num IN (
											SELECT tr1.card_num
											FROM training.fact_transactions tr1 
											WHERE trans_date::timestamp::date = $1
											GROUP BY tr1.card_num 
											HAVING COUNT(*) > 3
										  )	
					AND trans_date::timestamp::date = $1
				) q1		
			) q2		
		) q3	
		WHERE q3.oper_result_total = 1 AND q3.last_oper = 'Успешно' AND q3.prev_less_than_cur_total = 0
		GROUP BY q3.card_num, q3.trans_date
	) q4
	INNER JOIN training.dim_cards_hist dc    ON q4.card_num = dc.card_num 
	INNER JOIN training.dim_accounts_hist da ON dc.account_num = da.account_num 
	INNER JOIN training.dim_clients_hist dc2 ON da.client = dc2.client_id
	WHERE dc.end_dt IS NULL AND da.end_dt IS NULL AND dc2.end_dt IS NULL

$$
LANGUAGE SQL;
