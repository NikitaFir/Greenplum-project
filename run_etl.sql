
BEGIN;

	SELECT training.run_etl('2020-05-01');
	SELECT training.run_report('2020-05-01');
	
	--SELECT training.run_etl('2020-05-02');
	--SELECT training.run_report('2020-05-02');
	
	--SELECT training.run_etl('2020-05-03');
	--SELECT training.run_report('2020-05-03');

	COMMIT;

END;