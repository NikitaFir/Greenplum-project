CREATE SCHEMA training AUTHORIZATION example_user;


DROP TABLE IF EXISTS training.srs_load;

DROP TABLE IF EXISTS training.stg_terminals;
DROP TABLE IF EXISTS training.stg_clients;
DROP TABLE IF EXISTS training.stg_accounts;
DROP TABLE IF EXISTS training.stg_cards;
DROP TABLE IF EXISTS training.stg_transactions;

DROP TABLE IF EXISTS training.dim_terminals_hist CASCADE;
DROP TABLE IF EXISTS training.dim_clients_hist CASCADE;
DROP TABLE IF EXISTS training.dim_accounts_hist CASCADE;
DROP TABLE IF EXISTS training.dim_cards_hist CASCADE;
DROP TABLE IF EXISTS training.fact_transactions CASCADE;
DROP TABLE IF EXISTS training.report;

DROP TABLE IF EXISTS training.meta_terminals;
DROP TABLE IF EXISTS training.meta_clients;
DROP TABLE IF EXISTS training.meta_accounts;
DROP TABLE IF EXISTS training.meta_cards;
DROP TABLE IF EXISTS training.meta_transactions;


/* garbage - таблица */

CREATE TABLE training.srs_load (
    trans_id           VARCHAR(50),
    date               TIMESTAMP(6),
    card               VARCHAR(50),
    account            VARCHAR(50),
    account_valid_to   TIMESTAMP(6),
    client             VARCHAR(50),
    last_name          VARCHAR(50),
    first_name         VARCHAR(50),
    patronymic         VARCHAR(50),
    date_of_birth      TIMESTAMP(6),
    passport           VARCHAR(50),
    passport_valid_to  TIMESTAMP(6),
    phone              VARCHAR(50),
    oper_type          VARCHAR(50),
    amount             NUMERIC(10,2),
    oper_result        VARCHAR(50),
    terminal           VARCHAR(50),
    terminal_type      VARCHAR(50),
    city               VARCHAR(50),
    address            VARCHAR(50)
    )
DISTRIBUTED RANDOMLY;


/* staging - таблицы */

CREATE TABLE training.stg_terminals (
    terminal_id       VARCHAR(50),
    terminal_type     VARCHAR(50),
    terminal_city     VARCHAR(50),
    terminal_address  VARCHAR(50),

    create_dt         TIMESTAMP(6)
    )
DISTRIBUTED BY (terminal_id);

CREATE TABLE training.stg_clients (
    client_id          VARCHAR(50),
    last_name          VARCHAR(50),
    first_name         VARCHAR(50),
    patronymic         VARCHAR(50),
    date_of_birth      TIMESTAMP(6),
    passport_num       VARCHAR(50),
    passport_valid_to  TIMESTAMP(6),
    phone              VARCHAR(50),

    create_dt         TIMESTAMP(6)
    )
DISTRIBUTED BY (client_id);

CREATE TABLE training.stg_accounts (
    account_num  VARCHAR(50),
    valid_to     TIMESTAMP(6),
    client       VARCHAR(50),
    
    create_dt         TIMESTAMP(6)
    )
DISTRIBUTED BY (account_num);

CREATE TABLE training.stg_cards (
    card_num     VARCHAR(50),
    account_num  VARCHAR(50),

    create_dt    TIMESTAMP(6)

    )
DISTRIBUTED BY (card_num);

CREATE TABLE training.stg_transactions (
    trans_id    VARCHAR(50),
    trans_date  TIMESTAMP(6),
    card_num    VARCHAR(50),
    oper_type   VARCHAR(50),
    amt         NUMERIC(10, 2),
    oper_result VARCHAR(50),
    terminal    VARCHAR(50) 
    )
DISTRIBUTED RANDOMLY;


/* Хранилище данных */

CREATE TABLE training.dim_terminals_hist (
    terminal_id       VARCHAR(50),
    terminal_type     VARCHAR(50)  NOT NULL,
    terminal_city     VARCHAR(50)  NOT NULL,
    terminal_address  VARCHAR(100) NOT NULL,

    start_dt          TIMESTAMP(6),
    end_dt            TIMESTAMP(6),

    PRIMARY KEY (terminal_id, start_dt)
    )
DISTRIBUTED BY (terminal_id);

CREATE TABLE training.dim_clients_hist (
    client_id          VARCHAR(50),
    last_name          VARCHAR(50)  NOT NULL,
    first_name         VARCHAR(50)  NOT NULL,
    patronymic         VARCHAR(50)  NOT NULL,
    date_of_birth      TIMESTAMP(6) NOT NULL,
    passport_num       VARCHAR(50)  NOT NULL,
    passport_valid_to  TIMESTAMP(6) NOT NULL,
    phone              VARCHAR(50)  NOT NULL,

    start_dt           TIMESTAMP(6),
    end_dt             TIMESTAMP(6),

    PRIMARY KEY (client_id, start_dt)
    )
DISTRIBUTED BY (client_id);

CREATE TABLE training.dim_accounts_hist (
    account_num  VARCHAR(50),
    valid_to     TIMESTAMP(6) NOT NULL,
    client       VARCHAR(50)  NOT NULL,
    
    start_dt     TIMESTAMP(6),
    end_dt       TIMESTAMP(6),

    PRIMARY KEY (account_num, start_dt)
    )
DISTRIBUTED BY (account_num);

CREATE TABLE training.dim_cards_hist (
    card_num     VARCHAR(50),
    account_num  VARCHAR(100) NOT NULL,

    start_dt     TIMESTAMP(6),
    end_dt       TIMESTAMP(6),

    PRIMARY KEY (card_num, start_dt)
    )
DISTRIBUTED BY (card_num);

CREATE TABLE training.fact_transactions (
    trans_id    VARCHAR(50),
    trans_date  TIMESTAMP(6)  NOT NULL,
    card_num    VARCHAR(50)   NOT NULL,
    oper_type   VARCHAR(50),
    amt         NUMERIC(10,2) NOT NULL,
    oper_result VARCHAR(50),
    terminal    VARCHAR(50)   NOT NULL,

    PRIMARY KEY (trans_id, oper_type, oper_result)
    )
DISTRIBUTED BY (trans_id)
PARTITION BY LIST(oper_type)
    SUBPARTITION BY LIST(oper_result)
        SUBPARTITION TEMPLATE
        (
            SUBPARTITION successfully VALUES('Успешно'),
            SUBPARTITION refusal      VALUES('Отказ')
        )
(
    PARTITION withdrawal VALUES('Снятие'),
    PARTITION refill     VALUES('Пополнение'),
    PARTITION payment    VALUES('Оплата')
);


CREATE TABLE training.report (
    fraud_dt    TIMESTAMP(6) NOT NULL,
    passport    VARCHAR(50)  NOT NULL,
    fio         VARCHAR(150) NOT NULL,
    phone       VARCHAR(50)  NOT NULL,
    fraud_type  VARCHAR(100) NOT NULL,
    report_dt   TIMESTAMP(6) NOT NULL
    )
DISTRIBUTED RANDOMLY;


/* Таблицы для метаданных */

CREATE TABLE training.meta_terminals (
    action       VARCHAR(10),
    amount       INT4,
    action_date  TIMESTAMP(6)
    )
DISTRIBUTED RANDOMLY
PARTITION BY LIST(action)
(
    PARTITION inserted VALUES('INSERTED'),
    PARTITION updated  VALUES('UPDATED')
);

CREATE TABLE training.meta_clients (
    action       VARCHAR(10),
    amount       INT4,
    action_date  TIMESTAMP(6)
    )
DISTRIBUTED RANDOMLY
PARTITION BY LIST(action)
(
    PARTITION inserted VALUES('INSERTED'),
    PARTITION updated  VALUES('UPDATED')
);

CREATE TABLE training.meta_accounts (
    action       VARCHAR(10),
    amount       INT4,
    action_date  TIMESTAMP(6)
    )
DISTRIBUTED RANDOMLY
PARTITION BY LIST(action)
(
    PARTITION inserted VALUES('INSERTED'),
    PARTITION updated  VALUES('UPDATED')
);

CREATE TABLE training.meta_cards (
    action       VARCHAR(10),
    amount       INT4,
    action_date  TIMESTAMP(6)
    )
DISTRIBUTED RANDOMLY
PARTITION BY LIST(action)
(
    PARTITION inserted VALUES('INSERTED'),
    PARTITION updated  VALUES('UPDATED')
);

CREATE TABLE training.meta_transactions (
    action       VARCHAR(10),
    amount       INT4,
    action_date  TIMESTAMP(6)
    )
DISTRIBUTED RANDOMLY;
