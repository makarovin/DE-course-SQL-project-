import sqlite3
import os
from pathlib import Path
import pandas as pd

# init cards, accounts, clients data
def init_data_clients():
    try:
        sqlite_connection = sqlite3.connect('sber.db')
        cursor = sqlite_connection.cursor()
        print("База данных подключена к SQLite")

        with open('ddl_dml.sql', 'r', encoding='utf-8') as sqlite_file:
            sql_script = sqlite_file.read()

        cursor.executescript(sql_script)
        print("Скрипт SQLite успешно выполнен")

    except sqlite3.Error as error:
        print("Ошибка при подключении к sqlite", error)
    finally:
        if sqlite_connection:
            sqlite_connection.close()
            print("Соединение с SQLite закрыто")


# добавляем строки scd2 в БД
def add_scd2_rows_to_init(table_name, effective_from, effective_to):
    sqlite_connection = sqlite3.connect('sber.db')
    cursor = sqlite_connection.cursor()

    print("База данных подключена к SQLite")
    try:
        cursor.execute(f'''
        select effective_from from {table_name}
        ''', [table_name])
    except BaseException as e:
        cursor.execute(f'''
        alter table {table_name} add effective_from datetime;
        ''')
        cursor.execute(f'''
        alter table {table_name} add effective_to datetime;
        ''')
        cursor.execute(f'''
        alter table {table_name} add deleted_flg integer default 0;
        ''')
    cursor.execute(f'''
            UPDATE {table_name}
            set effective_from = ?
            ''', [effective_from])
    cursor.execute(f'''
            UPDATE {table_name}
            set effective_to = ?
            ''', [effective_to])
    sqlite_connection.commit()
    if sqlite_connection:
        sqlite_connection.close()
        print("Соединение с SQLite закрыто")


# create table terminals
def create_tables_terminals(filepath, effective_from, effective_to):
    sqlite_connection = sqlite3.connect('sber.db')
    cursor = sqlite_connection.cursor()

    print("База данных подключена к SQLite")
    data_term = pd.read_excel(filepath, sheet_name=None)
    for sheet in data_term:
        data_term[sheet].to_sql(sheet, sqlite_connection, index=False, if_exists='append')
    try:
        cursor.execute('''
        select effective_from from terminals
        ''')
    except BaseException as e:
        cursor.execute('''
        alter table terminals add effective_from datetime;
        ''')
        cursor.execute('''
        alter table terminals add effective_to datetime;
        ''')
        cursor.execute('''
        alter table terminals add deleted_flg integer default 0;
        ''')
    cursor.execute('''
        UPDATE terminals
        set effective_from = ?
        where effective_from is Null
        ''', [effective_from])

    cursor.execute('''
        UPDATE terminals
        set effective_to = ?
        where effective_to is Null
        ''', [effective_to])
    sqlite_connection.commit()
    p = Path(filepath)
    try:
        os.mkdir('archive')
    except:
        pass
    file = p.rename(p.with_suffix('.xlsx.backup'))
    Path(f"{file}").rename(f"archive/{file}")
    sqlite_connection.commit()
    if sqlite_connection:
        sqlite_connection.close()
        print("Соединение с SQLite закрыто")


# create table transactions
def create_tables_transactions(filepath, effective_from, effective_to):
    sqlite_connection = sqlite3.connect('sber.db')
    cursor = sqlite_connection.cursor()

    print("База данных подключена к SQLite")
    cursor.execute('''
            create table if not exists transactions(
            transaction_id varchar(128),
            transaction_date datetime,
            amount decimal,
            card_num varchar(128),
            oper_type varchar(128),
            oper_result varchar(128),
            terminal varchar(128),
            effective_from datetime,
            effective_to datetime,
            deleted_flg integer default 0
            )
        ''')
    columns = '''
            transaction_id,
            transaction_date,
            amount,
            card_num,
            oper_type,
            oper_result,
            terminal
            '''
    with open(filepath, 'r') as f:
        for line in f:
            if not line.startswith('transaction_id'):
                line = line.replace('\n', '')
                data_trans = line.split(';')
                cursor.execute(f'''
                insert into transactions
                (
                    {columns}
                )
                values (?, ?, ?, ?, ?, ?, ?)
                ''', [*data_trans])
        cursor.execute('''
                UPDATE transactions
                set effective_from = ?
                where effective_from is Null
            ''', [effective_from])
        cursor.execute('''
                UPDATE transactions
                set effective_to = ?
                where effective_to is Null
            ''', [effective_to])
    try:
        os.mkdir('archive')
    except:
        pass
    p = Path(filepath)
    file = p.rename(p.with_suffix('.txt.backup'))
    Path(f"{file}").rename(f"archive/{file}")
    sqlite_connection.commit()
    if sqlite_connection:
        sqlite_connection.close()
    print("Соединение с SQLite закрыто")


# create table blacklist
def create_tables_blacklist(filepath, effective_from, effective_to):
    sqlite_connection = sqlite3.connect('sber.db')
    cursor = sqlite_connection.cursor()

    print("База данных подключена к SQLite")
    data_term = pd.read_excel(filepath, sheet_name=None)
    for sheet in data_term:
        data_term[sheet].to_sql(sheet, sqlite_connection, index=False, if_exists='append')
    try:
        cursor.execute('''
        select effective_from from blacklist
        ''')
    except BaseException as e:
        cursor.execute('''
        ALTER TABLE blacklist add effective_from datetime;
        ''')
        cursor.execute('''
        ALTER TABLE blacklist add effective_to datetime;
        ''')
        cursor.execute('''
        ALTER TABLE blacklist add deleted_flg integer default 0;
        ''')
    cursor.execute('''
        UPDATE blacklist
        set effective_from = ?
        where effective_from is Null
        ''', [effective_from])

    cursor.execute('''
        UPDATE blacklist
        set effective_to = ?
        where effective_to is Null
        ''', [effective_to])

    try:
        os.mkdir('archive')
    except:
        pass
    p = Path(filepath)
    file = p.rename(p.with_suffix('.xlsx.backup'))
    Path(f"{file}").rename(f"archive/{file}")
    sqlite_connection.commit()
    if sqlite_connection:
        sqlite_connection.close()
        print("Соединение с SQLite закрыто")


# 1А.	Совершение операции при заблокированном паспорте.
def locked_passport():
    sqlite_connection = sqlite3.connect('sber.db')
    cursor = sqlite_connection.cursor()
    print("База данных подключена к SQLite")
    cursor.execute('''
    select distinct
        transaction_date as event_dt, 
        passport_num as passport, 
        last_name, 
        first_name, 
        patronymic, 
        phone,
        'locked passport',
        datetime('now')
    from
    ( 
        select 
            *
        from 
            transactions t1
        inner join 
        (
            select 
                * 
            from cards t1
            inner join 
            (
                select 
                    * 
                from 
                    accounts t1
                inner join 
                    clients t2 on t1.client = t2.client_id
            ) t2 on t1.account = t2.account
        ) t2 on t1.card_num = t2.card_num
    ) t1
    inner join
        blacklist t2 on t1.passport_num = t2.passport
    where julianday(transaction_date) > julianday(t2.date)
    ''')
    for row in cursor.fetchall():
        print(row)
    if sqlite_connection:
        sqlite_connection.close()
        print("Соединение с SQLite закрыто")


# 1Б.	Совершение операции при просроченном паспорте.
def overdue_passport():
    sqlite_connection = sqlite3.connect('sber.db')
    cursor = sqlite_connection.cursor()
    print("База данных подключена к SQLite")
    cursor.execute('''
    select distinct
        transaction_date as event_dt, 
        passport_num as passport, 
        last_name, 
        first_name, 
        patronymic, 
        phone, 
        'overdue passport',
        datetime('now') 
    from 
        transactions t1
    inner join 
    (
        select 
            * 
        from 
            cards t1
        inner join 
            (
                select 
                    * 
                from 
                    accounts t1
                inner join 
                    clients t2 on t1.client = t2.client_id
            ) t2 on t1.account = t2.account
    ) t2 on t1.card_num = t2.card_num
    where julianday(transaction_date) > julianday(passport_valid_to)
    ''')
    for row in cursor.fetchall():
        print(row)
    if sqlite_connection:
        sqlite_connection.close()
        print("Соединение с SQLite закрыто")


# 2. Совершение операции при недействующем договоре.
def invalid_contract():
    sqlite_connection = sqlite3.connect('sber.db')
    cursor = sqlite_connection.cursor()
    print("База данных подключена к SQLite")
    cursor.execute('''
        select distinct
            transaction_date as event_dt, 
            passport_num as passport, 
            last_name, 
            first_name, 
            patronymic, 
            phone, 
            'invalid_contract',
            datetime('now')             
        from 
            transactions t1
        inner join 
        (
            select 
                * 
            from cards t1
            inner join 
            (
                select 
                    * 
                from 
                    accounts t1
                inner join 
                    clients t2 on t1.client = t2.client_id
            ) t2 on t1.account = t2.account
        ) t2 on t1.card_num = t2.card_num
        where julianday(transaction_date) > julianday(valid_to)
        ''')
    for row in cursor.fetchall():
        print(row)
    if sqlite_connection:
        sqlite_connection.close()
        print("Соединение с SQLite закрыто")


# 3. Совершение операций в разных городах в течение одного часа.
def different_cities(data_from, data_to):
    sqlite_connection = sqlite3.connect('sber.db')
    cursor = sqlite_connection.cursor()
    print("База данных подключена к SQLite")
    cursor.execute('''
    select distinct
        t1.transaction_date as event_date,
        t1.passport_num as passport,
        t1.last_name, 
        t1.first_name, 
        t1.patronymic, 
        t1.phone as phone, 
        'different cities within one hour' as event_type,
        datetime('now') as report_dt
    from
    (
        select
            *
        from
        (
            select
                *
            from 
                transactions t1
            inner join
            (
                select
                    *
                from
                    cards t1
                inner join
                (
                    select 
                        * 
                    from 
                        accounts t1
                    inner join
                        clients t2 on t1.client = t2.client_id
                ) t2 on t1.account = t2.account
            ) t2 on t1.card_num = t2.card_num
        ) t1
        inner join
            terminals t2 on t1.terminal = t2.terminal_id
    ) t1
    inner join
    (
        select
                *
            from
            (
                select
                    *
                from 
                    transactions t1
                inner join
                (
                    select
                        *
                    from
                        cards t1
                    inner join
                    (
                        select 
                            * 
                        from 
                            accounts t1
                        inner join
                            clients t2 on t1.client = t2.client_id
                    ) t2 on t1.account = t2.account
                ) t2 on t1.card_num = t2.card_num
            ) t1
            inner join
                terminals t2 on t1.terminal = t2.terminal_id 
    ) t2
    on t1.account = t2.account and t1.terminal_city <> t2.terminal_city 
    where t1.transaction_date between (:data_from) and (:data_to) 
    and t2.transaction_date between (:data_from) and (:data_to)
    and julianday(t1.transaction_date) - julianday(t2.transaction_date) < julianday('1980-01-01 01:00:00')-julianday('1980-01-01 00:00:00')
    and julianday(t2.transaction_date) - julianday(t1.transaction_date) < julianday('1980-01-01 01:00:00')-julianday('1980-01-01 00:00:00')
    ''', {"data_from": data_from, "data_to": data_to})
    for row in cursor.fetchall():
        print(row)
    if sqlite_connection:
        sqlite_connection.close()
        print("Соединение с SQLite закрыто")


# 4. Попытка подбора суммы.
def attempt_to_select_the_amount(data_from, data_to):
    sqlite_connection = sqlite3.connect('sber.db')
    cursor = sqlite_connection.cursor()
    print("База данных подключена к SQLite")
    cursor.execute('''
    select
        transaction_date as event_date,
        passport_num as passport,
        last_name, 
        first_name, 
        patronymic, 
        phone as phone, 
        'attempt_to_select_the_amount' as event_type,
        datetime('now') as report_dt
    from
    (
        select 
            lag(transaction_id) over(order by account) as lag_trans,
            lag(transaction_date) over(order by account) as lag_date,
            lag(account) over(order by transaction_id) as lag_acc,
            lag(amount) over(order by transaction_date) as lag_amount,
            transaction_id,
            transaction_date,
            amount,
            account as acc,
            lead(transaction_id) over(order by account) as lead_trans,
            lead(transaction_date) over(order by account) as lead_date,
            lead(account) over(order by transaction_id) as lead_acc,
            lead(oper_result) over(order by account) as oper_res,
            lead(amount) over(order by transaction_date) as lead_amount,
            passport_num,
            last_name, 
            first_name, 
            patronymic, 
            phone
        from 
            transactions t1
        inner join
            (
                select 
                    *
                from 
                    cards t1 
                inner join 
                    (
                        select 
                            * 
                        from 
                            accounts t1 
                        inner join 
                            clients t2 on t1.client = t2.client_id
                    ) t2 on t1.account = t2.account
            ) t2 on t1.card_num = t2.card_num
    )
    where cast(amount as decimal) > cast(lead_amount as decimal)
    and cast(lag_amount as decimal) > cast(amount as decimal)
    and cast(lag_amount as decimal) > cast(lead_amount as decimal)
    and lag_acc = acc 
    and lead_acc = acc
    and lag_acc = lead_acc
    and julianday(lag_date) - julianday(lead_date) < julianday('1980-01-01 00:20:00')-julianday('1980-01-01 00:00:00')
    and julianday(lead_date) - julianday(lag_date) < julianday('1980-01-01 00:20:00')-julianday('1980-01-01 00:00:00')
    and oper_res = 'SUCCESS'
    and lag_date between (:data_from) and (:data_to) 
    and transaction_date between (:data_from) and (:data_to)
    and lead_date between (:data_from) and (:data_to)
            ''', {"data_from": data_from, "data_to": data_to})
    for row in cursor.fetchall():
        print(row)
    if sqlite_connection:
        sqlite_connection.close()
        print("Соединение с SQLite закрыто")


# функция для рандомных запросов
def example():
    sqlite_connection = sqlite3.connect('sber.db')
    cursor = sqlite_connection.cursor()
    print("База данных подключена к SQLite")
    cursor.execute('''
    ''')
    for row in cursor.fetchall():
        print(row)
    if sqlite_connection:
        sqlite_connection.close()
        print("Соединение с SQLite закрыто")


# функция для вывода табличного состава базы данных
def included_tables():
    sqlite_connection = sqlite3.connect('sber.db')
    cursor = sqlite_connection.cursor()
    print("База данных подключена к SQLite")
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    for row in cursor.fetchall():
        print(row)
    if sqlite_connection:
        sqlite_connection.close()
        print("Соединение с SQLite закрыто")

included_tables()
# DDL
# init_data_clients()
# add_scd2_rows_to_init('cards', '2021-03-01 00:00:00', '2021-03-01 23:59:59')
# add_scd2_rows_to_init('clients', '2021-03-01 00:00:00', '2021-03-01 23:59:59')
# add_scd2_rows_to_init('accounts', '2021-03-01 00:00:00', '2021-03-01 23:59:59')
# create_tables_terminals('terminals_01032021.xlsx', '2021-03-01 00:00:00', '2021-03-01 23:59:59')
# create_tables_terminals('terminals_02032021.xlsx', '2021-03-02 00:00:00', '2021-03-02 23:59:59')
# create_tables_terminals('terminals_03032021.xlsx', '2021-03-03 00:00:00', '2021-03-03 23:59:59')
# create_tables_transactions('transactions_01032021.txt', '2021-03-01 00:00:00', '2021-03-01 23:59:59')
# create_tables_transactions('transactions_02032021.txt', '2021-03-02 00:00:00', '2021-03-02 23:59:59')
# create_tables_transactions('transactions_03032021.txt', '2021-03-03 00:00:00', '2021-03-03 23:59:59')
# create_tables_blacklist('passport_blacklist_01032021.xlsx', '2021-03-01 00:00:00', '2021-03-01 23:59:59')
# create_tables_blacklist('passport_blacklist_02032021.xlsx', '2021-03-02 00:00:00', '2021-03-02 23:59:59')
# create_tables_blacklist('passport_blacklist_03032021.xlsx', '2021-03-03 00:00:00', '2021-03-03 23:59:59')

# DML. Решил не создавать представлений, просто вывожу витрину в консоль.
# 1A task. Разбил первую задачу на 2 подзадачи.
# Мошенник Пначин Я.
# locked_passport()
# 1B task
# Мошенник Приемский Е.
# overdue_passport()
# 2 task
# Мошенник Узенева Н.
# invalid_contract()
# 3 task. Решил, что лучше будет посмотреть за каждый день в отдельности.
# 1 день(Приемский Е.).
# 2 день(Шкабкина А.).
# 3 день(Рубакова О.).
# different_cities('2021-03-01 00:00:00', '2021-03-01 23:59:59')
# different_cities('2021-03-02 00:00:00', '2021-03-02 23:59:59')
# different_cities('2021-03-03 00:00:00', '2021-03-03 23:59:59')
# 4 task. Также решил смотреть за каждый день. Нужный период передаю в виде аргумента в функцию.
# В 1 день найден мошенник, в последующие два их не найдено.(Пначин Я.).
# attempt_to_select_the_amount('2021-03-01 00:00:00', '2021-03-01 23:59:59')
# attempt_to_select_the_amount('2021-03-02 00:00:00', '2021-03-02 23:59:59')
# attempt_to_select_the_amount('2021-03-03 00:00:00', '2021-03-03 23:59:59')
