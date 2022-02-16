import pandas as pd


def load_data(input_file, output_file_name):
    excel_data = pd.read_excel(input_file, engine='openpyxl')

    data_for_load = pd.DataFrame(excel_data,
                                 columns=['trans_id', 'date', 'card', 'account', 'account_valid_to', 'client',
                                          'last_name', 'first_name', 'patronymic', 'date_of_birth', 'passport',
                                          'passport_valid_to', 'phone', 'oper_type', 'amount', 'oper_result',
                                          'terminal', 'terminal_type', 'city', 'address']).dropna(how='all')

    with open(output_file_name, 'w') as file:
        file.write("BEGIN;\n")

        for index, row in data_for_load.iterrows():
            file.write(f"    INSERT INTO training.srs_load (trans_id, date, card, account, account_valid_to, client, "
                       f"last_name, first_name, patronymic, date_of_birth, passport, passport_valid_to, phone, "
                       f"oper_type, amount, oper_result, terminal, terminal_type, city, address) "
                       f"VALUES ('{row['trans_id']}', timestamp '{row['date']}', '{row['card']}', '{row['account']}', "
                       f"timestamp '{row['account_valid_to']}', '{row['client']}', '{row['last_name']}', "
                       f"'{row['first_name']}', '{row['patronymic']}', timestamp '{row['date_of_birth']}', "
                       f"'{row['passport']}', timestamp '{row['passport_valid_to']}', '{row['phone']}', "
                       f"'{row['oper_type']}', {row['amount']}, '{row['oper_result']}', '{row['terminal']}', "
                       f"'{row['terminal_type']}', '{row['city']}', '{row['address']}');\n")

        file.write("    COMMIT;\n")
        file.write("END;\n")


if __name__ == '__main__':
    in_file1 = "transactions_01052020.xlsx"
    in_file2 = "transactions_02052020.xlsx"
    in_file3 = "transactions_03052020.xlsx"

    out_file1 = "load_data1.sql"
    out_file2 = "load_data2.sql"
    out_file3 = "load_data3.sql"

    load_data(in_file1, out_file1)
    load_data(in_file2, out_file2)
    load_data(in_file3, out_file3)


