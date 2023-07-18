import json

import pandas as pd

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 2000)

file_default = 'trial_task.json'


def get_json_from_file(file: str = file_default) -> dict:
    """Возвращает загруженный json как словарь."""
    with open(file, encoding='UTF-8') as json_file:
        data = json.load(json_file)
    return data


def get_dataframe_from_file(file: str = file_default) -> pd:
    """Возвращает объект dataframe pandas."""
    with open(file, encoding='UTF-8') as json_file:
        data = json.load(json_file)
    return pd.json_normalize(data, record_path=['products'],
                             meta=['order_id', 'warehouse_name', 'highway_cost'])


def get_dict_tariff_warehouse() -> dict:
    """Возвращает словарь с названием склада и тарифом склада."""
    all_warehouse = dict()
    for dct in get_json_from_file():
        if dct['warehouse_name'] not in all_warehouse:
            quantity = sum([product['quantity'] for product in dct['products']])
            tariff = abs(dct['highway_cost']) / quantity
            all_warehouse[dct['warehouse_name']] = tariff

    return all_warehouse


def add_tariff(row):
    """Добавляет тариф склада к таблице."""
    all_warehouse = get_dict_tariff_warehouse()
    return all_warehouse[row['warehouse_name']]


def tariff_warehouse():
    """Найти тариф стоимости доставки для каждого склада."""
    return pd.Series(get_dict_tariff_warehouse())


def get_statistic_product():
    """
    Найти суммарное количество, суммарный доход, суммарный расход и суммарную
    прибыль для каждого товара (представить как таблицу со столбцами
    'product', 'quantity', 'income', 'expenses', 'profit')
    """
    df = get_dataframe_from_file()
    df['tariff_warehouse'] = df.apply(add_tariff, axis=1)
    df = df.assign(income=df['price'] * df['quantity'],
                   expenses=df['tariff_warehouse'] * df['quantity'])
    df['profit'] = df.apply(lambda row: row.income - row.expenses, axis=1)
    df.drop(columns=['order_id', 'price', 'warehouse_name', 'highway_cost',
                     'tariff_warehouse'],
            axis=1,
            inplace=True)

    return df.groupby('product').sum().reset_index()


def get_order_profit():
    """
    Составить табличку со столбцами 'order_id' (id заказа) и 'order_profit'
    (прибыль полученная с заказа). А также вывести среднюю прибыль заказов.
    """
    df = get_dataframe_from_file()
    df['tariff_warehouse'] = df.apply(add_tariff, axis=1)
    df = df.assign(income=df['price'] * df['quantity'],
                   expenses=df['tariff_warehouse'] * df['quantity'])
    df['profit'] = df.apply(lambda row: row.income - row.expenses, axis=1)

    df.drop(columns=['product', 'price', 'quantity', 'warehouse_name',
                     'highway_cost', 'tariff_warehouse', 'income', 'expenses'],
            axis=1,
            inplace=True)
    df.rename(columns={'profit': 'order_profit'}, inplace=True)
    return df.groupby('order_id').sum().reset_index()


def percent_profit_product_of_warehouse():
    """
    Составить табличку типа 'warehouse_name' , 'product','quantity', 'profit',
    'percent_profit_product_of_warehouse' (процент прибыли продукта заказанного
     из определенного склада к прибыли этого склада)
    """
    df = get_dataframe_from_file()
    df['tariff_warehouse'] = df.apply(add_tariff, axis=1)
    df = df.assign(income=df['price'] * df['quantity'],
                   expenses=df['tariff_warehouse'] * df['quantity'])
    df['profit'] = df.apply(lambda row: row.income - row.expenses, axis=1)
    df.drop(columns=['order_id', 'price', 'highway_cost',
                     'tariff_warehouse', 'income', 'expenses'],
            axis=1,
            inplace=True)
    df2 = df.groupby(['warehouse_name', 'product']).sum().reset_index()

    df3 = df2.groupby(['warehouse_name']).sum().reset_index()
    df3.drop(columns=['product', 'quantity'], axis=1, inplace=True)

    tmp_dct = {row['warehouse_name']: row['profit'] for _, row in df3.iterrows()}

    df2['percent_profit_product_of_warehouse'] = df2.apply(
        lambda row: (row.profit / tmp_dct[row['warehouse_name']]) * 100,
        axis=1)

    return df2


def percent_profit_product_of_warehouse_2():
    """
    Взять предыдущую табличку и отсортировать 'percent_profit_product_of_warehouse'
    по убыванию, после посчитать накопленный процент. Накопленный процент - это
    новый столбец в этой табличке, который должен называться
    'accumulated_percent_profit_product_of_warehouse'. По своей сути это
    постоянно растущая сумма отсортированного по убыванию
    столбца 'percent_profit_product_of_warehouse'.
    """
    df = percent_profit_product_of_warehouse()
    df_sort = df.sort_values(by=['warehouse_name',
                                 'percent_profit_product_of_warehouse'],
                             ascending=[False, True])

    df_sort['accumulated_percent_profit_product_of_warehouse'] = df_sort.groupby(
        ['warehouse_name'])['percent_profit_product_of_warehouse'].cumsum()
    return df_sort


def table_with_category():
    """
    Присвоить A,B,C - категории на основании значения накопленного процента
    ('accumulated_percent_profit_product_of_warehouse'). Если значение
    накопленного процента меньше или равно 70, то категория A.
    Если от 70 до 90 (включая 90), то категория Б. Остальное - категория C.
    Новый столбец обозначить в таблице как 'category'
    """
    df = percent_profit_product_of_warehouse_2()

    def case_category(row):
        val = row['accumulated_percent_profit_product_of_warehouse']
        if val <= 70:
            return 'A'
        elif 70 < val <= 90:
            return 'B'
        else:
            return 'C'

    df['category'] = df.apply(case_category, axis=1)

    return df


if __name__ == '__main__':
    pass
    print(tariff_warehouse())
    # print(get_statistic_product())
    # print(get_order_profit())
    # print(percent_profit_product_of_warehouse())
    # print(percent_profit_product_of_warehouse_2())
    # print(table_with_category())
