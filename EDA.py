import textwrap

import matplotlib
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import datetime


def wrap_labels(labels, wrap_length):
    return ['\n'.join(textwrap.wrap(label, wrap_length)) for label in labels]

def factor_analysis(df_t_a: pd.DataFrame, cols, graph_type: str, user: str, file: str):
    matplotlib.use('Agg')
    save_path = ""
    plt.clf()
    cols2 = []

    for c in cols:
        cols2.append(c.replace(" ", "_").replace(":", "_"))
    if (str(file).find("user_", 0) == -1):
        file_name = str(file).split('=')[1].split('\'')[0].replace(" ", "_").replace(":", "_")
    else:
        file_name = "db" + str(file).split("_")[2]

    if graph_type == "Гистограмма":
        df_t_a[~df_t_a[cols[0]].isnull()][cols[0]].hist()
        save_path = f'D:/Diplom/pics/{str(user)}_{str(file_name)}_{cols2[0]}_hist.png'
        plt.xlabel(str(cols[0]))
        plt.savefig(save_path)

    if graph_type == "Ящик с усами":
        try:
            df_t_a[cols[0]] = pd.to_numeric(df_t_a[cols[0]].str.replace(',','.'), errors="coerce")
        except Exception as e:
            print(e)
        plt.boxplot(df_t_a[~df_t_a[cols[0]].isnull()][cols[0]])
        save_path = f'D:/Diplom/pics/{str(user)}_{str(file_name)}_{cols2[0]}_boxplot.png'
        plt.xlabel(str(cols[0]))
        plt.savefig(save_path)

    if graph_type == "Тепловая карта":
        df_r = df_t_a.copy()
        for c in cols:
            try:
                df_r[f"{c}"] = pd.to_numeric(df_r[f"{c}"].str.replace(',', '.'), errors="coerce")
            except Exception as e:
                print(e)

        fig, ax = plt.subplots(figsize=(10, 10))
        corr_matrix = df_r[cols].corr()
        im = ax.imshow(corr_matrix, cmap="hot", vmin=0, vmax=1)

        for i in range(len(corr_matrix.columns)):
            for j in range(len(corr_matrix.columns)):
                text = ax.text(j, i, round(corr_matrix.iloc[i, j], 2),
                               ha="center", va="center", color="black")

        cbar = ax.figure.colorbar(im, ax=ax, ticks=[0, 0.5, 1])
        cbar.ax.set_yticklabels(['Low', 'Medium', 'High'])

        wrapped_labels = wrap_labels(corr_matrix.columns, wrap_length=15)

        ax.set_xticks(range(len(wrapped_labels)))
        ax.set_yticks(range(len(wrapped_labels)))
        ax.set_xticklabels(wrapped_labels)
        ax.set_yticklabels(wrapped_labels)
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right", rotation_mode="anchor")
        ax.set_title("Корреляционная матрица")

        str_cols = ''
        for c in cols2:
            str_cols = str_cols + c + "_"
        save_path = f'D:/Diplom/pics/{str(user)}_{str(file_name)}_heatmap.png'
        plt.savefig(save_path)

    if graph_type == "Диаграмма рассеяния и корреляция":

        df_r = df_t_a.copy()
        for c in cols:
            try:
                df_r[f"{c}"] = pd.to_numeric(df_r[f"{c}"].str.replace(',', '.'), errors="coerce")
            except Exception as e:
                print(e)

        plt.scatter(df_r[~df_r[cols].isnull()][cols[0]], df_r[~df_r[cols].isnull()][cols[1]])
        plt.xlabel(cols[0])
        plt.ylabel(cols[1])
        plt.title(
            f'Диаграмма рассеяния.\nКорреляция = {df_r[~df_r[cols].isnull()][cols[0]].corr(df_r[~df_r[cols].isnull()][cols[1]])}')

        save_path = f'D:/Diplom/pics/{str(user)}_{str(file_name)}_{cols2[0]}_{cols2[1]}_corr.png'
        plt.savefig(save_path)

    return save_path


def classic_analyis(df_t_a: pd.DataFrame):
    text = ""
    df2 = df_t_a.describe()

    for c in df2.columns:
        text = text + f"Статистика столбца {c} :\n"
        text = text + f"\tКол-во null : {df_t_a.isnull().sum()[c]}\n"
        text = text + f"\tСреднее : {df2[c]["mean"]}\n"
        text = text + f"\tМинимум : {df2[c]["min"]}\n"
        text = text + f"\tМаксимум : {df2[c]["max"]}\n"
        text = text + f"\tМедиана : {df2[c]["50%"]}\n"
        text = text + f"\tМода : {df_t_a[c].mode()[0]}\n\n"
    return text


if __name__ == "__main__":
    cols = ['MOEX_end_of_month', 'MOEX_max', 'MOEX_min', 'Urals_Crude_Oil_Price_per_1_Barrel__Russia', 'CPI', 'CPI_(SA)', 'Labour_Force_Demand__Russia', 'Labour_Force_Demand__Russia_(SA)', 'Real_wages', 'Real_wages_(SA)', 'Freight_Turnover__Russia', 'Freight_Turnover__Russia_(SA)', 'Passenger_Turnover__Russia', 'Passenger_Turnover__Russia_(SA)']
    print(cols.__str__())
    #df = pd.read_csv("tinkoff_legal_entities_new.csv")
    #print(df.describe())
    #factor_analysis(df,["bad_rep_tcb", 'rating_avito'], "Тепловая карта", "1412", "1=1412")

    # print(classicAnalyis(df))
    # print(df.dtypes)
    # print(df.select_dtypes(exclude = [pd.DatetimeTZDtype]))

    #conn = psycopg2.connect(dbname="bibot", user="postgres", password="qirces", host="127.0.0.1")
    #cursor = conn.cursor()
    #conn.autocommit = True
    # команда для создания базы данных metanit
    #sql = "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
    query = "SELECT * FROM user_1277945121_tinkoff_legal_entities_new_csv"
    engine = create_engine('postgresql+psycopg2://postgres:qirces@127.0.0.1/bibot')
    df = pd.read_sql(query, con=engine, )
    print(df.columns)
    """sql = DO
$$
DECLARE
    _tableName TEXT;
BEGIN
    FOR _tableName IN SELECT tablename FROM pg_tables WHERE tablename LIKE '1%'
    LOOP
        -- Подготовка к удалению таблиц.
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(_tableName) || ' CASCADE';
    END LOOP;
END;
$$;"""
    # выполняем код sql
    #cursor.execute(sql)
    #print(cursor.fetchall())
    #cursor.close()
    #conn.close()
