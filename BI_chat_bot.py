import pandas
import telebot
from telebot import util, types
import psycopg2
from pyunpack import Archive

from EDA import *


class TelebotBI(telebot.TeleBot):
    token = "7467312401:AAEbp7_It8bGovDne1LxYiYM7jsWMy2bqIY"

    def __init__(self):
        super().__init__(self.token)

        self.bot = telebot.TeleBot(self.token)
        self.files = {}
        self.file_names = []
        self.current_file: str = ""
        self.df: pandas.DataFrame = None
        self.cur_df_cols = []

        self.engine = create_engine('postgresql+psycopg2://postgres:qirces@127.0.0.1/bibot')
        self.tables_list = []
        self.conn2 = psycopg2.connect(dbname="bibot", user="postgres", password="qirces", host="127.0.0.1")

        @self.bot.message_handler(commands=['start'])
        def button_message(message):
            text = f"""Здравствуйте,{message.from_user.username}!
            \nПоддерживаемые форматы файлов: xlsx, csv.
            \nПосле первичной загрузки файлов они сохранятся в базе данных, и вы можете работать с ними без повторной загрузки.
            \nДоступ к командам осуществляется с помощью кнопок."""

            markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
            item2 = types.KeyboardButton("/выбрать_файл")
            markup.add(item2)
            item3 = types.KeyboardButton("/статистика")
            markup.add(item3)
            item4 = types.KeyboardButton("/гистограмма")
            markup.add(item4)
            item5 = types.KeyboardButton("/ящик_с_усами")
            markup.add(item5)
            item6 = types.KeyboardButton("/тепловая_карта")
            markup.add(item6)
            item7 = types.KeyboardButton("/корреляция_и_диаграмма_рассеяния")
            markup.add(item7)
            item8 = types.KeyboardButton("/выбрать_файл_из_бд")
            markup.add(item8)
            self.bot.send_message(message.chat.id, text=text, reply_markup=markup)

        @self.bot.message_handler(content_types=['document'])
        def add_file(message):
            try:
                file_name = message.document.file_name
                file_info = self.bot.get_file(message.document.file_id)
                downloaded_file = self.bot.download_file(file_info.file_path)
                with open(file_name, 'wb') as new_file:
                    new_file.write(downloaded_file)
                    self.files.update({file_name: new_file})
                    self.file_names = self.file_names + [file_name]
                    self.bot.send_message(message.chat.id, text=f"Файл: {file_name} успешно загружен")
            except Exception as e:
                self.bot.send_message(message.chat.id, text=e)

        @self.bot.message_handler(commands=['выбрать_файл'])
        def choose_file_handler(message):
            try:
                if len(self.files.keys()) == 0:
                    self.bot.send_message(message.chat.id, text=f"Сначала загрузите файл")
                else:
                    text = "Выберите файл для операций:"
                    count = 1
                    for i in self.file_names:
                        text = text + f"\n\t{count}) {i}"
                        count = count + 1
                    self.bot.send_message(message.chat.id, text=text)
                    self.bot.register_next_step_handler(message, create_df)
            except Exception as e:
                self.bot.send_message(message.chat.id, text=str(e))

        @self.bot.message_handler(commands=['выбрать_файл_из_бд'])
        def choose_file_db_handler(message):
            try:
                cursor2 = self.conn2.cursor()
                sql2 = f"SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name LIKE 'user_{message.chat.id}%'"
                cursor2.execute(sql2)
                tables = cursor2.fetchall()
                text = "Выберите таблицу из существующих:"
                count = 1
                for t in tables:
                    t_split = str(t).split('\'')[1].split("_")[2:]
                    t_name = ""
                    for s in t_split:
                        t_name = t_name + s + "_"
                    t_name = t_name[:-1]
                    text = text + f"\n\t{count}) {t_name}"
                    self.tables_list.append(str(t).split('\'')[1])
                    count = count + 1
                splitted_text = util.smart_split(text, 3000)
                for i in splitted_text:
                    self.bot.send_message(message.chat.id, text=i)
                self.bot.register_next_step_handler(message, create_df_from_db)
                cursor2.close()
            except Exception as e:
                self.bot.send_message(message.chat.id, text=str(e))

        @self.bot.message_handler(commands=['create_df_from_db'])
        def create_df_from_db(message):
            try:
                table = self.tables_list[int(message.text) - 1]
                table_split = str(table).split("_")[2:]
                table_name = ""
                for s in table_split:
                    table_name = table_name + s + "_"
                table_name = table_name[:-1]
                self.bot.send_message(message.chat.id, text=f"Вы работаете с файлом: {table_name}")
                query = f"""SELECT * FROM {table};"""
                self.df = pd.read_sql(query, con=self.engine, )
                self.cur_df_cols = self.df.columns
                self.current_file = str(table)

            except Exception as e:
                self.bot.send_message(message.chat.id, text=str(e))

        @self.bot.message_handler(commands=['create_df'])
        def create_df(message):
            try:
                file = self.file_names[int(message.text) - 1]
                self.current_file = self.files[file]
                self.bot.send_message(message.chat.id, text=f"Создаю датафрейм из : {file}")
                if file.split('.')[::-1][0] == "xlsx":
                    self.df = pd.read_excel(file)
                elif file.split('.')[::-1][0] == "csv":
                    self.df = pd.read_csv(file, on_bad_lines='skip', sep = ";")
                self.cur_df_cols = self.df.columns
                self.bot.send_message(message.chat.id, text=f"Датафрейм создан")
                r_file_name = str(file).replace(".", "_").replace(",", "_").replace("-", "_").replace(" ", "_")
                self.df.to_sql(f'user_{str(message.chat.id)}_{r_file_name}', con=self.engine,
                               if_exists='replace', index=False)
            except Exception as e:
                self.bot.send_message(message.chat.id, text=str(e))

        @self.bot.message_handler(commands=['статистика'])
        def general_statistics_handler(message):
            try:
                if self.df is None:
                    self.bot.send_message(message.chat.id, text=f"Сначала выберите файл для анализа")
                else:
                    text = classic_analyis(self.df)
                    splitted_text = util.smart_split(text, 3000)
                    for i in splitted_text:
                        self.bot.send_message(message.chat.id, text=i)
            except Exception as e:
                self.bot.send_message(message.chat.id, text=str(e))

        @self.bot.message_handler(commands=['гистограмма'])
        def bar_chart_handler(message):
            try:
                if self.df is None:
                    self.bot.send_message(message.chat.id, text=f"Сначала выберите файл для анализа")
                else:
                    count = 1
                    text = "Выберите колонку для построения гистограммы:\n"
                    for c in self.cur_df_cols:
                        text = text + f"\n\t{count}) {c}"
                        count = count + 1
                    self.bot.send_message(message.chat.id, text=text)
                    self.bot.register_next_step_handler(message, bar_chart)
            except Exception as e:
                self.bot.send_message(message.chat.id, text=str(e))

        @self.bot.message_handler(commands=['bar_chart'])
        def bar_chart(message):
            try:
                send_cols = str(message.text).split(' ')
                cols = []
                for i in send_cols:
                    cols.append(self.cur_df_cols[int(i) - 1])
                path = factor_analysis(self.df, cols, "Гистограмма", message.chat.id, self.current_file)
                photo = open(f'{path}', 'rb')
                self.bot.send_photo(message.chat.id, photo)
                self.bot.send_photo(message.chat.id, "FILEID")
            except Exception as e:
                print(e)

        @self.bot.message_handler(commands=['ящик_с_усами'])
        def box_plot_handler(message):
            try:
                if self.df is None:
                    self.bot.send_message(message.chat.id, text=f"Сначала выберите файл для анализа")
                else:
                    count = 1
                    text = "Выберите колонку для построения ящика с усами:\n"
                    for c in self.cur_df_cols:
                        text = text + f"\n\t{count}) {c}"
                        count = count + 1
                    self.bot.send_message(message.chat.id, text=text)
                    self.bot.register_next_step_handler(message, box_plot)
            except Exception as e:
                self.bot.send_message(message.chat.id, text=str(e))

        @self.bot.message_handler(commands=['box_plot'])
        def box_plot(message):
            try:
                send_cols = str(message.text).split(' ')
                cols = []
                for i in send_cols:
                    cols.append(self.cur_df_cols[int(i) - 1])
                path = factor_analysis(self.df, cols, "Ящик с усами", message.chat.id, self.current_file)
                photo = open(f'{path}', 'rb')
                self.bot.send_photo(message.chat.id, photo)
                self.bot.send_photo(message.chat.id, "FILEID")
            except Exception as e:
                print(e)

        @self.bot.message_handler(commands=['корреляция_и_диаграмма_рассеяния'])
        def bar_chart_handler(message):
            try:
                if self.df is None:
                    self.bot.send_message(message.chat.id, text=f"Сначала выберите файл для анализа")
                else:
                    count = 1
                    text = "Перечислите через пробел 2 колонки для построения диаграммы рассеяния и расчёта корреляции:\n"
                    for c in self.cur_df_cols:
                        text = text + f"\n\t{count}) {c}"
                        count = count + 1
                    self.bot.send_message(message.chat.id, text=text)
                    self.bot.register_next_step_handler(message, correlation)
            except Exception as e:
                self.bot.send_message(message.chat.id, text=str(e))

        @self.bot.message_handler(commands=['correlation'])
        def correlation(message):
            try:
                send_cols = str(message.text).split(' ')
                cols = []
                for i in send_cols:
                    cols.append(self.cur_df_cols[int(i) - 1])
                path = factor_analysis(self.df, cols, "Диаграмма рассеяния и корреляция", message.chat.id,
                                       self.current_file)
                photo = open(f'{path}', 'rb')
                self.bot.send_photo(message.chat.id, photo)
                self.bot.send_photo(message.chat.id, "FILEID")
            except Exception as e:
                print(e)

        @self.bot.message_handler(commands=['тепловая_карта'])
        def bar_chart_handler(message):
            try:
                if self.df is None:
                    self.bot.send_message(message.chat.id, text=f"Сначала выберите файл для анализа")
                else:
                    count = 1
                    text = "Перечислите через пробел колонки для расчёта тепловой карты"
                    for c in self.cur_df_cols:
                        text = text + f"\n\t{count}) {c}"
                        count = count + 1
                    self.bot.send_message(message.chat.id, text=text)
                    self.bot.register_next_step_handler(message, heatmap)
            except Exception as e:
                self.bot.send_message(message.chat.id, text=str(e))

        @self.bot.message_handler(commands=['heatmap'])
        def heatmap(message):
            try:
                send_cols = str(message.text).split(' ')
                cols = []
                for i in send_cols:
                    cols.append(self.cur_df_cols[int(i) - 1])
                path = factor_analysis(self.df, cols, "Тепловая карта", message.chat.id,
                                       self.current_file)
                photo = open(f'{path}', 'rb')
                self.bot.send_photo(message.chat.id, photo)
                self.bot.send_photo(message.chat.id, "FILEID")
            except Exception as e:
                print(e)

        self.bot.polling(none_stop=True)


if __name__ == "__main__":
    TelebotBI()
