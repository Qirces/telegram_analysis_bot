import sys

import pandas as pd
from PyQt6.QtCore import pyqtSlot, Qt
from PyQt6.QtGui import QPixmap
from PyQt6.QtWidgets import *
from EDA import *


def data_analysis(path: str, parent):
    df = None
    try:
        if path.split('.')[::-1][0] == "xlsx":
            print("Начал чтение")
            df = pd.read_excel(path)
            print("Закончил чтение")
        elif path.split('.')[::-1][0] == "csv":
            print("Начал чтение")
            df = pd.read_csv(path)
            print("Закончил чтение")

        parent.hide()
        parent.a_w = AnalysisWindow(parent, df)
        parent.a_w.show()

    except Exception as e:
        print(e)


class OneFactorAnalysisWindow(QWidget):
    def __init__(self, df):
        super().__init__()
        self.df = df
        layout = QGridLayout(self)
        self.lbl_cols = QLabel()
        self.lbl_cols.setText("Выберите столбец для построения графика")
        self.cols = QComboBox()
        self.cols.setMinimumSize(50,30)
        self.cols.addItems(self.df.columns)

        self.graphs = QComboBox()
        self.graphs.setMinimumSize(50,30)
        self.lbl_graphs = QLabel()
        self.lbl_graphs.setText("Выберите, какой тип графика нужно построить")
        self.graphs.addItems(["Гистограмма", "Ящик с усами"])

        layout.addWidget(self.lbl_cols,0,0)
        layout.addWidget(self.cols,0,1)
        layout.addWidget(self.lbl_graphs,1,0)
        layout.addWidget(self.graphs,1,1)

        butn = QPushButton(parent = self, text = "Построить график")
        butn.clicked.connect(self.createGraph)

        layout.addWidget(butn)

        self.setLayout(layout)

    @pyqtSlot()
    def createGraph(self):
        try:
            factor_analysis(self.df, self.cols.currentText(), self.graphs.currentText())
        except Exception as e:
            print(e)

class TwoFactorAnalysisWindow(QWidget):
    def __init__(self, df):
        super().__init__()
        self.df = df
        layout = QGridLayout(self)

        self.lbl_cols = QLabel()
        self.lbl_cols.setText("Выберите первый столбец для построения графика")
        self.cols = QComboBox()
        self.cols.setMinimumSize(50,30)
        self.cols.addItems(self.df.columns)


        self.lbl_cols2 = QLabel()
        self.lbl_cols2.setText("Выберите второй столбец для построения графика")
        self.cols2 = QComboBox()
        self.cols2.setMinimumSize(50,30)
        self.cols2.addItems(self.df.columns)

        self.graphs = QComboBox()
        self.graphs.setMinimumSize(50,30)
        self.lbl_graphs = QLabel()
        self.lbl_graphs.setText("Выберите, какой тип графика нужно построить")
        self.graphs.addItems(["Диаграмма рассеяния и корреляция", "Heatmap"])

        layout.addWidget(self.lbl_cols,0,0)
        layout.addWidget(self.cols,0,1)
        layout.addWidget(self.lbl_cols2,1,0)
        layout.addWidget(self.cols2,1,1)
        layout.addWidget(self.lbl_graphs,2,0)
        layout.addWidget(self.graphs,2,1)

        butn = QPushButton(parent = self, text = "Построить график")
        butn.clicked.connect(self.createGraph)

        layout.addWidget(butn)

        self.setLayout(layout)

    @pyqtSlot()
    def createGraph(self):
        try:
            factor_analysis(self.df, [self.cols.currentText(), self.cols2.currentText()], self.graphs.currentText())
        except Exception as e:
            print(e)


class ClassicAnalysisWindow(QWidget):
    def __init__(self, df: pd.DataFrame):
        super().__init__()

        self.text = QPlainTextEdit()
        self.text.setReadOnly(True)
        self.text.setPlainText(classic_analyis(df))
        self.layout = QGridLayout()
        self.layout.addWidget(self.text)
        self.setLayout(self.layout)
        self.setMinimumSize(600, 300)


class AnalysisWindow(QWidget):
    parent = None

    def closeEvent(self, e):
        self.parent.show()
        e.accept()

    def __init__(self, parent, df):
        super().__init__()

        self.parent = parent

        self.tabs = QTabWidget()
        self.tabs.setMovable(True)

        self.analysis_windows = [OneFactorAnalysisWindow(df), TwoFactorAnalysisWindow(df), ClassicAnalysisWindow(df)]

        for n in enumerate(["Одномерный анализ", "Двумерный Анализ", "Классическая статистика"]):
            self.tabs.addTab(self.analysis_windows[n[0]], n[1])

        self.layout = QVBoxLayout()
        self.layout.addWidget(self.tabs)
        self.setLayout(self.layout)
        self.setMinimumSize(600, 300)


class StartWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        layout = QVBoxLayout()

        label = QLabel("Выберите файл с данными (csv, xlsx)")
        font = label.font()
        font.setPointSize(30)
        label.setFont(font)
        label.setAlignment(Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignVCenter)

        btn = QPushButton(parent = self, text = "Open file dialog")
        btn.clicked.connect(self.open_dialog)

        layout.addWidget(label)
        layout.addWidget(btn)

        widget = QWidget()
        widget.setLayout(layout)

        self.setCentralWidget(widget)

    @pyqtSlot()
    def open_dialog(self):
        fname = QFileDialog.getOpenFileName(
            self,
            "Open File",
            "${HOME}",
            "csv Files (*.csv);; xlsx Files (*.xlsx)",
        )
        try:
            data_analysis(fname[0], self)
        except Exception as e:
            print(e)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.main_gui = StartWindow()
    app.main_gui.setMinimumSize(300, 300)
    app.main_gui.show()
    sys.exit(app.exec())
