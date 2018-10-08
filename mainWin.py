# coding:utf-8
# 界面的逻辑功能函数
import sys,os
# sys.path.append(r'..\.')
import pandas as pd
import numpy as np
import json
from mainWinUI import Ui_MainWindow
from deal_taizhang import deal_taizhang
# from myLogging import myLogging
import myLogging as mylogger
from PyQt5.QtWidgets import QApplication, QMainWindow
from PyQt5.QtWidgets import QDesktopWidget,QFileDialog,QTableWidgetItem,QMainWindow
from PyQt5.QtCore import Qt

# Ui_MainWindow
class MyMainWindow(QMainWindow,Ui_MainWindow):
    def __init__(self,parent=None):
        super(MyMainWindow,self).__init__(parent)
        # self.logger = myLogging()
        self.deal_taizhang = deal_taizhang()
        # self.townList = ''
        self.A = ''
        self.A_flag = False
        self.zy = ''
        self.zy_flag = False
        self.townTable = ''
        self.setupUi(self)
        self.initUI()

        self.now_show_table = ''
        # self.openTownList()
        self.connectSlot()
        mylogger.logger.debug('mainWin>init ok')


    # 重新设置UI界面
    def initUI(self):
        self.splitter.setStretchFactor(1,3)
        self.splitter_2.setStretchFactor(0,1)
        # self.setWindowFlags(Qt.FramelessWindowHint)


    # 为组件绑定事件
    def connectSlot(self):
        # self.actionimportFile = QtWidgets.QAction("导入文件", self, triggered=self.openFile)
        self.actionimportFile.triggered.connect(self.showInTable)
        self.open_tableA.clicked.connect(self.setTableA)
        self.open_zy.clicked.connect(self.setZy)
        self.xz_comboBox.currentIndexChanged.connect(self.townSelectChange)
        self.generateTz.clicked.connect(self.genTz)
        self.import_townList.clicked.connect(self.openTownList)
        self.export_taizhang.clicked.connect(self.save_to_csv)

    def showMaximized(self):
        desktop = QApplication.desktop()
        rect = desktop.availableGeometry()
        self.setGeometry(rect)
        # self.show()
    # 打开csv文件 返回DataFrame对象
    def read_csv(self,path):
        with open(path, 'r') as f:
            mylogger.logger.debug('mainWin>openFile:' + path)
            file = pd.read_csv(f, header=0)
        return file


    def openFile(self):
        # print("openFile")
        fileName1, filetype = QFileDialog.getOpenFileName(self,
                                                          "选取文件",
                                                          os.getcwd(),
                                                          "All Files (*);;Text Files (*.txt)")  # 设置文件扩展名过滤,注意用双分号间隔

        # print("filename>",fileName1,"<",type(fileName1),"len:",fileName1)
        mylogger.logger.debug("mainWin>function:openFile:%s"%fileName1)
        # 若没有选中文件
        if fileName1.strip() != "":
            # print(fileName1, filetype)
            self.file_list.addItem(fileName1)

            try:
                # print("try")
                mylogger.logger.debug("mainWin>function:openFile:try")
                df = self.read_csv(fileName1)
                return df,True
                # with open(fileName1,'r') as f:
                #     # print("openfile")
                #     df = pd.read_csv(f, header=0)
                #     # print("read")
                #     return df,True
                    # self.showInTable(df)

            except e:
                mylogger.logger.error("mainWin>function:openFile:exception")
                # print("openFile Error",e)
                return '',False
        else:
            self.statusbar.showMessage("未选择文件")
            return '',False

    def openTownList(self):
        # file_path = "D:/Document/Code/Python/AuditingApp/src/输入文件夹/小区名录310151.18.csv"
        # self.read_csv(file_path)
        self.townTable,flag = self.openFile()
        mylogger.logger.debug("openTownList openFile")
        if flag == False:
            mylogger.logger.debug("openTownList 未选中文件")
        else:
            mylogger.logger.debug("in openTownList else")
            self.townTable = self.townTable.drop(0)
            townList = self.townTable["townName"].drop_duplicates()
            self.xz_comboBox.addItems(townList)
            self.townSelectChange()
            # print("townList:",townList)
            mylogger.logger.debug('mainWin>function:openTownList')

    def townSelectChange(self):
        mylogger.logger.debug("in townSelectChange")
        town = self.xz_comboBox.currentText()
        community = self.townTable[self.townTable["townName"] == town]
        communityList = community["vName"].drop_duplicates()
        self.xq_comboBox.clear()
        self.xq_comboBox.addItems(communityList)

    def getCommunityCode(self):
        communityName = self.xq_comboBox.currentText()
        communityList = self.townTable[self.townTable["vName"] == communityName]
        communityCode = communityList["vID"].values[0]
        mylogger.logger.debug('mainWin>function:getCommunityCode')
        return communityCode

    def setTableA(self):
        # print('setA')
        mylogger.logger.debug('mainWin>function:setTableA')
        self.A,self.A_flag = self.openFile()
    def setZy(self):
        # print('setzy')
        mylogger.logger.debug('mainWin>function:setTableZy')
        self.zy,self.zy_flag = self.openFile()
    def genTz(self):
        # print(self.A,type(self.A))
        if self.xq_comboBox.currentText() == '':
            self.statusbar.showMessage("请先导入小区名录")
        else:
            if self.A_flag == False or self.zy_flag == False:
                print("请先导入A表与账页表")
                self.statusbar.showMessage("请先导入A表与账页表")
            else:
                print('生成台账')
                self.statusbar.showMessage("生成台账中...")
                communityCode = self.getCommunityCode()
                # print("communityCode:",communityCode)
                self.now_show_table = self.deal_taizhang.getCommunity(communityCode,self.A,self.zy)
                self.statusbar.showMessage("生成台账成功")
                mylogger.logger.debug("获取到生成台账结果")
                self.showData(self.now_show_table)
            # deal_taizhang.spliteFamily(self.A,self.zy)

    def showInTable(self):
        self.statusbar.showMessage("正在打开文件")
        df,flag = self.openFile()
        # print(pd.isnull(df))
        if flag == False:
            print("空文件")
        else:
            self.statusbar.showMessage("获取数据中，请稍等")
            print('getData')
            # 获取表头
            df = df.fillna('')
            header = df.columns.values.tolist()  # [str(col) for col in df]
            # 获取表的行列数
            colCount = df.columns.size
            rowCount = df.iloc[:, 0].size
            # 设置表行数
            self.tableData.setRowCount(rowCount)
            # 设置表列数
            self.tableData.setColumnCount(colCount)
            self.tableData.clear()
            # 重新设置表头
            self.tableData.setHorizontalHeaderLabels(header)
            for r in range(rowCount):
                for c in range(colCount):
                    item = df.iat[r, c]
                    self.tableData.setItem(r, c, QTableWidgetItem(str(item)))
            self.statusbar.showMessage("打开文件成功")

    def showData(self,df):
        df = df.fillna('')
        mylogger.logger.debug("将结果显示在表上")
        header = df.columns.values.tolist()  # [str(col) for col in df]
        # 获取表的行列数
        colCount = df.columns.size
        rowCount = df.iloc[:, 0].size
        # 设置表行数
        self.tableData.setRowCount(rowCount)
        # 设置表列数
        self.tableData.setColumnCount(colCount)
        self.tableData.clear()
        # 重新设置表头
        self.tableData.setHorizontalHeaderLabels(header)
        for r in range(rowCount):
            for c in range(colCount):
                item = df.iat[r, c]
                self.tableData.setItem(r, c, QTableWidgetItem(str(item)))

    def save_to_csv(self):
        self.now_show_table.to_csv("台账结果.csv",encoding="utf-8",index=False)
        mylogger.logger.debug("生成台账结果成功")

    def saveFile(self):
        filename = QFileDialog.getSaveFileName(self, 'save file', '/home/jm/study')
        with open(filename[0], 'w') as f:
            my_text = self.textEdit.toPlainText()
            f.write(my_text)

if __name__ == '__main__':
    app = QApplication(sys.argv)
    mywin = MyMainWindow()
    mywin.show()
    sys.exit(app.exec_())