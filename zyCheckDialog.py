# coding:utf-8
# 审核账页界面的逻辑功能函数

# import sys,os
# import pandas as pd
# import numpy as np
# import json
from zyCheckUI import Ui_zyCheckDialog
import myLogging as mylogger
from PyQt5.QtWidgets import QDialog
from PyQt5.QtCore import Qt


# Ui_MainWindow
class zyCheckDialog(QDialog,Ui_zyCheckDialog):
    def __init__(self,parent=None):
        super(zyCheckDialog,self).__init__(parent)

        self.setupUi(self)
        mylogger.logger.debug("zyCheckDialog init ok")