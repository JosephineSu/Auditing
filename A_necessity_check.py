# 指标行数:100
# ********************************
# *   全国住户生活状况调查季报   *
# *         问卷A审核公式        *
# *        2015年08月23日        *
# ********************************
import pandas as pd
# import numpy as np
import datetime
from deal_hu import spliteFamily
import myLogging as mylogger


Year = datetime.datetime.now().year
Month = datetime.datetime.now().month


def read_file(path):
    with open(path) as f:
        return pd.read_csv(f, header=0)

def A_necessity_check(TableA):
    mylogger.logger.debug("A_necessity_check init..")
    result = open(r'.\审核结果输出\A_necessity_check_result.txt', 'w')
    hu_total = spliteFamily(TableA)
    for hu in hu_total:
        A,M=9101,993
        psA,psB=0,0
        hzAge,xb,hz,po=0,0,0,0
        # print(hu)
        for index, A_table in hu.iterrows():
            result.write(A_table['A101'] + '\n')
            print(A_table['SID'])
            # print(index)
            # print(A_table)
            # while(A < 9120):
            person = A_table['A100']
            # print("请更新家庭成员情况") if A == 9101 and A_table['A100'] == 0 else 'kong'
            if A_table['A100'] == 0:
                result.write("请更新家庭成员情况(A1)\n")
            else:
                result.write('')

            if person > 0:
                psA += 1
                if pd.isnull(A_table['A200']) == False:
                    if A_table['A100'] != A_table['A200']:
                        # result.write(A_table['A200'])
                        result.write('问卷A有问题请在问卷录入窗口修正!\n')
                if A_table['A102'] % 4 == 3:
                    A += 1
                    # continue
                psB += 1
                if A_table['A103'] == 1: hzAge = A_table['YEAR'] - A_table['A105_1']
                if A_table['A103'] == 1: hz += 1
                if A_table['A103'] == 2: po += 1
                if A_table['A103'] == 1: xb = A_table['A104']
                if A_table['A103'] == 2:
                    if A_table['A104'] == xb: result.write("户主与配偶性别相同\n")

                AAAAAAAA = 1
                if A_table['A105'] is None: result.write('请补填年龄，不足1岁请填写0\n')
                tbAge = Year - A_table['A105_1'] #填报的年龄
                tbYear = A_table['A105_1']
                tbMonth = A_table['A105_2']
                Age = Year-tbYear
                # 实际年龄
                if Month < tbMonth: Age -= 1

                # A1部分
                # 应该填报内容
                # A102开户时填4，其他时间非4，但每个报告期都可能开户
                # A102 == c4 ?: "人员增减情况越界，请填写(4)"
                # 开户时，应直接填报"4"
                if A_table['A102'] < 1 or A_table['A102'] > 4: "人员增减情况越界，请填写(1-4)\n"
                if A_table['A103'] < 1 or A_table['A103'] > 10: "与本户户主的关系越界，请填写(1-10)\n"
                if A_table['A104'] != 1 or A_table['A104'] != 2:"性别越界，请填写(1-2)\n"
                if tbYear == Year:
                    if tbMonth > Month:
                        result.write("出生月份越界\n")
                if A_table['A105'] < 191001 or A_table['A105'] > Year * 100 + Month:result.write("出生年月越界\n")
                if A_table['A107'] < 1 or A_table['A107'] > 9:result.write("民族越界，请填写(1-9)\n")
                if A_table['A108'] == 1 or A_table['A108'] == 2 or A_table['A108'] == 3 or A_table['A108'] ==  4 or A_table['A108'] ==  5 or A_table['A108'] == 7 or A_table['A108'] == 11 or A_table['A108'] == 12 or A_table['A108'] == 13 or A_table['A108'] == 14 or A_table['A108'] == 15 or A_table['A108'] == 21 or A_table['A108'] == 22 or A_table['A108'] == 23 or A_table['A108'] == 31 or A_table['A108'] == 32 or A_table['A108'] == 33 or A_table['A108'] == 34 or A_table['A108'] == 35 or A_table['A108'] == 36 or A_table['A108'] == 37 or A_table['A108'] == 41 or A_table['A108'] == 42 or A_table['A108'] == 43 or A_table['A108'] == 44 or A_table['A108'] == 45 or A_table['A108'] == 46 or A_table['A108'] == 50 or A_table['A108'] == 51 or A_table['A108'] == 52 or A_table['A108'] == 53 or A_table['A108'] == 54 or A_table['A108'] == 61 or A_table['A108'] == 62 or A_table['A108'] == 63 or A_table['A108'] == 64 or A_table['A108'] == 65 or A_table['A108'] == 71 or A_table['A108'] == A_table['A108'] or A_table['A108'] == 82 :
                    pass
                else:
                    result.write("户口登记地越界，请重新填写\n")
                if A_table['A109'] < 1 or A_table['A109'] > 3: result.write("户口性质越界，请填写(1-3)\n")
                if A_table['A110'] < 1 or A_table['A110'] > 4: result.write("健康状况越界，请填写(1-4)\n")
                # if A_table['M205'] == 4:
                #     if A_table['A110'] == 4:
                #         result.write("住家保姆、帮工,生活不能自理？")
                #     if A_table['A112'] != 3:
                #         result.write("住家保姆、帮工,不能是在校学生？")

                if A_table['A111'] is None: result.write("A111漏填，若没有参加医疗保险请填7\n")
                if Age >= 6:
                    if A_table['A112'] < 1 or A_table['A112'] > 3:
                        result.write("是否在校生越界，请填写(1-3)\n")
                    if A_table['A113'] < 1 or A_table['A113'] > 7:
                        result.write("教育程度越界，请填写(1-7)\n")
                if Age >= 15:
                    if A_table['A114'] < 1 or A_table['A114'] > 4:
                        result.write("婚姻状况越界，请填写(1-4)\n")
                if A_table['A115'] is None: result.write("A115漏填，若本季度未在家居住请填0\n")
                if A_table['A115'] > 3: result.write("本季度居住时间越界\n")
                if A_table['A116'] != 1 and A_table['A116'] != 2: result.write("是否其它住宅居住越界，请填写(1-2)\n")
                if A_table['A117'] != 1 and A_table['A117'] != 2: result.write("是否在本住宅居住一天以上越界，请填写(1-2)\n")
                if A_table['A118'] != 1 and A_table['A118'] != 2: result.write("是否打算居住一个半月以上越界，请填写(1-2)\n")
                if A_table['A119'] != 1 and A_table['A119'] != 2: result.write("是否常住人口越界，请填写(1-2)\n")
                if A_table['A120'] != 1 and A_table['A120'] != 2: result.write("是否是否持证残疾人越界，请填写(1-2))\n")

                # 应该跳转内容
                if A_table['A112'] + A_table['A113'] > 0:
                    if Age < 6:
                        result.write("小于6岁，不用填报A112|A113\n")
                if A_table['A114'] > 0:
                    if Age < 15:
                        result.write("小于15岁，不用填报A114\n")
                if Age < 15 or A_table['A112'] != 3:
                    # age < c15 | | A112 != c3? A201 + +A208 > c0 ?"小于15岁或在校生，不用填报A2问卷":: // 可能按虚岁填报
                    if A_table['A201'] + A_table['A208'] > 0:
                        result.write("小于15岁或在校生，不用填报A2问卷\n")
    # result.write("ok")


# result.write(str)
    # result.write(hzAge,xb,hz,po)

if __name__ == "__main__":
    path = "D:/Document/Code/Python/AuditingApp/src/输入文件夹/A310151.18.csv"
    # path = "D:/Document/Code/Python/AuditingApp/src/输入文件夹/A310151.18.csv"
    df = read_file(path)
    print(df.index)
    # print(df)
    for row in df.iterrows():
        A_necessity_check(row[1])
        # print(row[1]["A101"])
