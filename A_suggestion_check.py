# 指标行数:100
# ********************************
# *   全国住户生活状况调查季报   *
# *         问卷A审核公式        *
# *        2015年08月23日        *
# ********************************
import pandas as pd
import datetime

Year = datetime.datetime.now().year
Month = datetime.datetime.now().month

A,M=9101,993
psA,psB=0,0
# xb 性别 ，po 配偶
hzAge,xb,hz,po=0,0,0,0

result = open(r'.\审核结果输出\A_suggestion_check_result.txt','w')

def read_file(path):
    return pd.read_csv(path, header=0, encoding='gbk')

def A_suggestion_check(hu):
    global A,psA,psB,hzAge,xb,hz,po
    # while(A < 9120):
    # result.write(table['A101'] + '\n')

    for index, table in hu.iterrows():
        result.write(table['A101'] + '\n')
        person = table['A100']
        if A == 9101 and table['A100'] == 0:
            result.write("请更新家庭成员情况(A1)\n")
        if person > 0:
            psA += 1
            if pd.isnull(table['A200']) == False:
                if table['A100'] != table['A200']:
                    result.write('问卷A有问题请在问卷录入窗口修正!\n')
            if table['A102'] % 4 == 3:
                A += 1
                # continue
            psB += 1
            if table['A103'] == 1: hzAge = table['A106']
            if table['A103'] == 1: hz += 1
            if table['A103'] == 2: po += 1
            if table['A103'] == 1: xb = table['A104']
    
            AAAAAAAA = 1
            tbAge = table['A106'] #填报的年龄
            tbYear = table['A105_1']
            tbMonth = table['A105_2']
            Age = Year-tbYear
            if Month < tbMonth: Age -= 1    #实际年龄
    
        #******A1部分******

            # if table['A103'] == 2 and abs(hzAge-Age) > 20:
            if table['A103'] == 2 and (hzAge - Age > 20 or Age - hzAge > 20):
                result.write("|户主的年龄－配偶的年龄|>20\n")
            if table['A103'] == 3 and hzAge-Age < 8:
                result.write("户主的年龄－子女的年龄<8\n ")
            if table['A103'] == 7 and abs(Age-hzAge) < 8:
                result.write("户主的年龄－媳婿的年龄<8\n")
            if table['A103'] == 9 and abs(Age - hzAge) >= 20:
                result.write("户主的年龄－兄弟姐妹的年龄≥20\n")
            if table['A103'] == 4 and Age - hzAge < 8:
                result.write("父母的年龄－户主的年龄<8\n")
            if table['A103'] == 5 and Age - hzAge < 8:
                result.write("岳父母的年龄－户主的年龄<8\n")
            if table['A103'] == 6 and Age - hzAge < 15:
                result.write("祖父母的年龄－户主的年龄<15\n")
            if table['A103'] == 8 and hzAge - Age < 15:
                result.write("户主的年龄－孙子女的年龄<15\n")
            if table['A110'] == 4:
                if table['A112'] != 1 and table['A112'] != 2:
                    result.write("生活不能自理，是否在校生，请确认\n")
            if table['A112'] == 3:
                if table['A106'] >= 8 and table['A106'] <= 14:
                    if table['A110'] != 1 and table['A110'] != 2:
                        result.write("义务教育年龄且健康，辍学？\n")
    
            # if table['A111'] is None:
            #     result.write("医疗保险漏填！")
            # else:
            #     medicalType = (0, 0, 0, 0, 0, 0, 0, 0)
            #     p = table['A111']
            #     for i in range(0,6):
            #         Q = p[i]
            #         if Q > 0 and Q < 7:
            #             medicalType[Q] += 1
            #     #j = 1 + +7
            #     if table['109'] == 2 and table['A111'] == 1:
            #         result.write(" 参加的医疗保险中（A111=[A111]），非农业户口不应出现选项1")
    
            if table['A112'] == 1 or table['A112'] == 2:
                if table['A106'] > 32:
                    result.write("32周岁以上还是在校生？\n")
                if table['A113'] == 7 and table['A106'] <= 20:
                    result.write("不到20岁就读研究生？\n")
                if table['A113'] == 6 or table['A113'] == 5:
                    if table['A106'] <16:
                        result.write("不到16岁就上大学？\n")
                if table['A113'] == 4 and table['A106'] <= 14:
                    result.write("不到14岁就上高中？\n")
                if table['A113'] == 3 and table['A106'] <= 10:
                    result.write("不到10岁就上初中？\n")
    
    
        #***************A2部分****************
        #劳动力部分全部都是A，跟第一部分A不一样。
            if table['A200'] is None:
                result.write ("劳动力成员的编码未填\n")
            else:
                if Age >= 16 and table['A112'] == 3:
                    if table['A201'] == 1 and Age < 50:
                        result.write("不到50岁就离退休，请核实\n")
        A += 1
        # result.write(str)
        # result.write(hzAge,xb,hz,po)

if __name__ == "__main__":
    path = "D:/研一/审核程序/src/输入文件夹/A310151.18.csv"
    # path = "D:/Document/Code/Python/AuditingApp/src/输入文件夹/A310151.18.csv"
    df = read_file(path)
    print(df.index)
    i = 2
    # result.write(df)
    for row in df.iterrows():
        result.write('第%d行数据：\n'%i)
        i += 1
        A_suggestion_check(row[1])
        # result.write(row[1]["A101"])
        result.write('\n')
    result.close()
