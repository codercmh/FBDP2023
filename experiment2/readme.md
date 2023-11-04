# 实验二 

**211275009 陈铭浩**

---

## 1.背景
在借贷交易中，银⾏和其他⾦融机构通常提供资⾦给借款⼈，期望借款⼈能够按时还款本⾦和利
息。然⽽，由于各种原因，有时借款⼈可能⽆法按照合同规定的⽅式履⾏还款义务，从⽽导致贷
款违约。本次实验以银⾏贷款违约为背景，选取了约30万条贷款信息 ，包含在
application_data.csv⽂件中，数据描述包含在columns_description.csv⽂件夹中。
数据来源：https://www.kaggle.com/datasets/mishra5001/credit-card/data

---

## 2.任务一
### 2.1.任务描述
编写MapReduce程序，统计数据集中违约和⾮违约的数量，按照标签TARGET进⾏输出，即1代
表有违约的情况出现，0代表其他情况。
输出格式：
<标签><交易数量>
例：
1 100

### 2.2.设计思路
任务一可看作是wordcount任务的一个简单变体，考虑在mapper中直接取出每一行数据（除第一行标题行外）的target标签并计数，在reducer中对每一种target标签（0/1）计数即可。

![33f87fa20d90eab4ff870e8545e0132](G:\NJU_课程!!!!!!!!\金融大数据处理技术\FBDP2023\experiment2\33f87fa20d90eab4ff870e8545e0132.png)

![7a1b70261eecda3d09a69425f4137dd](G:\NJU_课程!!!!!!!!\金融大数据处理技术\FBDP2023\experiment2\7a1b70261eecda3d09a69425f4137dd-1699101552328-6.png)

![1d432b94ae15da53489fe1e21d3463b](G:\NJU_课程!!!!!!!!\金融大数据处理技术\FBDP2023\experiment2\1d432b94ae15da53489fe1e21d3463b-1699101554539-8.png)

---

### 2.3.运行结果

![744fb73b68c693e56c0832f67f6b810](G:\NJU_课程!!!!!!!!\金融大数据处理技术\FBDP2023\experiment2\744fb73b68c693e56c0832f67f6b810.png)

![b0c6bcb2335aadda791563fd3fc4603](G:\NJU_课程!!!!!!!!\金融大数据处理技术\FBDP2023\experiment2\b0c6bcb2335aadda791563fd3fc4603.png)
