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

---

### 2.2.设计思路
任务一可看作是wordcount任务的一个简单变体，考虑在mapper中直接取出每一行数据（除第一行标题行外）的target标签并计数，在reducer中对每一种target标签（0/1）计数即可。

![33f87fa20d90eab4ff870e8545e0132](G:\NJU_课程!!!!!!!!\金融大数据处理技术\FBDP2023\experiment2\33f87fa20d90eab4ff870e8545e0132.png)

![7a1b70261eecda3d09a69425f4137dd](G:\NJU_课程!!!!!!!!\金融大数据处理技术\FBDP2023\experiment2\7a1b70261eecda3d09a69425f4137dd-1699101552328-6.png)

![1d432b94ae15da53489fe1e21d3463b](G:\NJU_课程!!!!!!!!\金融大数据处理技术\FBDP2023\experiment2\1d432b94ae15da53489fe1e21d3463b-1699101554539-8.png)

---

### 2.3.运行结果

![744fb73b68c693e56c0832f67f6b810](G:\NJU_课程!!!!!!!!\金融大数据处理技术\FBDP2023\experiment2\744fb73b68c693e56c0832f67f6b810.png)

![b0c6bcb2335aadda791563fd3fc4603](G:\NJU_课程!!!!!!!!\金融大数据处理技术\FBDP2023\experiment2\b0c6bcb2335aadda791563fd3fc4603.png)


---

## 3.任务二
### 3.1.任务描述
编写MapReduce程序，统计⼀周当中每天申请贷款的交易数
WEEKDAY_APPR_PROCESS_START，并按照交易数从⼤到⼩进⾏排序。
输出格式：
<weekday><交易数量>
例：
Sunday 16000

---

### 3.2.设计思路
首先在mapper中直接取出每一行数据（除第一行标题行外）的WEEKDAY_APPR_PROCESS_START标签并计数，在reducer中对每一种Weekday累计计数。为了按照交易数对结果从大到小排序，我使用了treemap，并利用Collections.reverseOrder()将元素逆序排列，最后在cleanup中输出排序好的结果
> TreeMap是一种基于红黑树实现的有序映射数据结构，它根据键的自然顺序或自定义排序顺序来维护键-值对的有序性。默认情况下，TreeMap会按照键的自然顺序（升序）来排序。如果想要逆序（降序）排序，可以使用Collections.reverseOrder()来创建一个比较器，它会将元素逆序排列。

> 在Hadoop MapReduce中，cleanup函数是一个用于Mapper和Reducer任务的生命周期方法之一。cleanup函数在Map或Reduce任务执行结束后被调用，用于执行一些清理工作。具体而言：
> 对于Mapper任务：cleanup函数会在Mapper任务执行完毕后调用。你可以在cleanup函数中进行一些资源释放、缓存刷新等清理工作；
> 对于Reducer任务：cleanup函数会在Reducer任务执行完毕后调用。它可以用于执行一些清理操作，例如将数据写入数据库、关闭文件句柄等。
> cleanup函数通常用于处理与MapReduce任务相关的资源管理，以确保任务执行后资源被正确释放，或者用于执行一些最终的计算和输出操作。

![4c6ec209a038e699f8c8c4cffdcc96a](G:\NJU_课程!!!!!!!!\金融大数据处理技术\FBDP2023\experiment2\4c6ec209a038e699f8c8c4cffdcc96a.png)

![0a598642d3c0b4cac67cb0dcfcb7ab5](G:\NJU_课程!!!!!!!!\金融大数据处理技术\FBDP2023\experiment2\0a598642d3c0b4cac67cb0dcfcb7ab5.png)

---

### 3.3.运行结果

![ddf6739b208f01342f52d2a77c2adc6](G:\NJU_课程!!!!!!!!\金融大数据处理技术\FBDP2023\experiment2\ddf6739b208f01342f52d2a77c2adc6.png)

![cb272c9fe9e93ffd751a082654a347c](G:\NJU_课程!!!!!!!!\金融大数据处理技术\FBDP2023\experiment2\cb272c9fe9e93ffd751a082654a347c.png)
