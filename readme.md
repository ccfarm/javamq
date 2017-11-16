# 赛题介绍
题目: <实现进程内消息队列>  
开发语言:JAVA  
相关知识点: JAVA编程, 多线程, IO    
测评环境:   
	4核机器 4G内存   
	JVM内存2.6G -Xms2560M -Xmx2560M  
	磁盘速度:40MB/s  
以正确性, 运行效率为排名标准  
# 评测网站:
网址 http://115.29.111.99:9000/    
组名要求: xxx(n)  
n表示你是第几组    
# 代码提交方式
通过git提交, 我们在https://gitee.com/托管代码  
代码框架:https://gitee.com/mocyx/javamq  
你需要:   
1 注册一个账号, fork这个项目, 作为你的git地址     
2 在管理界面-项目成员管理-添加项目成员, 添加一个开发者:mocyx  
3 在管理界面, 将项目设置为非公开的    
然后在评测网站上设置git地址    
然后点"提交评测"来提交  

# 时间表
11.16-12.1   准备阶段,开放评测网站,提供单核机器评测, 主要给大家熟悉环境       
12.1-12.15   预热阶段,在4核机器上评测,成绩不会作为最终成绩      
12.15-12.30  正式比赛,产生最后的排名  
# 赛题背景
见ppt
# 编程目标
你的coding目标是实现以下接口:
Producer的createBytesMessageToTopic(topic, body) 创建一个消息, 指定Topic  
Producer的send(message) 发送消息  
Producer的flush(), 发送结束时会调用一次  
Consumer的attachQueue(queue, topics) 为Queue绑定Topics  
Consumer的poll()  拉消息  
# 评测逻辑:
1 git clone下载代码
2 push阶段: 四个线程同时push消息
3 kill程序,清理页面缓存
4 pull阶段: 四个线程同时pull消息
5 以push和pull的总时间作为排名依据

push和pull都有时间限制
# 代码结构
## pku包下面是你要用到的的类:
核心包括: Producer Consume KeyValue ByteMessage MessageHeader  
我们的评测程序只需要这5个类就能工作  
DefaultKeyValue和DefaultMessage是默认的key-value和message实现, 你完全可以自己自己的版本  
## pku.demo下面是一个内存实现的消息队列
为了方便大家理解题目, 为大家实现了一个内存存储的消息队列  
DemoConsumer: 继承Consumer  
DemoProducer: 继承Producer  
DemoMessageStore: 消息队列的内存存储实现  
DemoTester: 一个测评程序, 里面会开启多个线程进行push与pull, 通过这个类你可以了解到测评程序的运行逻辑    
## 其他
如果有其他问题, 或者找到了评测程序的bug, 请直接联系我  
如果不确定某些hack可不可以做, 可以直接问我  

