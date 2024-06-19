# 数据同步工具功能说明
通过bboss数据同步工具，可以非常高效快速方便地将Elasticsearch和Database中的数据实时导出到文件并上传到SFTP/FTP服务器
# BBoss Environmental requirements

JDK requirement: JDK 1.8+

Elasticsearch version requirements: 1.x,2.X,5.X,6.X,7.x,8.x,+

Spring booter 1.x,2.x,+
# bboss elasticsearch 数据导入sftp/ftp工具demo
使用本demo所带的应用程序运行容器环境，可以快速编写，打包发布可运行的数据导入工具

支持的数据库：
mysql,maridb，postgress,oracle ,sqlserver,db2等

支持的Elasticsearch版本：
1.x,2.x,5.x,6.x,7.x,8.x,+

支持海量PB级数据同步导入功能
支持sftp/ftp

[使用参考文档](https://esdoc.bbossgroups.com/#/elasticsearch-sftp)


# 构建部署
## 准备工作
需要通过gradle构建发布版本，gradle安装配置参考文档：

https://esdoc.bbossgroups.com/#/bboss-build

## 下载源码工程-基于gradle
<https://github.com/bbossgroups/elasticsearch-file2ftp>

从上面的地址下载源码工程，然后导入idea或者eclipse，根据自己的需求，修改导入程序逻辑

org.frameworkset.elasticsearch.imp.ES2FileFtpDemo

如果需要测试和调试导入功能，运行Dbdemo的main方法即可即可：


```java
public static void main(String[] args){
		ES2FileFtpExportBuilder importBuilder = new ES2FileFtpExportBuilder();
		importBuilder.setBatchSize(500).setFetchSize(1000);
		String ftpIp = CommonLauncher.getProperty("ftpIP","localhost");//同时指定了默认值
		FileOupputConfig fileFtpOupputConfig = new FileOupputConfig();
		FtpOutConfig ftpOutConfig = new FtpOutConfig();
		fileFtpOupputConfig.setFtpOutConfig(ftpOutConfig);

		ftpOutConfig.setBackupSuccessFiles(true);
		ftpOutConfig.setTransferEmptyFiles(true);
		ftpOutConfig.setFtpIP(ftpIp);

		ftpOutConfig.setFtpPort(5322);
		ftpOutConfig.setFtpUser("root");
		ftpOutConfig.setFtpPassword("123456");
		ftpOutConfig.setRemoteFileDir("/home/ecs/failLog");
		ftpOutConfig.setKeepAliveTimeout(100000);
		ftpOutConfig.setFailedFileResendInterval(-1);
		fileFtpOupputConfig.setFileDir("D:\\workdir");
       .....
}
```

修改es和数据库配置-elasticsearch-file2ftp\src\main\resources\application.properties

elasticsearch-file2ftp工程已经内置mysql jdbc驱动，如果有依赖的第三方jdbc包（比如oracle驱动），可以将第三方jdbc依赖包放入elasticsearch-file2ftp\lib目录下

修改完毕配置后，就可以进行功能调试了。


测试调试通过后，就可以构建发布可运行的版本了：进入命令行模式，在源码工程根目录elasticsearch-file2ftp下运行以下gradle指令打包发布版本

release.bat

## 运行作业
gradle构建成功后，在build/distributions目录下会生成可以运行的zip包，解压运行导入程序

linux：

chmod +x restart.sh

./restart.sh

windows: restart.bat

## 作业jvm配置
修改jvm.options，设置内存大小和其他jvm参数

-Xms1g

-Xmx1g



# 作业参数配置

在使用[elasticsearch-file2ftp](https://github.com/bbossgroups/elasticsearch-file2ftp)时，为了避免调试过程中不断打包发布数据同步工具，可以将部分控制参数配置到启动配置文件resources/application.properties中,然后在代码中通过以下方法获取配置的参数：

```ini
#作业运行主程序配置
mainclass=org.frameworkset.elasticsearch.imp.ES2FileFtpBatchSplitFileDemo

# 参数配置
# 在代码中获取方法：CommonLauncher.getBooleanAttribute("dropIndice",false);//同时指定了默认值false
dropIndice=false
```

在代码中获取参数dropIndice方法：

```java
boolean dropIndice = CommonLauncher.getBooleanAttribute("dropIndice",false);//同时指定了默认值false
```

另外可以在resources/application.properties配置控制作业执行的一些参数，例如工作线程数，等待队列数，批处理size等等：

```
queueSize=50
workThreads=10
batchSize=20
```

在作业执行方法中获取并使用上述参数：

```java
int batchSize = CommonLauncher.getIntProperty("batchSize",10);//同时指定了默认值
int queueSize = CommonLauncher.getIntProperty("queueSize",50);//同时指定了默认值
int workThreads = CommonLauncher.getIntProperty("workThreads",10);//同时指定了默认值
importBuilder.setBatchSize(batchSize);
importBuilder.setQueue(queueSize);//设置批量导入线程池等待队列长度
importBuilder.setThreadCount(workThreads);//设置批量导入线程池工作线程数量
```

 

## 技术交流群:166471282 

## 微信公众号:bbossgroup   
![GitHub Logo](https://static.oschina.net/uploads/space/2017/0617/094201_QhWs_94045.jpg)


