package org.frameworkset.elasticsearch.imp.db;
/**
 * Copyright 2020 bboss
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.frameworkset.elasticsearch.serial.SerialUtil;
import org.frameworkset.runtime.CommonLauncher;
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.DataRefactor;
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.ftp.FtpConfig;
import org.frameworkset.tran.output.fileftp.FilenameGenerator;
import org.frameworkset.tran.output.ftp.FtpOutConfig;
import org.frameworkset.tran.plugin.db.input.DBInputConfig;
import org.frameworkset.tran.plugin.file.output.FileOutputConfig;
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.util.RecordGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * <p>Description: database到sftp数据上传案例</p>
 * <p>单表数据增量导出文件-》Ftp</p>
 * <p>Copyright (c) 2020</p>
 *
 * @author biaoping.yin
 * @version 1.0
 * @Date 2021/2/1 14:39
 */
public class DB2FileFtp {
    private static Logger logger = LoggerFactory.getLogger(DB2FileFtp.class);

    public static void main(String[] args) {
        //LocalPoolDeployer.addShutdownHook = true;//在应用程序stop.sh时，关闭数据源错误提示
        //启动数据采集
        DB2FileFtp db2file = new DB2FileFtp();
        db2file.scheduleTimestampImportData();
    }


    public static void scheduleTimestampImportData() {

        ImportBuilder importBuilder = new ImportBuilder();
        importBuilder
                .setBatchSize(500)
                .setFetchSize(1000)
                .setUseJavaName(false) //可选项,将数据库字段名称转换为java驼峰规范的名称，true转换，false不转换，默认false，例如:doc_id -> docId
                .setUseLowcase(false)  //可选项，true 列名称转小写，false列名称不转换小写，默认false，只要在UseJavaName为false的情况下，配置才起作用
        ;
        //源数据库设置
        //数据库相关配置参数(application.properties)
        String dbName = CommonLauncher.getProperty("db.name", "oradb01");
        String dbUser = CommonLauncher.getProperty("db.user", "zlsptb");
        String dbPassword = CommonLauncher.getProperty("db.password", "*********");
        String dbDriver = CommonLauncher.getProperty("db.driver", "oracle.jdbc.driver.OracleDriver");
        String dbUrl = CommonLauncher.getProperty("db.url", "jdbc:oracle:thin:@192.168.97.100:1521:ORCLPDB1");
        String showsql = CommonLauncher.getProperty("db.showsql", "false");
        String validateSQL = CommonLauncher.getProperty(" db.validateSQL", "select 1 from dual");
        //String dbInfoEncryptClass = CommonLauncher.getProperty("db.dbInfoEncryptClass");
        boolean dbUsePool = CommonLauncher.getBooleanAttribute("db.usePool", true);
        Integer dbInitSize = CommonLauncher.getIntProperty("db.initSize", 100);
        Integer dbMinIdleSize = CommonLauncher.getIntProperty("db.minIdleSize", 100);
        Integer dbMaxSize = CommonLauncher.getIntProperty("db.maxSize", 1000);
        Integer dbJdbcFetchSize = CommonLauncher.getIntProperty("db.jdbcFetchSize", 10000);
        boolean columnLableUpperCase = CommonLauncher.getBooleanAttribute("db.columnLableUpperCase", true);
        DBInputConfig dbInputConfig= new DBInputConfig();
        dbInputConfig

                .setSqlFilepath("sql.xml")
                .setSqlName("demoexportFull")

        //数据源相关配置，可选项，可以在外部启动数据源
                .setDbName(dbName)
                .setDbDriver(dbDriver) //数据库驱动程序，必须导入相关数据库的驱动jar包
                .setDbUrl(dbUrl)//通过useCursorFetch=true启用mysql的游标fetch机制，否则会有严重的性能隐患，useCursorFetch必须和jdbcFetchSize参数配合使用，否则不会生效
                .setDbUser(dbUser)
                .setDbPassword(dbPassword)
                .setValidateSQL(validateSQL)
                .setUsePool(dbUsePool)
                .setDbInitSize(dbInitSize)
                .setDbMinIdleSize(dbMinIdleSize)
                .setDbMaxSize(dbMaxSize)
                .setJdbcFetchSize(dbJdbcFetchSize)
                .setColumnLableUpperCase(columnLableUpperCase)
        ;//是否使用连接池
        importBuilder.setInputConfig(dbInputConfig);

        String ftpIp = CommonLauncher.getProperty("ftpIP", "192.168.97.100");//同时指定了默认值
        FileOutputConfig fileFtpOupputConfig = new FileOutputConfig();

        fileFtpOupputConfig.setFileDir("D:\\output_data");//数据生成目录
        FtpOutConfig ftpOutConfig = new FtpOutConfig();
        //fileFtpOupputConfig.setUseJavaName(false);
        //fileFtpOupputConfig.setUseLowcase(true);
        ftpOutConfig.setBackupSuccessFiles(true);
        ftpOutConfig.setTransferEmptyFiles(true);
        ftpOutConfig.setFtpIP(ftpIp);
        ftpOutConfig.setTransferProtocol(FtpConfig.TRANSFER_PROTOCOL_FTP); //采用ftp协议

        ftpOutConfig.setFtpPort(21);
//        fileFtpOupputConfig.addHostKeyVerifier("2a:da:5a:6a:cf:7d:65:e5:ac:ff:d3:73:7f:2c:55:c9");
        ftpOutConfig.setFtpUser("zhxq02");
        ftpOutConfig.setFtpPassword("**********");
        ftpOutConfig.setRemoteFileDir("/");
        ftpOutConfig.setKeepAliveTimeout(100000);
        ftpOutConfig.setFailedFileResendInterval(100000);
        fileFtpOupputConfig.setFtpOutConfig(ftpOutConfig);

        fileFtpOupputConfig.setFilenameGenerator(new FilenameGenerator() {
            @Override
            public String genName(TaskContext taskContext, int fileSeq) {
                String time = (String) taskContext.getTaskData("time");
                String _fileSeq = fileSeq + "";
                int t = 6 - _fileSeq.length();
                if (t > 0) {
                    String tmp = "";
                    for (int i = 0; i < t; i++) {
                        tmp += "0";
                    }
                    _fileSeq = tmp + _fileSeq;
                }

                return "ZLS_WATER_ACCOUNT_" + _fileSeq + "_" + time + ".json";
            }
        });
        fileFtpOupputConfig.setRecordGenerator(new RecordGenerator() {
            @Override
            public void buildRecord(Context taskContext, CommonRecord record, Writer builder) {
                SerialUtil.normalObject2json(record.getDatas(), builder);
                //String data = (String)taskContext.getTaskContext().getTaskData("data");
//				System.out.println(data);

            }
        });
        importBuilder.setOutputConfig(fileFtpOupputConfig);
//		importBuilder.setIncreamentEndOffset(300);//单位秒
        //vops-chbizcollect-2020.11.26,vops-chbizcollect-2020.11.27



        //定时任务配置，
        importBuilder.setFixedRate(false)//参考jdk timer task文档对fixedRate的说明
//					 .setScheduleDate(date) //指定任务开始执行时间：日期
                .setDeyLay(1000L) // 任务延迟执行deylay毫秒后执行
                .setPeriod(30000L); //每隔period毫秒执行，如果不设置，只执行一次
        //定时任务配置结束

        //设置任务执行拦截器，可以添加多个
        importBuilder.addCallInterceptor(new CallInterceptor() {
            @Override
            public void preCall(TaskContext taskContext) {

                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
                long start = System.currentTimeMillis();//预处理开始时间戳

                String formate = "yyyyMMddHHmmss";
                //HN_BOSS_TRADE00001_YYYYMMDDHHMM_000001.txt
                SimpleDateFormat dateFormat = new SimpleDateFormat(formate);
                String time = dateFormat.format(new Date());
                taskContext.addTaskData("time", time);

                Long end = System.currentTimeMillis();//结束时间戳
                logger.info("预处理<DB>结束耗时：" + (end - start) + " millis");
            }

            @Override
            public void afterCall(TaskContext taskContext) {
                System.out.println("afterCall 1");
                logger.info("数据采集结束!");
            }

            @Override
            public void throwException(TaskContext taskContext, Throwable e) {
                System.out.println("throwException 1");
            }
        });
//		//设置任务执行拦截器结束，可以添加多个
        //增量配置开始
//		importBuilder.setLastValueColumn("collecttime");//手动指定日期增量查询字段变量名称
//		importBuilder.setFromFirst(false);//setFromfirst(false)，如果作业停了，作业重启后从上次截止位置开始采集数据，
//		//setFromfirst(true) 如果作业停了，作业重启后，重新开始采集数据
//		importBuilder.setLastValueStorePath("db2fileftp_import");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
////		importBuilder.setLastValueStoreTableName("logs");//记录上次采集的增量字段值的表，可以不指定，采用默认表名increament_tab
//		importBuilder.setLastValueType(ImportIncreamentConfig.TIMESTAMP_TYPE);//如果没有指定增量查询字段名称，则需要指定字段类型：ImportIncreamentConfig.NUMBER_TYPE 数字类型
//		// 或者ImportIncreamentConfig.TIMESTAMP_TYPE 日期类型
////		importBuilder.setLastValue(new Date());
        //增量配置结束

        //映射和转换配置开始
//		/**
//		 * db-es mapping 表字段名称到es 文档字段的映射：比如document_id -> docId
//		 * 可以配置mapping，也可以不配置，默认基于java 驼峰规则进行db field-es field的映射和转换
//		 */
//		importBuilder.addFieldMapping("document_id","docId")
//				.addFieldMapping("docwtime","docwTime")
//				.addIgnoreFieldMapping("channel_id");//添加忽略字段
//
//
//		/**
//		 * 为每条记录添加额外的字段和值
//		 * 可以为基本数据类型，也可以是复杂的对象
//		 */
//		importBuilder.addFieldValue("testF1","f1value");
//		importBuilder.addFieldValue("testInt",0);
//		importBuilder.addFieldValue("testDate",new Date());
//		importBuilder.addFieldValue("testFormateDate","yyyy-MM-dd HH",new Date());
//		TestObject testObject = new TestObject();
//		testObject.setId("testid");
//		testObject.setName("jackson");
//		importBuilder.addFieldValue("testObject",testObject);
//		importBuilder.addFieldValue("author","张无忌");
//		importBuilder.addFieldMapping("operModule","OPER_MODULE");
//		importBuilder.addFieldMapping("logContent","LOG_CONTENT");
//		importBuilder.addFieldMapping("logOperuser","LOG_OPERUSER");


        /**
         * 重新设置es数据结构
         */
        importBuilder.setDataRefactor(new DataRefactor() {
            public void refactor(Context context) throws Exception {
                //可以根据条件定义是否丢弃当前记录
                //context.setDrop(true);return;
//				if(s.incrementAndGet() % 2 == 0) {
//					context.setDrop(true);
//					return;
//				}

                //String data = (String) context.getTaskContext().getTaskData("data");
                //String data =  context.getStringValue("ID_NO");
                //System.out.println(data);

//				context.addFieldValue("author","duoduo");//将会覆盖全局设置的author变量
//				context.addFieldValue("title","解放");
//				context.addFieldValue("subtitle","小康");

//				context.addIgnoreFieldMapping("title");
                //上述三个属性已经放置到docInfo中，如果无需再放置到索引文档中，可以忽略掉这些属性
//				context.addIgnoreFieldMapping("author");


                context.addFieldValue("collecttime", new Date());

                /**
                 //关联查询数据,单值查询
                 Map headdata = SQLExecutor.queryObjectWithDBName(Map.class,context.getEsjdbc().getDbConfig().getDbName(),
                 "select * from head where billid = ? and othercondition= ?",
                 context.getIntegerValue("billid"),"otherconditionvalue");//多个条件用逗号分隔追加
                 //将headdata中的数据,调用addFieldValue方法将数据加入当前es文档，具体如何构建文档数据结构根据需求定
                 context.addFieldValue("headdata",headdata);
                 //关联查询数据,多值查询
                 List<Map> facedatas = SQLExecutor.queryListWithDBName(Map.class,context.getEsjdbc().getDbConfig().getDbName(),
                 "select * from facedata where billid = ?",
                 context.getIntegerValue("billid"));
                 //将facedatas中的数据,调用addFieldValue方法将数据加入当前es文档，具体如何构建文档数据结构根据需求定
                 context.addFieldValue("facedatas",facedatas);
                 */
            }
        });
        //映射和转换配置结束

        /**
         * 内置线程池配置，实现多线程并行数据导入功能，作业完成退出时自动关闭该线程池
         */
        importBuilder.setParallel(true);//设置为多线程并行批量导入,false串行
        importBuilder.setQueue(10);//设置批量导入线程池等待队列长度
        importBuilder.setThreadCount(50);//设置批量导入线程池工作线程数量
        importBuilder.setContinueOnError(true);//任务出现异常，是否继续执行作业：true（默认值）继续执行 false 中断作业执行
        importBuilder.setPrintTaskLog(false);

        /**
         * 执行es数据导入数据库表操作
         */
        DataStream dataStream = importBuilder.builder();
        dataStream.execute();//执行导入操作
        logger.info("job started.");
    }
}
