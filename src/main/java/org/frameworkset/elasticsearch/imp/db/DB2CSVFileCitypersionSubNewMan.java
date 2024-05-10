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

import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.DataRefactor;
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.output.fileftp.FilenameGenerator;
import org.frameworkset.tran.plugin.db.input.DBInputConfig;
import org.frameworkset.tran.plugin.file.output.FileOutputConfig;
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.util.RecordGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Writer;
import java.util.Map;

/**
 * <p>Description: 提取不在【师大2021年新生医保（2021年）申报名单】中的【（1月8日导出）城乡居民申报信息列表】数据</p>
 * <p>单表数据增量导出文件-》Ftp</p>
 * <p>Copyright (c) 2020</p>
 *
 * @author biaoping.yin
 * @version 1.0
 * @Date 2021/2/1 14:39
 */
public class DB2CSVFileCitypersionSubNewMan {
    private static Logger logger = LoggerFactory.getLogger(DB2CSVFileCitypersionSubNewMan.class);

    public static void main(String[] args) {
        //LocalPoolDeployer.addShutdownHook = true;//在应用程序stop.sh时，关闭数据源错误提示
        //启动数据采集
        DB2CSVFileCitypersionSubNewMan db2file = new DB2CSVFileCitypersionSubNewMan();
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


        FileOutputConfig fileFtpOupputConfig = new FileOutputConfig();

        fileFtpOupputConfig.setFileDir("D:\\excelfiles\\subs");//数据生成目录

        fileFtpOupputConfig.setFilenameGenerator(new FilenameGenerator() {
            @Override
            public String genName(TaskContext taskContext, int fileSeq) {


                return "不在【师大2021年新生医保（2021年）申报名单】中的【（1月8日导出）城乡居民申报信息列表】.csv";
            }
        });
        fileFtpOupputConfig.setRecordGenerator(new RecordGenerator() {
            @Override
            public void buildRecord(TaskContext context, CommonRecord record, Writer builder)throws Exception {
                Map<String,Object> datas = record.getDatas();
                StringBuilder strBuilder = new StringBuilder();
                strBuilder.append(datas.get("shebao_org"))   ;
                strBuilder.append(",")   ;
                String person_no = (String)datas.get("person_no");
                if(person_no == null )
                    strBuilder.append("");
                else {
                    strBuilder.append("^").append(person_no);
                }
                strBuilder.append(",")   ;
                strBuilder.append(datas.get("name"))   ;
                strBuilder.append(",")   ;
                strBuilder.append(datas.get("cert_type"))   ;
                strBuilder.append(",^")   ;
                strBuilder.append(datas.get("cert_no"))   ;
                strBuilder.append(",")   ;
                strBuilder.append(datas.get("zhs_item"))   ;

                strBuilder.append(",")   ;
                strBuilder.append(datas.get("zhs_class"))   ;
                strBuilder.append(",")   ;
                strBuilder.append(datas.get("zhs_sub_class"))   ;
                strBuilder.append(",")   ;
                strBuilder.append(datas.get("zhs_year"))   ;
                strBuilder.append(",")   ;

                strBuilder.append(datas.get("zhs_level"))   ;
                builder.write(strBuilder.toString());
            }
        });
        importBuilder.setOutputConfig(fileFtpOupputConfig);
//		importBuilder.setIncreamentEndOffset(300);//单位秒
        //vops-chbizcollect-2020.11.26,vops-chbizcollect-2020.11.27

        DBInputConfig dbInputConfig= new DBInputConfig();
        dbInputConfig
                .setSqlFilepath("sql.xml")
                .setSqlName("citypersonSubNewmanrequests");
        importBuilder.setInputConfig(dbInputConfig);
//        //定时任务配置，
//        importBuilder.setFixedRate(false)//参考jdk timer task文档对fixedRate的说明
////					 .setScheduleDate(date) //指定任务开始执行时间：日期
//                .setDeyLay(1000L) // 任务延迟执行deylay毫秒后执行
//                .setPeriod(30000L); //每隔period毫秒执行，如果不设置，只执行一次
//        //定时任务配置结束

        //设置任务执行拦截器，可以添加多个
        importBuilder.addCallInterceptor(new CallInterceptor() {
            @Override
            public void preCall(TaskContext taskContext) {


            }

            @Override
            public void afterCall(TaskContext taskContext) {

            }

            @Override
            public void throwException(TaskContext taskContext, Throwable e) {
                logger.error("",e);
            }
        });

        /**
         * 重新设置es数据结构
         */
        importBuilder.setDataRefactor(new DataRefactor() {
            public void refactor(Context context) throws Exception {

            }
        });
        //映射和转换配置结束

        /**
         * 内置线程池配置，实现多线程并行数据导入功能，作业完成退出时自动关闭该线程池
         */
        importBuilder.setParallel(false);//设置为多线程并行批量导入,false串行
        importBuilder.setContinueOnError(true);//任务出现异常，是否继续执行作业：true（默认值）继续执行 false 中断作业执行
        importBuilder.setPrintTaskLog(true);

        /**
         * 执行db数据导入csv操作
         */
        DataStream dataStream = importBuilder.builder();
        dataStream.execute();//执行导入操作
        logger.info("job started.");
    }
}
