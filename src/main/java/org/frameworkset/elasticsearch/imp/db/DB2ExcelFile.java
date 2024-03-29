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

import org.frameworkset.tran.DataRefactor;
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.output.fileftp.FilenameGenerator;
import org.frameworkset.tran.plugin.db.input.DBInputConfig;
import org.frameworkset.tran.plugin.file.output.ExcelFileOutputConfig;
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Description: 合并城市居民人员编号到师大学生参保社保记录中</p>
 * <p>单表数据增量导出文件-》Ftp</p>
 * <p>Copyright (c) 2020</p>
 *
 * @author biaoping.yin
 * @version 1.0
 * @Date 2021/2/1 14:39
 */
public class DB2ExcelFile {
    private static Logger logger = LoggerFactory.getLogger(DB2ExcelFile.class);

    public static void main(String[] args) {
        //LocalPoolDeployer.addShutdownHook = true;//在应用程序stop.sh时，关闭数据源错误提示
        //启动数据采集
        DB2ExcelFile db2ExcelFile = new DB2ExcelFile();
        db2ExcelFile.scheduleTimestampImportData();
    }


    public static void scheduleTimestampImportData() {

        ImportBuilder importBuilder = new ImportBuilder();
        importBuilder
                .setBatchSize(500)
                .setFetchSize(1000)
        ;


        ExcelFileOutputConfig fileOupputConfig = new ExcelFileOutputConfig();
        fileOupputConfig.setTitle("师大2021年新生医保（2021年）申报名单");
        fileOupputConfig.setSheetName("2021年新生医保申报单");

        fileOupputConfig.addCellMapping(0,"shebao_org","社保经办机构（建议填写）")
                .addCellMapping(1,"person_no","人员编号")
                .addCellMapping(2,"name","*姓名")
                .addCellMapping(3,"cert_type","*证件类型")

                .addCellMapping(4,"cert_no","*证件号码","")
                .addCellMapping(5,"zhs_item","*征收项目")

                .addCellMapping(6,"zhs_class","*征收品目")
                .addCellMapping(7,"zhs_sub_class","征收子目")
                .addCellMapping(8,"zhs_year","*缴费年度","2022")
                .addCellMapping(9,"zhs_level","*缴费档次","1");
        fileOupputConfig.setFileDir("D:\\excelfiles\\hebin");//数据生成目录

        fileOupputConfig.setExistFileReplace(false);//替换重名文件，如果不替换，就需要在genname方法返回带序号的文件名称
        fileOupputConfig.setMaxFileRecordSize(15);
        fileOupputConfig.setFilenameGenerator(new FilenameGenerator() {
            @Override
            public String genName(TaskContext taskContext, int fileSeq) {


                return "师大2021年新生医保（2021年）申报名单-合并-"+fileSeq+".xlsx";
            }
        });

        importBuilder.setOutputConfig(fileOupputConfig);
//		importBuilder.setIncreamentEndOffset(300);//单位秒
        //vops-chbizcollect-2020.11.26,vops-chbizcollect-2020.11.27
        DBInputConfig dbInputConfig= new DBInputConfig();
        dbInputConfig
                .setSqlFilepath("sql.xml")
                .setSqlName("querynewmanrequests");
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
                System.out.println("throwException 1");
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
        importBuilder.setParallel(true);//设置为多线程并行批量导入,false串行
        importBuilder.setThreadCount(3);
        importBuilder.setQueue(10);
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
