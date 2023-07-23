package org.frameworkset.elasticsearch.imp;
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

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.elasticsearch.serial.SerialUtil;
import org.frameworkset.spi.assemble.PropertiesUtil;
import org.frameworkset.tran.DataRefactor;
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.output.fileftp.FilenameGenerator;
import org.frameworkset.tran.plugin.es.input.ElasticsearchInputConfig;
import org.frameworkset.tran.plugin.file.output.ExcelFileOutputConfig;
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.util.annotations.DateFormateMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.util.Date;
import java.util.Map;

/**
 * <p>Description: elasticsearch到sftp数据上传案例</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/2/1 14:39
 * @author biaoping.yin
 * @version 1.0
 */
public class ESSlice2ExcelBatchOnceDemo {
    private static Logger logger = LoggerFactory.getLogger(ESSlice2ExcelBatchOnceDemo.class);
	public static void main(String[] args){
		ImportBuilder importBuilder = new ImportBuilder();
        int batchSize = PropertiesUtil.getPropertiesContainer().getIntProperty("batchSize",5);
        int fetchSize = PropertiesUtil.getPropertiesContainer().getIntProperty("fetchSize",5);
		importBuilder.setBatchSize(batchSize).setFetchSize(fetchSize);
        importBuilder.setIncreamentEndOffset(300);//单位秒，同步从上次同步截止时间当前时间前5分钟的数据，下次继续从上次截止时间开始同步数据
        //vops-chbizcollect-2020.11.26,vops-chbizcollect-2020.11.27
        ElasticsearchInputConfig elasticsearchInputConfig = new ElasticsearchInputConfig();
        elasticsearchInputConfig
                .setDslFile("dsl2ndSqlFile.xml")
                .setDslName("scrollSliceQuery")
//                .setDslName("scrollQuery")
                .setScrollLiveTime("10m")
                .setSliceQuery(true)
                .setSliceSize(5)
                .setSourceElasticsearch("default")
                .setQueryUrl("metrics-report/_search");
//				.setQueryUrlFunction((TaskContext taskContext,Date lastStartTime,Date lastEndTime)->{
//					return "kafkademo/_search";
////					return "vops-chbizcollect-2020.11.26,vops-chbizcollect-2020.11.27/_search";
//				})
        importBuilder.setInputConfig(elasticsearchInputConfig)
                .addJobInputParam("fullImport",true)
//				//添加dsl中需要用到的参数及参数值
                .addJobInputParam("var1","v1")
                .addJobInputParam("var2","v2")
                .addJobInputParam("var3","v3");

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
                .addCellMapping(9,"zhs_level","*缴费档次","1")
                .addCellMapping(10,"author","*作者","1")
                 .addCellMapping(11,"logOperuser","操作员")
                .addCellMapping(11,"message","消息")
                .addCellMapping(12,"timestamp","操作时间")
                .addCellMapping(13,"newcollecttime","采集时间")
                .addCellMapping(14,"tag","tag");
        fileOupputConfig.setFileDir("D:\\excelfiles\\hebin");//数据生成目录

        fileOupputConfig.setExistFileReplace(true);//替换重名文件，如果不替换，就需要在genname方法返回带序号的文件名称
        int maxFileRecordSize = PropertiesUtil.getPropertiesContainer().getIntProperty("maxFileRecordSize",-1);
        if(maxFileRecordSize > 0)
            fileOupputConfig.setMaxFileRecordSize(maxFileRecordSize);
        fileOupputConfig.setFilenameGenerator(new FilenameGenerator() {
            @Override
            public String genName(TaskContext taskContext, int fileSeq) {
                Date date = taskContext.getJobStartTime();
                String time = DateFormateMeta.format(date,"yyyyMMddHHmmss");
                return "师大2021年新生医保（2021年）申报名单-合并-"+time+"-"+fileSeq+".xlsx";
            }
        });

        importBuilder.setOutputConfig(fileOupputConfig);



		//设置任务执行拦截器，可以添加多个
		importBuilder.addCallInterceptor(new CallInterceptor() {
			@Override
			public void preCall(TaskContext taskContext) {
				System.out.println("preCall 1");
				taskContext.addTaskData("data","testData");
			}

			@Override
			public void afterCall(TaskContext taskContext) {
				System.out.println("afterCall 1");
			}

			@Override
			public void throwException(TaskContext taskContext, Throwable e) {
				System.out.println("throwException 1");
			}
		});

		importBuilder.addFieldValue("author","张无忌");


        importBuilder.addFieldMapping("@timestamp","timestamp");
		/**
		 * 重新设置es数据结构
		 */
		importBuilder.setDataRefactor(new DataRefactor() {
			public void refactor(Context context) throws Exception  {
				//可以根据条件定义是否丢弃当前记录
				//context.setDrop(true);return;
//				if(s.incrementAndGet() % 2 == 0) {
//					context.setDrop(true);
//					return;
//				}
				String data = (String)context.getTaskContext().getTaskData("data");
//				System.out.println(data);

//				context.addFieldValue("author","duoduo");//将会覆盖全局设置的author变量
				context.addFieldValue("zhs_item","解放");
				context.addFieldValue("zhs_class","小康");

//				context.addIgnoreFieldMapping("title");
				//上述三个属性已经放置到docInfo中，如果无需再放置到索引文档中，可以忽略掉这些属性
//				context.addIgnoreFieldMapping("author");

//				//修改字段名称title为新名称newTitle，并且修改字段的值
//				context.newName2ndData("title","newTitle",(String)context.getValue("title")+" append new Value");
                String message = (String)context.getValue("message");
//                if(message != null && !message.trim().equals(""))
//                    logger.info("message",message);
//                else{
//                    context.setDrop(true);
//                }
                String person_no = (String)context.getValue("person_no");
                if(person_no == null){
                    context.addFieldValue("person_no", "43052519222222222222");
                }
                String name = (String)context.getValue("name");
                if(name == null){
                    context.addFieldValue("name", "超人");
                }
				/**
				 * 获取ip对应的运营商和区域信息
				 */
				Map ipInfo = (Map)context.getValue("ipInfo");
				if(ipInfo != null)
					context.addFieldValue("ipinfo", SimpleStringUtil.object2json(ipInfo));
				else{
					context.addFieldValue("ipinfo", "");
				}
				DateFormat dateFormat = SerialUtil.getDateFormateMeta().toDateFormat();
//				Date optime = context.getDateValue("LOG_OPERTIME",dateFormat);
//				context.addFieldValue("logOpertime",optime);
//				context.addFieldValue("newcollecttime",new Date());

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
		importBuilder.setThreadCount(10);//设置批量导入线程池工作线程数量
		importBuilder.setContinueOnError(true);//任务出现异常，是否继续执行作业：true（默认值）继续执行 false 中断作业执行
//		importBuilder.setDebugResponse(false);//设置是否将每次处理的reponse打印到日志文件中，默认false，不打印响应报文将大大提升性能，只有在调试需要的时候才打开，log日志级别同时要设置为INFO
//		importBuilder.setDiscardBulkResponse(true);//设置是否需要批量处理的响应报文，不需要设置为false，true为需要，默认true，如果不需要响应报文将大大提升处理速度
		importBuilder.setPrintTaskLog(true);

		/**
		 * 执行es数据导入数据库表操作
		 */
		DataStream dataStream = importBuilder.builder();
		dataStream.execute();//执行导入操作
	}
}
