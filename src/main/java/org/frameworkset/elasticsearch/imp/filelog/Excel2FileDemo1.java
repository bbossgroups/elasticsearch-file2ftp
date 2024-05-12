package org.frameworkset.elasticsearch.imp.filelog;
/**
 * Copyright 2008 biaoping.yin
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
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.input.excel.ExcelFileConfig;
import org.frameworkset.tran.input.file.FileConfig;
import org.frameworkset.tran.input.file.FileFilter;
import org.frameworkset.tran.input.file.FilterFileInfo;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.output.fileftp.FilenameGenerator;
import org.frameworkset.tran.plugin.file.input.ExcelFileInputConfig;
import org.frameworkset.tran.plugin.file.output.FileOutputConfig;
import org.frameworkset.tran.record.CellMapping;
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.tran.util.RecordGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;

/**
 * <p>Description: 采集日志文件数据并发送kafka作业，如需调试同步功能，直接运行main方法</p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/9/27 20:38
 * @author biaoping.yin
 * @version 1.0
 */
public class Excel2FileDemo1 {
	private static Logger logger = LoggerFactory.getLogger(Excel2FileDemo1.class);
	public static void main(String args[]){

		Excel2FileDemo1 dbdemo = new Excel2FileDemo1();

		dbdemo.scheduleTimestampImportData();
	}



	/**
	 * elasticsearch地址和数据库地址都从外部配置文件application.properties中获取，加载数据源配置和es配置
	 */
	public void scheduleTimestampImportData(){
		ImportBuilder importBuilder = new ImportBuilder();
		importBuilder.setBatchSize(500).setFetchSize(1000);

		ExcelFileInputConfig config = new ExcelFileInputConfig();
		//shebao_org,person_no, name, cert_type,cert_no,zhs_item  ,zhs_class ,zhs_sub_class,zhs_year  , zhs_level
		//配置excel文件列与导出字段名称映射关系
		ExcelFileConfig excelFileConfig = new ExcelFileConfig();

		excelFileConfig.setSourcePath("D:\\workspace\\bbossesdemo\\filelog-elasticsearch\\excelfiles")//指定目录
				.setFileFilter(new FileFilter() {
					@Override
					public boolean accept(FilterFileInfo fileInfo, FileConfig fileConfig) {
						//判断是否采集文件数据，返回true标识采集，false 不采集
						return fileInfo.getFileName().equals("works0329.xlsx");
					}
				});//指定文件过滤器
//				.setSkipHeaderLines(1)//忽略第一行

		excelFileConfig.addCellMappingWithType(0,"works", CellMapping.CELL_NUMBER_INTEGER);
		config.addConfig(excelFileConfig);


		config.setEnableMeta(true);
		FileOutputConfig fileFtpOupputConfig = new FileOutputConfig();

		fileFtpOupputConfig.setFileDir("D:\\workspace\\bbossesdemo\\filelog-elasticsearch\\excelfiles");
		fileFtpOupputConfig.setFilenameGenerator(new FilenameGenerator() {
			@Override
			public String genName(TaskContext taskContext, int fileSeq) {


				return "works-0329.txt";
			}
		});
		fileFtpOupputConfig.setRecordGenerator(new RecordGenerator() {
			@Override
			public void buildRecord(TaskContext taskContext, CommonRecord record, Writer builder) throws IOException {
				Integer tmp = (Integer) record.getData("works");
				int v = tmp;
//				if(v == 4 || v == 3 || v == 2){
//				if(v > 2){
//					v = v -1;
//				}
                v =v +1;
				String works = String.valueOf(v);
				builder.write(works);

			}
		});
		importBuilder.setOutputConfig(fileFtpOupputConfig);
		//定时任务配置结束



		importBuilder.setInputConfig(config);
		importBuilder.setFlushInterval(10000l);
		importBuilder.setFromFirst(true);//setFromfirst(false)，如果作业停了，作业重启后从上次截止位置开始采集数据，
		//setFromfirst(true) 如果作业停了，作业重启后，重新开始采集数据
		importBuilder.setLastValueStorePath("excelworks0329");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
//		importBuilder.setLastValueStoreTableName("logs");//记录上次采集的增量字段值的表，可以不指定，采用默认表名increament_tab
		// 或者ImportIncreamentConfig.TIMESTAMP_TYPE 日期类型
		//指定增量同步的起始时间
//		importBuilder.setLastValue(new Date());
		//增量配置结束

		//映射和转换配置开始

		/**
		 * 重新设置es数据结构
		 */
		importBuilder.setDataRefactor(new DataRefactor() {
			public void refactor(Context context) throws Exception  {

			}
		});
		//映射和转换配置结束
		importBuilder.setExportResultHandler(new ExportResultHandler<String>() {
			@Override
			public void success(TaskCommand<String> taskCommand, String result) {
				TaskMetrics taskMetric = taskCommand.getTaskMetrics();
				logger.info(taskCommand.getTaskMetrics().toString());
			}

			@Override
			public void error(TaskCommand<String> taskCommand, String result) {
				logger.warn(taskCommand.getTaskMetrics().toString());
			}

			@Override
			public void exception(TaskCommand<String> taskCommand, Throwable exception) {
				logger.warn(taskCommand.getTaskMetrics().toString(),exception);
			}


		});

		importBuilder.addCallInterceptor(new CallInterceptor() {
			@Override
			public void preCall(TaskContext taskContext) {

			}

			@Override
			public void afterCall(TaskContext taskContext) {
				if(taskContext != null)
					logger.info(taskContext.getJobTaskMetrics().toString());
			}

			@Override
			public void throwException(TaskContext taskContext, Throwable e) {
				if(taskContext != null)
					logger.info(taskContext.getJobTaskMetrics().toString());
			}
		});

		importBuilder.setContinueOnError(true);//任务出现异常，是否继续执行作业：true（默认值）继续执行 false 中断作业执行
		importBuilder.setPrintTaskLog(false);

		/**
		 * 构建和启动导出elasticsearch数据并发送kafka同步作业
		 */
		DataStream dataStream = importBuilder.builder();
		dataStream.execute();

	}

}
