package org.frameworkset.elasticsearch.imp;

import org.frameworkset.elasticsearch.serial.SerialUtil;
import org.frameworkset.runtime.CommonLauncher;
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.output.fileftp.FilenameGenerator;
import org.frameworkset.tran.output.ftp.FtpOutConfig;
import org.frameworkset.tran.plugin.es.input.ElasticsearchInputConfig;
import org.frameworkset.tran.plugin.file.output.FileOutputConfig;
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.util.RecordGenerator;

import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ES2FileFtpBatchSplitFile22 {

	public static void main(String[] args){
		ImportBuilder importBuilder = new ImportBuilder();
		//importBuilder.setBatchSize(500).setFetchSize(1000);
		String ftpIp = CommonLauncher.getProperty("ftpIP","10.13.6.127");//同时指定了默认值
		FileOutputConfig fileFtpOupputConfig = new FileOutputConfig();
		FtpOutConfig ftpOutConfig = new FtpOutConfig();
		fileFtpOupputConfig.setFtpOutConfig(ftpOutConfig);

		ftpOutConfig.setFtpIP(ftpIp);

		ftpOutConfig.setFtpPort(22);
		ftpOutConfig.setFtpUser("test");
		ftpOutConfig.setFtpPassword("test123");
		ftpOutConfig.setRemoteFileDir("/data/workdir");
		ftpOutConfig.setKeepAliveTimeout(100000);
		ftpOutConfig.setTransferEmptyFiles(true);
		ftpOutConfig.setFailedFileResendInterval(-1);
		ftpOutConfig.setBackupSuccessFiles(true);

		ftpOutConfig.setSuccessFilesCleanInterval(5000);
		ftpOutConfig.setFileLiveTime(86400);//设置上传成功文件备份保留时间，默认2天
		//fileFtpOupputConfig.setMaxFileRecordSize(1000);//每千条记录生成一个文件
		fileFtpOupputConfig.setFileDir("F:\\data\\workdir");
		//自定义文件名称
		fileFtpOupputConfig.setFilenameGenerator(new FilenameGenerator() {
			@Override
			public String genName( TaskContext taskContext,int fileSeq) {
				//fileSeq为切割文件时的文件递增序号
				String time = (String)taskContext.getTaskData("time");//从任务上下文中获取本次任务执行前设置时间戳
				String _fileSeq = fileSeq+"";
				int t = 6 - _fileSeq.length();
				if(t > 0){
					String tmp = "";
					for(int i = 0; i < t; i ++){
						tmp += "0";
					}
					_fileSeq = tmp+_fileSeq;
				}

				return "HN_BOSS_TRADE"+_fileSeq + "_"+time +"_" + _fileSeq+".txt";
			}
		});
		//指定文件中每条记录格式，不指定默认为json格式输出
		fileFtpOupputConfig.setRecordGenerator(new RecordGenerator() {
			@Override
			public void buildRecord(Context taskContext, CommonRecord record, Writer builder) {
				//直接将记录按照json格式输出到文本文件中
				SerialUtil.normalObject2json(record.getDatas(), builder);//获取记录中的字段数据
			}
		});
		importBuilder.setOutputConfig(fileFtpOupputConfig);
		importBuilder.setIncreamentEndOffset(300);//单位秒，同步从上次同步截止时间当前时间前5分钟的数据，下次继续从上次截止时间开始同步数据
		//vops-chbizcollect-2020.11.26,vops-chbizcollect-2020.11.27
		ElasticsearchInputConfig elasticsearchInputConfig = new ElasticsearchInputConfig();
		elasticsearchInputConfig
				.setDslFile("dsl2ndSqlFile.xml")
				.setDslName("scrollQuery")
				.setScrollLiveTime("10m")
//				.setSliceQuery(true)
//				.setSliceSize(5)
				.setQueryUrl("cloud_address_resolve_uat/_search");
		elasticsearchInputConfig.setSourceElasticsearch("default");
		importBuilder.setInputConfig(elasticsearchInputConfig);

		//设置任务执行拦截器，可以添加多个
		importBuilder.addCallInterceptor(new CallInterceptor() {
			@Override
			public void preCall(TaskContext taskContext) {
				String formate = "yyyyMMddHHmmss";
				//HN_BOSS_TRADE00001_YYYYMMDDHHMM_000001.txt
				SimpleDateFormat dateFormat = new SimpleDateFormat(formate);
				String time = dateFormat.format(new Date());
				taskContext.addTaskData("time",time);
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
//		//设置任务执行拦截器结束，可以添加多个

		/**
		 * 内置线程池配置，实现多线程并行数据导入功能，作业完成退出时自动关闭该线程池
		 */
		importBuilder.setParallel(false);//设置为多线程并行批量导入,false串行
		importBuilder.setQueue(1);//设置批量导入线程池等待队列长度
		importBuilder.setThreadCount(1);//设置批量导入线程池工作线程数量
		importBuilder.setContinueOnError(true);//任务出现异常，是否继续执行作业：true（默认值）继续执行 false 中断作业执行
		importBuilder.setPrintTaskLog(true);

		/**
		 * 启动es数据导入文件并上传sftp/ftp作业
		 */
		DataStream dataStream = importBuilder.builder();
		dataStream.execute();//启动同步作业
	}
	
}
