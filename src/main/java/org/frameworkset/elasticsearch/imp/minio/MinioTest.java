package org.frameworkset.elasticsearch.imp.minio;
/**
 * Copyright 2025 bboss
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

import org.frameworkset.nosql.minio.Minio;
import org.frameworkset.nosql.minio.MinioConfig;
import org.frameworkset.nosql.minio.MinioHelper;

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2025/3/20
 */
public class MinioTest {
    public static void main(String[] args) throws Exception {
        //1. 初始化Minio数据源chan_fqa，用来操作Minio数据库，一个Minio数据源只需要定义一次即可，后续通过名称miniotest反复引用，多线程安全
        // 可以通过以下方法定义多个Minio数据源，只要name不同即可，通过名称引用对应的数据源
        MinioConfig minioConfig = new MinioConfig();

        minioConfig.setEndpoint("http://172.24.176.18:9000");

        minioConfig.setName("miniotest");
        minioConfig.setAccessKeyId("N3XNZFqSZfpthypuoOzL");
        minioConfig.setSecretAccesskey("2hkDSEll1Z7oYVfhr0uLEam7r0M4UWT8akEBqO97");
        minioConfig.setConnectTimeout(5000l);
        minioConfig.setReadTimeout(5000l);
        minioConfig.setWriteTimeout(5000l);

        minioConfig.setMaxFilePartSize(10*1024*1024*1024);
        boolean result = MinioHelper.init(minioConfig);

        //获取数据源
        Minio minio = MinioHelper.getMinio("miniotest");
        //操作minio
        minio.createBucket("filedown");
        minio.uploadObject("C:/data/filedown/HN_BOSS_TRADE_202501092032_000001.txt","filedown","filedown/HN_BOSS_TRADE_202501092032_000001.txt");
        minio.downloadObject("filedown","filedown/HN_BOSS_TRADE_202501092032_000001.txt","C:/data/filedown/xxxxxaaaa.txt");
        minio.deleteOssFile("filedown","filedown/HN_BOSS_TRADE_202501092032_000001.txt");
    }
}
