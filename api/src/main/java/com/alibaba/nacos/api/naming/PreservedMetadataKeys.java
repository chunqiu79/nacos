/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.api.naming;

import com.alibaba.nacos.api.common.Constants;

/**
 * 客户端注册到服务端时候的元数据信息
 */
public class PreservedMetadataKeys {
    
    /**
     * The key to indicate the registry source of service instance, such as Dubbo, SpringCloud, etc.
     */
    public static final String REGISTER_SOURCE = "preserved.register.source";
    /**
     * 多久没有收到心跳标记为不健康
     * 默认值：{@link Constants#DEFAULT_HEART_BEAT_TIMEOUT}
     */
    public static final String HEART_BEAT_TIMEOUT = "preserved.heart.beat.timeout";
    /**
     * 多久没有收到心跳删除实例
     * 默认值：{@link Constants#DEFAULT_IP_DELETE_TIMEOUT}
     */
    public static final String IP_DELETE_TIMEOUT = "preserved.ip.delete.timeout";
    /**
     * 心跳间隔时间
     * 默认值：{@link Constants#DEFAULT_HEART_BEAT_INTERVAL}
     */
    public static final String HEART_BEAT_INTERVAL = "preserved.heart.beat.interval";
    
    public static final String INSTANCE_ID_GENERATOR = "preserved.instance.id.generator";
}
