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

package com.alibaba.nacos.naming.core;

import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.core.cluster.MemberChangeListener;
import com.alibaba.nacos.core.cluster.MemberUtil;
import com.alibaba.nacos.core.cluster.MembersChangeEvent;
import com.alibaba.nacos.core.cluster.NodeState;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.sys.env.EnvUtil;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Distro mapper, judge which server response input service.
 *
 * @author nkorange
 */
@Component("distroMapper")
public class DistroMapper extends MemberChangeListener {
    
    /**
     * List of service nodes, you must ensure that the order of healthyList is the same for all nodes.
     */
    private volatile List<String> healthyList = new ArrayList<>();
    
    private final SwitchDomain switchDomain;
    
    private final ServerMemberManager memberManager;
    
    public DistroMapper(ServerMemberManager memberManager, SwitchDomain switchDomain) {
        this.memberManager = memberManager;
        this.switchDomain = switchDomain;
    }
    
    public List<String> getHealthyList() {
        return healthyList;
    }
    
    /**
     * init server list.
     */
    @PostConstruct
    public void init() {
        NotifyCenter.registerSubscriber(this);
        this.healthyList = MemberUtil.simpleMembers(memberManager.allMembers());
    }
    
    public boolean responsible(Cluster cluster, Instance instance) {
        return switchDomain.isHealthCheckEnabled(cluster.getServiceName()) && !cluster.getHealthCheckTask()
                .isCancelled() && responsible(cluster.getServiceName()) && cluster.contains(instance);
    }
    
    /**
     * 判断当前服务端是否对当前传入的 responsibleTag 进行处理
     * 如果是v1版本的话，responsibleTag就是服务名，如果是v2版本的话，responsibleTag就是ip+":"+port
     * 所以，其实就是把所有注册到nacos服务端的服务进行了划分（responsibleTag%servers.size()），其实就是一致性hash算法（redis也用了这个）
     * 保证1个集群下只会有1个服务端对当前传入的responsibleTag返回true
     *
     * Judge whether current server is responsible for input tag.
     *
     * @param responsibleTag responsible tag, serviceName for v1 and ip:port for v2
     * @return true if input service is response, otherwise false
     */
    public boolean responsible(String responsibleTag) {
        final List<String> servers = healthyList;
        
        if (!switchDomain.isDistroEnabled() || EnvUtil.getStandaloneMode()) {
            return true;
        }
        
        if (CollectionUtils.isEmpty(servers)) {
            // means distro config is not ready yet
            return false;
        }
        
        String localAddress = EnvUtil.getLocalAddress();
        int index = servers.indexOf(localAddress);
        int lastIndex = servers.lastIndexOf(localAddress);
        /*
         * 如果本机地址不在健康节点列表中（即index和lastIndex都小于0），则返回true
         * 这里为什么返回true？
         * 实际上，当节点不在健康列表中时，通常意味着该节点已被排除在集群之外（如宕机），
         * 但这里返回true可能是为了处理一些边界情况（例如节点刚刚启动还未加入健康列表），
         * 让节点自己处理请求，避免出现所有节点都不处理的情况。但这种情况在实际运行中应该避免。
         */
        if (lastIndex < 0 || index < 0) {
            return true;
        }
        /*
         * 如：这里的的 responsibleTag 表示的是服务名
         * 那么就是 每个服务端对属于自己的服务才返回true
         * servers = ["A", "B", "C"]  // 健康节点
         * localAddress = "B"        // 当前节点
         * responsibleTag = "order-service" → 哈希取模后 target=1 (B的索引)
         */
        int target = distroHash(responsibleTag) % servers.size();
        return target >= index && target <= lastIndex;
    }
    
    /**
     * Calculate which other server response input tag.
     *
     * @param responsibleTag responsible tag, serviceName for v1 and ip:port for v2
     * @return server which response input service
     */
    public String mapSrv(String responsibleTag) {
        final List<String> servers = healthyList;
        
        if (CollectionUtils.isEmpty(servers) || !switchDomain.isDistroEnabled()) {
            return EnvUtil.getLocalAddress();
        }
        
        try {
            // 先取hash 然后取模
            int index = distroHash(responsibleTag) % servers.size();
            return servers.get(index);
        } catch (Throwable e) {
            Loggers.SRV_LOG
                    .warn("[NACOS-DISTRO] distro mapper failed, return localhost: " + EnvUtil.getLocalAddress(), e);
            return EnvUtil.getLocalAddress();
        }
    }
    
    private int distroHash(String responsibleTag) {
        return Math.abs(responsibleTag.hashCode() % Integer.MAX_VALUE);
    }
    
    @Override
    public void onEvent(MembersChangeEvent event) {
        // Here, the node list must be sorted to ensure that all nacos-server's
        // node list is in the same order
        List<String> list = MemberUtil.simpleMembers(MemberUtil.selectTargetMembers(event.getMembers(),
                member -> NodeState.UP.equals(member.getState()) || NodeState.SUSPICIOUS.equals(member.getState())));
        Collections.sort(list);
        Collection<String> old = healthyList;
        healthyList = Collections.unmodifiableList(list);
        Loggers.SRV_LOG.info("[NACOS-DISTRO] healthy server list changed, old: {}, new: {}", old, healthyList);
    }
    
    @Override
    public boolean ignoreExpireEvent() {
        return true;
    }
}
