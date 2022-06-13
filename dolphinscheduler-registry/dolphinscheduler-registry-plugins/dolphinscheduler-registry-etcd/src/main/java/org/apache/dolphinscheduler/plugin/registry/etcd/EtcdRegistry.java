package org.apache.dolphinscheduler.plugin.registry.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import org.apache.dolphinscheduler.registry.api.*;
import org.apache.dolphinscheduler.registry.api.RegistryProperties.ZookeeperProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


@Component
@ConditionalOnProperty(prefix = "registry", name = "type", havingValue = "etcd")
public class EtcdRegistry implements Registry {
    // TODO 更改为Etcd专有的属性类
    private final ZookeeperProperties properties;
    private final Client client;
    private final Map<String, Watch.Watcher> watcherMap = new ConcurrentHashMap<>();
    // TODO Specify the kv revision
    Long rev = 0L;
    /** TODO
     设置重试机制
     设置命令空间
     设置会话超时时间
     设置连接超时时间
     设置连接地址
     */
    public EtcdRegistry(RegistryProperties registryProperties) {
        properties = registryProperties.getZookeeper();
        client = Client.builder().endpoints(properties.getConnectString().split(",")).build();
    }


    @Override
    public boolean subscribe(String path, SubscribeListener listener) {
        try {
            ByteSequence watchKey = byteSequence(path);
            watcherMap.computeIfAbsent(path, $ -> client.getWatchClient().watch(watchKey, watchResponse -> {
                for (WatchEvent event : watchResponse.getEvents()) {
                    listener.notify(new EventAdaptor(event, path));
                }
            }));
        } catch (Exception e){
            watcherMap.remove(path);
            throw new RegistryException("Failed to subscribe listener for key: " + path, e);
        }
        return true;
    }

    @Override
    public void unsubscribe(String path) {
        // TODO 根据路径关闭监听的Path
        watcherMap.get(path).close();
    }

    @Override
    public void addConnectionStateListener(ConnectionListener listener) {
        // TODO 连接状态监听
    }

    @Override
    public String get(String key) {
        try {
            GetResponse getResponse = client.getKVClient().get(byteSequence(key), GetOption.newBuilder().withRevision(rev).build()).get();
            // TODO 返回的是一组String，需要判别与返回一个String的区别,GetOption isPrefix
            return String.valueOf(getResponse.getKvs());
        } catch (Exception e) {
            throw new RegistryException("zookeeper get data error", e);
        }
    }

    @Override
    public void put(String key, String value, boolean deleteOnDisconnect) {
        // TODO 采用租约机制实现断开连接时删除
        try{
            client.getKVClient().put(byteSequence(key),byteSequence(value)).get();
        } catch (Exception e){
            throw new RegistryException("Failed to put registry key: " + key, e);
        }
    }

    @Override
    public void delete(String key) {
        try {
            // TODO DELETE Option 添加
            client.getKVClient().delete(ByteSequence.from(key,StandardCharsets.UTF_8)).get();
        }  catch (Exception e) {
            throw new RegistryException("Failed to delete registry key: " + key, e);
        }
    }

    @Override
    public Collection<String> children(String key) {
        // TODO 获取所有子元素并排序
        return null;
    }

    @Override
    public boolean exists(String key) {
        // TODO 判断元素是否存在
        GetOption getOption = GetOption.newBuilder().withCountOnly(true).build();
        try {
            if (client.getKVClient().get(byteSequence(key),getOption).get().getCount() >= 1)
                return true;
        }catch (Exception e) {
            throw new RegistryException("zookeeper check key is existed error", e);
        }
        return false;
    }

    @Override
    public boolean acquireLock(String key) {
        return false;
    }

    @Override
    public boolean releaseLock(String key) {
        return false;
    }

    @Override
    public Duration getSessionTimeout() {
        return properties.getSessionTimeout();
    }

    @Override
    public void close() throws IOException {
        watcherMap.values().forEach(watcher -> watcher.close());
        client.close();
    }

    private static ByteSequence byteSequence(String val){
        return ByteSequence.from(val,StandardCharsets.UTF_8);
    }
    static final class EventAdaptor extends Event {
        public EventAdaptor(WatchEvent event, String key) {
            key(key);

            switch (event.getEventType()) {
                case PUT:
                    type(Type.ADD);
                    break;
                // TODO 添加数据修改操作
                case UNRECOGNIZED:
                    type(Type.UPDATE);
                    break;
                case DELETE:
                    type(Type.REMOVE);
                    break;
                default:
                    break;
            }
            // TODO 添加数据处理
//            if (data != null) {
//                path(data.getPath());
//                data(new String(data.getData()));
//            }
        }
    }
}

