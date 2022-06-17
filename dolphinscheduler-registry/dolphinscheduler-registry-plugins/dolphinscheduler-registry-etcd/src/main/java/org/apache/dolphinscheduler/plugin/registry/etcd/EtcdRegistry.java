package org.apache.dolphinscheduler.plugin.registry.etcd;

import io.etcd.jetcd.*;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.support.Observers;
import io.etcd.jetcd.watch.WatchEvent;
import org.apache.dolphinscheduler.registry.api.*;
import org.apache.dolphinscheduler.registry.api.RegistryProperties.ZookeeperProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Component
@ConditionalOnProperty(prefix = "registry", name = "type", havingValue = "etcd")
public class EtcdRegistry implements Registry {
    // TODO 更改为Etcd专有的属性类
    private final ZookeeperProperties properties;
    private final Client client;
    private final Map<String, Watch.Watcher> watcherMap = new ConcurrentHashMap<>();
    // 保存锁和对应的租约
    private static final ThreadLocal<Set<String>> threadLocalLockSet = new ThreadLocal<>();
    // 锁保存的时间
    private static Long TIME_TO_LIVE_SECONDS=30L;
    String PATH_SEPARATOR = "/";
    // TODO Specify the kv revision. Default is latest
    Long rev = 0L;
    /** TODO
     设置重试机制
     */
    public EtcdRegistry(RegistryProperties registryProperties) {
        properties = registryProperties.getZookeeper();
        client = Client.builder().endpoints(properties.getConnectString().split(","))
                .namespace(byteSequence(properties.getNamespace())) // TODO Retry机制设置
                .connectTimeout(properties.getConnectionTimeout())
                // SessionTime设置
                .build();
        // TODO 权限设置
    }


    @Override
    public boolean subscribe(String path, SubscribeListener listener) {
        try {
            ByteSequence watchKey = byteSequence(path);
            WatchOption watchOption = WatchOption.newBuilder().isPrefix(true).build();
            watcherMap.computeIfAbsent(path, $ -> client.getWatchClient().watch(watchKey, watchOption,watchResponse -> {
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
            List<KeyValue> keyValues = client.getKVClient().get(byteSequence(key)).get().getKvs();
            return keyValues.iterator().next().getValue().toString(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RegistryException("zookeeper get data error", e);
        }
    }

    @Override
    public void put(String key, String value, boolean deleteOnDisconnect) {
        // TODO 采用租约机制实现断开连接时删除
        try{
            if(deleteOnDisconnect) {
                // 建立租约，设置租约一直保持
                long leaseId = client.getLeaseClient().grant(TIME_TO_LIVE_SECONDS).get().getID();
                client.getLeaseClient().keepAlive(leaseId, Observers.observer(response -> {
                }));
                PutOption putOption = PutOption.newBuilder().withLeaseId(leaseId).build();
                client.getKVClient().put(byteSequence(key), byteSequence(value),putOption).get();
            }else{
                // 不设置租约
                client.getKVClient().put(byteSequence(key), byteSequence(value)).get();
            }
        } catch (Exception e){
            throw new RegistryException("Failed to put registry key: " + key, e);
        }
    }

    @Override
    public void delete(String key) {
        try {
            // 设置根据前缀删除
            DeleteOption deleteOption =DeleteOption.newBuilder().isPrefix(true).build();
            client.getKVClient().delete(ByteSequence.from(key, StandardCharsets.UTF_8), DeleteOption.newBuilder().isPrefix(true).build());
        }  catch (Exception e) {
            throw new RegistryException("Failed to delete registry key: " + key, e);
        }
    }

    @Override
    public Collection<String> children(String key) {
        // 获取所有子元素并排序 ,排序ASCEND
        String prefix = key + PATH_SEPARATOR;
        GetOption getOption = GetOption.newBuilder().isPrefix(true).withSortField(GetOption.SortTarget.KEY).withSortOrder(GetOption.SortOrder.ASCEND).build();
        try {
            List<KeyValue> keyValues = client.getKVClient().get(byteSequence(prefix),getOption).get().getKvs();
            return keyValues.stream().map(e -> getSubNodeKeyName(prefix, e.getKey().toString(StandardCharsets.UTF_8))).distinct().collect(Collectors.toList());
        } catch (Exception e){
            throw new RegistryException("zookeeper get children error", e);
        }
    }

    private String getSubNodeKeyName(final String prefix, final String fullPath) {
        String pathWithoutPrefix = fullPath.substring(prefix.length());
        return pathWithoutPrefix.contains(PATH_SEPARATOR) ? pathWithoutPrefix.substring(0, pathWithoutPrefix.indexOf(PATH_SEPARATOR)) : pathWithoutPrefix;
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
        // TODO 可重入排他锁
        Lock lockClient = client.getLockClient();
        Lease leaseClient = client.getLeaseClient();
        // 创建租约，Lock会在租约失效时直接释放锁
        try {
            // TODO magic number 租约时间，待设置 持有时间可用PropertyKey 设置
            long leaseId = leaseClient.grant(30).get().getID();
            lockClient.lock(byteSequence(key),leaseId).get().getKey();
            return true;
        } catch (Exception e) {
            throw new RegistryException("zookeeper release lock error", e);
        }
    }

    @Override
    public boolean releaseLock(String key) {
        Lock lockClient = client.getLockClient();
        try {
            lockClient.unlock(byteSequence(key)).get();
        } catch (Exception e){
            throw new RegistryException("zookeeper release lock error", e);
        }
        return true;
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

