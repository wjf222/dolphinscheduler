package org.apache.dolphinscheduler.plugin.registry.etcd;

import org.apache.dolphinscheduler.registry.api.ConnectionListener;
import org.apache.dolphinscheduler.registry.api.Registry;
import org.apache.dolphinscheduler.registry.api.SubscribeListener;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;


@Component
@ConditionalOnProperty(prefix = "registry", name = "type", havingValue = "etcd")
public class EtcdRegistry implements Registry {
    @Override
    public boolean subscribe(String path, SubscribeListener listener) {
        return false;
    }

    @Override
    public void unsubscribe(String path) {

    }

    @Override
    public void addConnectionStateListener(ConnectionListener listener) {

    }

    @Override
    public String get(String key) {
        return null;
    }

    @Override
    public void put(String key, String value, boolean deleteOnDisconnect) {

    }

    @Override
    public void delete(String key) {

    }

    @Override
    public Collection<String> children(String key) {
        return null;
    }

    @Override
    public boolean exists(String key) {
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
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
