package no.ssb.dapla.spark.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ApplicationRegistry {

    private final Map<Class<?>, Object> instanceByType = new ConcurrentHashMap<>();

    public <T> T put(Class<T> clazz, T instance) {
        return (T) instanceByType.put(clazz, instance);
    }

    public <T> T get(Class<T> clazz) {
        return (T) instanceByType.get(clazz);
    }

}
