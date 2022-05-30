package cn.whu.wy.kafka.test.clients;


import com.google.common.reflect.ClassPath;
import com.google.gson.Gson;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author WangYong
 * Date 2021/07/03
 * Time 20:52
 */
public class Util {

    public static final String PAY_REQ_TOPIC = "pay-request";
    public static final String PAY_RESP_TOPIC = "pay-response";

    public static Gson gson() {
        return new Gson();
    }

    public static void main(String[] args) throws IOException {
        Map<String, Integer> map = new HashMap<>();
        map.put("wang", 90);
        map.put("zhang", 100);
        System.out.println(map.entrySet().stream().map(Map.Entry::getValue).reduce(Integer::sum));
        System.out.println(map.entrySet().stream().map(Map.Entry::getValue).mapToInt(Integer::intValue).sum());


        System.out.println(findAllClassesUsingGoogleGuice("cn.whu.wy.kafka.test.clients.bean"));
        System.out.println(findAllClassesUsingReflectionsLibrary("cn.whu.wy.kafka.test.clients.bean"));


    }

    public static Set<Class> findAllClassesUsingGoogleGuice(String packageName) throws IOException {
        return ClassPath.from(ClassLoader.getSystemClassLoader())
                .getAllClasses()
                .stream()
                .filter(clazz -> clazz.getPackageName()
                        .equalsIgnoreCase(packageName))
                .filter(clazz -> !clazz.getName().contains("$"))
                .map(clazz -> clazz.load())
                .collect(Collectors.toSet());
    }

    public static Set<Class> findAllClassesUsingReflectionsLibrary(String packageName) {
        Reflections reflections = new Reflections(packageName, new SubTypesScanner(false), new TypeAnnotationsScanner());
        return reflections.getSubTypesOf(Object.class)
                .stream()
                .filter(aClass -> !aClass.getName().contains("$"))
                .collect(Collectors.toSet());
    }
}
