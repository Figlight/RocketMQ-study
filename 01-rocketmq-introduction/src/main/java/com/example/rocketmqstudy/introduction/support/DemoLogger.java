package com.example.rocketmqstudy.introduction.support;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * 演示日志工具，用于输出带时间戳的控制台日志。
 */
public final class DemoLogger {

    /**
     * 控制台时间格式化器，用于让日志输出更容易观察先后顺序。
     */
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    /**
     * 工具类不需要被实例化。
     */
    private DemoLogger() {
    }

    /**
     * 输出带时间戳的演示日志。
     *
     * @param message 需要输出到控制台的日志内容。
     */
    public static void log(String message) {
        System.out.println(LocalTime.now().format(TIME_FORMATTER) + " | " + message);
    }
}
