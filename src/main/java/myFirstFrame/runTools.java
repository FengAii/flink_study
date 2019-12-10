package myFirstFrame;

import java.lang.reflect.Method;

/**
 * 框架的执行工具, 只需要继承 parents， 实现 1， 多， 1 的简单实现；
 */
public class runTools {

    public static void run(Object o) throws Exception{

        // 给类地址，首先获取 类对象
        // 可以反射，获取到 对象， 后续一样的操作；

        Class<?> clazz = o.getClass();

        // 这里就可以控制 你有多少东西
        // 每个方法 执行几次
        // 就是执行  的顺序， 都可以在这里完成；
        Method start = clazz.getDeclaredMethod("start");
        Method show = clazz.getDeclaredMethod("show");
        Method end = clazz.getDeclaredMethod("end");
        Method times = clazz.getDeclaredMethod("times");

        Object invoke1 = times.invoke(o);
        int k = Integer.parseInt(String.valueOf(invoke1));

        start.invoke(o);

        for (int i = 0; i < k ; i++) {
            show.invoke(o);
        }

        end.invoke(o);

    }

}
