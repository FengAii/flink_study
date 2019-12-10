package myFirstFrame;

/**
 * 框架，实现的是 1，多，1的实现；
 */
public abstract class parent {

    /**
     * 执行1次，开始
     */
    public abstract void start();

    /**
     * 执行多次
     */
    public abstract void show();

    /**
     * 执行1次，最后
     */
    public abstract void end();

    /**
     * show 方法执行的次数
     * @return
     */
    public abstract int times();

}
