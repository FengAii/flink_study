package myFirstFrame;

/**
 * 需要手动实现的类；
 */
public class son extends parent {

    int i = 0;

    @Override
    public void start() {
        System.out.println("son:i am start");
    }

    @Override
    public void show() {
        i++;
        System.out.println("son:i am show");
    }

    @Override
    public void end() {

        System.out.println("son:i am end");
        System.out.println("final i is "+i);
    }

    @Override
    public int times() {
        return 5;
    }


    // 可以不写这个 run
    // 就跟 mr一样 ， 写一个 driver， 实际上 配置文件里都在mr的run方法
    // 就是根据传入的参数， 通过反射， 执行固定的方法名字；
    // 这就是 框架；
    public static void main(String[] args) throws Exception {
        runTools.run(new son());
    }

}

