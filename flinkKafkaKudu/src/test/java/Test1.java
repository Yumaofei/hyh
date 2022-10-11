import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Test1 {
    public static void main(String[] args) {
        System.out.println("abd");
        String s = "{\"id\":2395358,\"valuess\":112,\"timess\":\"2022-10-11 17:56:35\"}";
        System.out.println(21 % 10);

        invoke(" s s ");
        System.out.println(" s s ".trim());
    }

    public static void invoke(String s) {
        if ((s.contains("\n")) || (s.contains("\r")) || (s.contains("\r\n"))) {
            s = s.trim();
        }
        System.out.println(s);
        List<String> lines=new ArrayList<>(Arrays.asList(s.split("\n",-1)));
        for(String line:lines){
            //xxx为分隔符
            List<String> strs = new ArrayList<>(Arrays.asList(line.split("xxx", -1)));
            String op_type = strs.get(0);
            //这里可以写一些业务逻辑
            if (op_type.toUpperCase().equals("I")) {
            } else if (op_type.toUpperCase().equals("U")) {
            } else if (op_type.toUpperCase().equals("D")) {
            }
        }
    }
}