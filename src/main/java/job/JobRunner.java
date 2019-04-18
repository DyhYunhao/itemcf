package job;

import Step1.MR1;
import Step2.MR2;
import Step3.MR3;
import Step4.MR4;
import Step5.MR5;

public class JobRunner {
    public static void main(String[] args) {
        int status1 = -1;
        int status2 = -1;
        int status3 = -1;
        int status4 = -1;
        int status5 = -1;

        status1 = new MR1().run();
        if (status1 == 1){
            System.out.println("Step1 运行成功！ 开始运行Step2......");
            status2 = new MR2().run();
        }else {
            System.out.println("Step1 运行失败！");
        }
        if (status2 == 1){
            System.out.println("Step2 运行成功！ 开始运行Step3......");
            status3 = new MR3().run();
        }else {
            System.out.println("Step2 运行失败！");
        }
        if (status3 == 1){
            System.out.println("Step3 运行成功！ 开始运行Step4......");
            status4 = new MR4().run();
        }else {
            System.out.println("Step3 运行失败！");
        }
        if (status4 == 1){
            System.out.println("Step4 运行成功！ 开始运行Step5......");
            status5 = new MR5().run();
        }else {
            System.out.println("Step4 运行失败！");
        }
        if (status5 == 1){
            System.out.println("Step5 运行成功!");
        }else {
            System.out.println("Step5 运行失败！");
        }
    }
}
