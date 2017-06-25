package octo.mythbuster.spark;

import java.util.Random;

/**
 * Created by adrien on 5/17/17.
 */
public class JVMBenchmark {

    public static Adder createAdder() {
        return new AdderImpl();
    }

    public static Subtractor createSubtractor() {
        return new SubtractorImpl();
    }

    public static void main(String[] arguments) {
        long startTime, stopTime;
        Random random;
        int result;

        random = new Random(123_456);
        startTime = System.nanoTime();
        Adder adder = createAdder();
        Subtractor subtractor = createSubtractor();

        result = 0;
        for (long i = 0; i < 1_000_000_00_000l; i++) {
            result += adder.add(subtractor.subtract(random.nextInt(), random.nextInt()), subtractor.subtract(random.nextInt(), random.nextInt()));
        }
        stopTime = System.nanoTime();
        System.out.println(" ==> " + (stopTime - startTime) + " / " + result);


        random = new Random(123_456);
        startTime = System.nanoTime();
        result = 0;
        for (long i = 0; i < 1_000_000_00_000l; i++) {
            result += result = (random.nextInt() - random.nextInt()) + (random.nextInt() - random.nextInt());
        }
        stopTime = System.nanoTime();
        System.out.println(" ==> " + (stopTime - startTime) + " / " + result);
    }



}
