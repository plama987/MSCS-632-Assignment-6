import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Assignment 6: Multi-threaded Data Processing System
 * Demonstrates a shared task queue, worker threads, synchronized output,
 * interruption handling, and file I/O error handling.
 */
public class DataProcessingSystem {
    private static final Logger LOGGER =
            Logger.getLogger(DataProcessingSystem.class.getName());
    private static final int WORKER_COUNT = 3;
    private static final int TASK_COUNT = 10;
    private static final Path OUTPUT_FILE = Path.of("java-results.txt");

    static class Task {
        private final int id;
        private final String payload;
        private final boolean poisonPill;

        Task(int id, String payload, boolean poisonPill) {
            this.id = id;
            this.payload = payload;
            this.poisonPill = poisonPill;
        }

        static Task stopSignal() {
            return new Task(-1, "", true);
        }
    }

    static class Result {
        private final int taskId;
        private final String value;

        Result(int taskId, String value) {
            this.taskId = taskId;
            this.value = value;
        }

        @Override
        public String toString() {
            return "Task " + taskId + " -> " + value;
        }
    }

    private final BlockingQueue<Task> taskQueue = new LinkedBlockingQueue<>();
    private final List<Result> results =
            Collections.synchronizedList(new ArrayList<>());
    private final Object fileLock = new Object();
    private final AtomicInteger completedCount = new AtomicInteger();

    private void addTask(Task task) {
        try {
            taskQueue.put(task);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.log(Level.WARNING, "Interrupted while adding a task.", e);
        }
    }

    private Task getTask() throws InterruptedException {
        return taskQueue.take(); // Blocks safely until a task is available.
    }

    private String process(Task task) throws InterruptedException {
        Thread.sleep(250); // Simulate computational work.
        return task.payload.toUpperCase();
    }

    private void appendResult(Result result) throws IOException {
        // One worker at a time may append to the shared output file.
        synchronized (fileLock) {
            try (BufferedWriter writer = Files.newBufferedWriter(
                    OUTPUT_FILE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND)) {
                writer.write(result.toString());
                writer.newLine();
            }
        }
    }

    private void worker(int workerId) {
        LOGGER.info("Worker " + workerId + " started.");
        try {
            while (true) {
                Task task = getTask();
                if (task.poisonPill) {
                    break;
                }

                String processed = process(task);
                Result result = new Result(task.id, processed);
                results.add(result);

                try {
                    appendResult(result);
                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE,
                            "Worker " + workerId + " could not write task "
                                    + task.id + " to output file.", e);
                }

                int done = completedCount.incrementAndGet();
                LOGGER.info("Worker " + workerId + " completed task "
                        + task.id + " (" + done + "/" + TASK_COUNT + ").");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.log(Level.WARNING, "Worker " + workerId + " was interrupted.", e);
        } catch (RuntimeException e) {
            LOGGER.log(Level.SEVERE, "Worker " + workerId + " encountered an error.", e);
        } finally {
            LOGGER.info("Worker " + workerId + " stopped.");
        }
    }

    public void run() {
        try {
            Files.deleteIfExists(OUTPUT_FILE); // Fresh output for each run.
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Could not clear old output file.", e);
        }

        for (int i = 1; i <= TASK_COUNT; i++) {
            addTask(new Task(i, "ride-request-" + i, false));
        }
        for (int i = 0; i < WORKER_COUNT; i++) {
            addTask(Task.stopSignal());
        }

        ExecutorService executor = Executors.newFixedThreadPool(WORKER_COUNT);
        for (int i = 1; i <= WORKER_COUNT; i++) {
            final int workerId = i;
            executor.submit(() -> worker(workerId));
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                LOGGER.warning("Workers did not finish in time; requesting shutdown.");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
            LOGGER.log(Level.WARNING, "Main thread interrupted while waiting.", e);
        }

        System.out.println("\nProcessed results (" + results.size() + "):");
        synchronized (results) {
            results.stream()
                    .sorted((a, b) -> Integer.compare(a.taskId, b.taskId))
                    .forEach(System.out::println);
        }
        System.out.println("Output file: " + OUTPUT_FILE.toAbsolutePath());
        System.out.println("Expected tasks: " + TASK_COUNT
                + "; completed tasks: " + completedCount.get());
    }

    public static void main(String[] args) {
        LOGGER.info("Data processing system starting at " + LocalTime.now());
        new DataProcessingSystem().run();
        LOGGER.info("Data processing system finished.");
    }
}
