package org.example.opentext;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

public class Main {

    /**
     * Enumeration of task types.
     */
    public enum TaskType {
        READ,
        WRITE,
    }

    public interface TaskExecutor {
        /**
         * Submit new task to be queued and executed.
         *
         * @param task Task to be executed by the executor. Must not be null.
         * @return Future for the task asynchronous computation result.
         */

        <T> Future<T> submitTask(Task<T> task);
        void shutdown();
    }

    /**
     * Representation of computation to be performed by the {@link TaskExecutor}.
     *
     * @param taskUUID Unique task identifier.
     * @param taskGroup Task group.
     * @param taskType Task type.
     * @param taskAction Callable representing task computation and returning the result.
     * @param <T> Task computation result value type.
     */


    public record Task<T>(
            UUID taskUUID,
            TaskGroup taskGroup,
            TaskType taskType,
            Callable<T> taskAction
    ) {
        public Task {
            if (taskUUID == null || taskGroup == null || taskType == null || taskAction == null) {
                throw new IllegalArgumentException("All parameters must not be null");
            }
        }
    }

    /**
     * Task group.
     *
     * @param groupUUID Unique group identifier.
     */

    public record TaskGroup(UUID groupUUID) {
        public TaskGroup {
            if (groupUUID == null) {
                throw new IllegalArgumentException("All parameters must not be null");
            }
        }
    }

    public static class TaskExecutorService implements TaskExecutor {
        private final ExecutorService executorService;
        private final BlockingQueue<WrappedTask<?>> taskQueue;
        private final Map<TaskGroup, Object> groupLocks;
        private final Thread workerThread;
        private volatile boolean isRunning = true;

        public TaskExecutorService(int maxThreads) {
            this.executorService = Executors.newFixedThreadPool(maxThreads);
            this.taskQueue = new LinkedBlockingQueue<>();
            this.groupLocks = new ConcurrentHashMap<>();

            this.workerThread = new Thread(this::processTasks);
            this.workerThread.start();
        }

        @Override
        public <T> Future<T> submitTask(Task<T> task) {
            CompletableFuture<T> future = new CompletableFuture<>();
            WrappedTask<T> wrappedTask = new WrappedTask<>(task, future);
            taskQueue.offer(wrappedTask);
            return future;
        }

        private void processTasks() {
            while (isRunning || !taskQueue.isEmpty()) {  // Process all pending tasks before shutdown
                try {
                    WrappedTask<?> wrappedTask = taskQueue.poll(500, TimeUnit.MILLISECONDS);
                    if (wrappedTask != null) {
                        executeTask(wrappedTask);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        private <T> void executeTask(WrappedTask<T> wrappedTask) {
            Task<T> task = wrappedTask.task;
            CompletableFuture<T> future = wrappedTask.future;

            Object lock = groupLocks.computeIfAbsent(task.taskGroup(), k -> new Object());

            executorService.submit(() -> {
                synchronized (lock) {
                    try {
                        T result = task.taskAction().call();
                        future.complete(result);
                    } catch (Exception e) {
                        future.completeExceptionally(e);
                    }
                }
            });
        }

        @Override
        public void shutdown() {
            isRunning = false;  // Signal worker thread to stop
            try {
                workerThread.join();  // Wait for worker thread to exit
                executorService.shutdown();
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    executorService.shutdownNow(); // Force shutdown if tasks are still running
                }
                System.out.println("TaskExecutorService has shut down.");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        private static class WrappedTask<T> {
            final Task<T> task;
            final CompletableFuture<T> future;

            WrappedTask(Task<T> task, CompletableFuture<T> future) {
                this.task = task;
                this.future = future;
            }
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        TaskExecutorService taskExecutor = new TaskExecutorService(3);

        TaskGroup group1 = new TaskGroup(UUID.randomUUID());

        Task<Integer> task1 = new Task<>(UUID.randomUUID(), group1, TaskType.READ, () -> {
            Thread.sleep(2000);
            return 42;
        });

        Task<Integer> task2 = new Task<>(UUID.randomUUID(), group1, TaskType.READ, () -> {
            Thread.sleep(1000);
            return 99;
        });

        Future<Integer> future1 = taskExecutor.submitTask(task1);
        Future<Integer> future2 = taskExecutor.submitTask(task2);

        System.out.println("Task 1 result: " + future1.get());
        System.out.println("Task 2 result: " + future2.get());

        taskExecutor.shutdown(); // Ensure proper shutdown
    }
}
