package com.firebase.queue;

import java.util.HashMap;

class ValidityChecker {
    private static final HashMap<Long, String> threadIdToTaskIdMap = new HashMap<>();

    private final Long id;

    public ValidityChecker(Thread thread, String taskId) {
        this.id = thread.getId();

        threadIdToTaskIdMap.put(id, taskId);
    }

    public boolean isValid(Thread processingThread, String taskId) {
        return taskId.equals(threadIdToTaskIdMap.get(processingThread.getId()));
    }

    public void destroy() {
        threadIdToTaskIdMap.remove(id);
    }
}
