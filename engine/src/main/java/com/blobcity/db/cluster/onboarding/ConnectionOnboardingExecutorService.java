package com.blobcity.db.cluster.onboarding;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConnectionOnboardingExecutorService {
    private static ConnectionOnboardingExecutorService ourInstance = new ConnectionOnboardingExecutorService();

    public static ConnectionOnboardingExecutorService getInstance() {
        return ourInstance;
    }

    private final ExecutorService executorService;

    private ConnectionOnboardingExecutorService() {
        executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }

    public void submit(OnboardingHandler onboardingHandler) {
        executorService.submit(onboardingHandler);
    }
}
