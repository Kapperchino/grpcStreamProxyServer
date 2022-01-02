package io.helidon.examples.quickstart;

import lombok.NoArgsConstructor;

import javax.annotation.PostConstruct;
import javax.inject.Singleton;
import javax.ws.rs.Path;
import java.util.concurrent.ScheduledThreadPoolExecutor;

@Path("/resource")
@Singleton
public class SchedulerWrapper {
    private static final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(12);
    public static ScheduledThreadPoolExecutor getScheduler(){
        return scheduler;
    }
}
