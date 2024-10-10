package com.example.api.service;

import com.example.api.EmbeddedRedis;
import com.example.api.exception.ApplicationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.test.context.ActiveProfiles;
import reactor.test.StepVerifier;

@SpringBootTest
@Import(EmbeddedRedis.class)
@ActiveProfiles("test")
class UserQueueServiceTest {

    @Autowired
    private UserQueueService userQueueService;

    @Autowired
    private ReactiveRedisTemplate<String, String> reactiveRedisTemplate;

    @BeforeEach
    public void beforeEach() {
        var con = reactiveRedisTemplate.getConnectionFactory().getReactiveConnection();
        con.serverCommands().flushAll().subscribe();
    }

    @Test
    void registerWaitngQueue() {
        StepVerifier.create(userQueueService.registerWaitngQueue("default", 100L))
                .expectNext(1L)
                .verifyComplete();
        StepVerifier.create(userQueueService.registerWaitngQueue("default", 101L))
                .expectNext(2L)
                .verifyComplete();
        StepVerifier.create(userQueueService.registerWaitngQueue("default", 102L))
                .expectNext(3L)
                .verifyComplete();
    }

    @Test
    void alreadyRegisterWatingQueue() {
        StepVerifier.create(userQueueService.registerWaitngQueue("default", 100L))
                .expectNext(1L)
                .verifyComplete();
        StepVerifier.create(userQueueService.registerWaitngQueue("default", 100L))
                .expectError(ApplicationException.class)
                .verify();

    }

    @Test
    void emptyAllowUser() {
        StepVerifier.create(userQueueService.allowUser("default", 3L))
                .expectNext(0L)
                .verifyComplete();
    }
    @Test
    void allowUser() {
        StepVerifier.create(userQueueService.registerWaitngQueue("default", 1L)
                    .then(userQueueService.registerWaitngQueue("default", 2L))
                    .then(userQueueService.registerWaitngQueue("default", 3L))
                    .then(userQueueService.allowUser("default", 3L)))
                .expectNext(3L)
                .verifyComplete();
    }

    @Test
    void allowUser2() {
        StepVerifier.create(userQueueService.registerWaitngQueue("default", 1L)
                        .then(userQueueService.registerWaitngQueue("default", 2L))
                        .then(userQueueService.registerWaitngQueue("default", 3L))
                        .then(userQueueService.allowUser("default", 100L)))
                .expectNext(3L)
                .verifyComplete();
    }

    @Test
    void allUserAfterRegisterWaitingQueue() {
        StepVerifier.create(userQueueService.registerWaitngQueue("default", 1L)
                        .then(userQueueService.registerWaitngQueue("default", 2L))
                        .then(userQueueService.registerWaitngQueue("default", 3L))
                        .then(userQueueService.allowUser("default", 3L))
                        .then(userQueueService.registerWaitngQueue("default", 4L))
                )
                .expectNext(1L)
                .verifyComplete();
    }

    @Test
    void isNotAllowed() {
        StepVerifier.create(userQueueService.isAllowed("default", 1L))
                .expectNext(false)
                .verifyComplete();
    }

    @Test
    void isNotAllowed2() {
        StepVerifier.create(userQueueService.registerWaitngQueue("default", 100L)
                        .then(userQueueService.allowUser("default", 3L))
                        .then(userQueueService.isAllowed("default", 4L))
                )
                .expectNext(false)
                .verifyComplete();
    }

    @Test
    void isAllowed() {
        StepVerifier.create(userQueueService.registerWaitngQueue("default", 100L)
                        .then(userQueueService.allowUser("default", 3L))
                        .then(userQueueService.isAllowed("default", 100L))
                )
                .expectNext(true)
                .verifyComplete();
    }

    @Test
    void getRank() {
        StepVerifier.create(
                userQueueService.registerWaitngQueue("default", 100L)
                        .then(userQueueService.getRank("default", 100L)))
                .expectNext(1L)
                .verifyComplete();

        StepVerifier.create(
                        userQueueService.registerWaitngQueue("default", 101L)
                                .then(userQueueService.getRank("default", 101L)))
                .expectNext(2L)
                .verifyComplete();

    }

    @Test
    void emptyRank() {
        StepVerifier.create(userQueueService.getRank("default", 100L))
                .expectNext(-1L)
                .verifyComplete();

    }

    @Test
    void isNotAllowedByToken() {
        StepVerifier.create(userQueueService.isAllowedByToken("default", 100L, ""))
                .expectNext(false)
                .verifyComplete();
    }

    @Test
    void isAllowedByToken() {
        StepVerifier.create(userQueueService.isAllowedByToken("default", 100L, "d333a5d4eb24f3f5cdd767d79b8c01aad3cd73d3537c70dec430455d37afe4b8"))
                .expectNext(true)
                .verifyComplete();
    }

    @Test
    void generateToken() {
        StepVerifier.create(userQueueService.generateToken("default", 100L))
                .expectNext("d333a5d4eb24f3f5cdd767d79b8c01aad3cd73d3537c70dec430455d37afe4b8")
                .verifyComplete();
    }
}