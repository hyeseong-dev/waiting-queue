package com.example.api.controller;

import com.example.api.service.UserQueueService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.reactive.result.view.Rendering;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@Controller
@RequiredArgsConstructor
public class WaitingRoomController {

    private final UserQueueService userQueueService;

    @GetMapping("/waiting-room")
    public Mono<Rendering> waitingRoomPage(
            @RequestParam(name = "queue", defaultValue = "default") String queue,
            @RequestParam(name = "user_id") Long userId,
            @RequestParam(name = "redirect_url") String redirectUrl,
            ServerWebExchange exchange
    ) {
        var key = "user-queue-%s-token".formatted(queue);
        var cookieValue = exchange.getRequest().getCookies().getFirst(key);
        var token = (cookieValue == null) ? "" : cookieValue.getValue();
        // 1. 사용자가 입장이 허용되었는지 확인
        return checkIfUserIsAllowed(queue, userId, token, redirectUrl)
                .switchIfEmpty(registerUserAndShowWaitingPage(queue, userId));
    }

    // 사용자가 입장이 허용되었는지 확인하고, 허용되었다면 리다이렉트
    private Mono<Rendering> checkIfUserIsAllowed(String queue, Long userId, String token, String redirectUrl) {
        return userQueueService.isAllowedByToken(queue, userId, token)
                .filter(Boolean::booleanValue) // true인 경우만 처리
                .flatMap(allowed -> Mono.just(Rendering.redirectTo(redirectUrl).build()));
    }

    // 사용자가 허용되지 않았을 때, 대기열에 등록하고 대기 페이지를 보여줌
    private Mono<Rendering> registerUserAndShowWaitingPage(String queue, Long userId) {
        return userQueueService.registerWaitngQueue(queue, userId)
                .onErrorResume(ex -> userQueueService.getRank(queue, userId)) // 등록 실패 시 순번 조회
                .map(rank -> Rendering.view("waiting-room.html")
                        .modelAttribute("number", rank)
                        .modelAttribute("userId", userId)
                        .modelAttribute("queue", queue)
                        .build());
    }

}
