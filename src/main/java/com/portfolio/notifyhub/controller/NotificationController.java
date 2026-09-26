package com.portfolio.notifyhub.controller;

import com.portfolio.notifyhub.api.CreateNotificationRequest;
import com.portfolio.notifyhub.api.CreateNotificationResponse;
import com.portfolio.notifyhub.service.NotificationService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/v1/notifications")
public class NotificationController {

    private final NotificationService notificationService;

    public NotificationController(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @PostMapping
    public ResponseEntity<CreateNotificationResponse> create(
            @Valid @RequestBody CreateNotificationRequest request,
            @RequestHeader("Idempotency-Key") String key) {

        CreateNotificationResponse response =
                notificationService.create(request, key);

        if (response.duplicate()) {
            return ResponseEntity.ok(response);       // 200
        }

        return ResponseEntity
                .accepted()
                .body(response);                      // 202
    }
}
