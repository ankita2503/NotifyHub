package com.portfolio.notifyhub.controller;

import com.portfolio.notifyhub.Exception.ApiExceptionHandler;
import com.portfolio.notifyhub.service.NotificationService;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import com.portfolio.notifyhub.idempotency.IdempotencyConflictException;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class NotificationValidationTests {

    @Test
    void invalidBodyReturnsFieldErrorsWithoutCallingService() throws Exception {
        NotificationService service = mock(NotificationService.class);
        MockMvc mvc = MockMvcBuilders.standaloneSetup(new NotificationController(service))
                .setControllerAdvice(new ApiExceptionHandler())
                .build();

        mvc.perform(post("/v1/notifications")
                        .header("Idempotency-Key", "invalid-request")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"userId":"", "category":null, "channels":" ", "templateId":""}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.code").value("VALIDATION_ERROR"))
                .andExpect(jsonPath("$.message").value("Request validation failed"))
                .andExpect(jsonPath("$.errors.length()").value(4))
                .andExpect(jsonPath("$.errors[*].field", containsInAnyOrder(
                        "userId", "category", "channels", "templateId")));

        verifyNoInteractions(service);
    }
    @Test
    void reusedKeyWithDifferentContentReturns409() throws Exception {
        NotificationService service = mock(NotificationService.class);
        when(service.create(any(), eq("used-key"))).thenThrow(new IdempotencyConflictException());
        MockMvc mvc = MockMvcBuilders.standaloneSetup(new NotificationController(service))
                .setControllerAdvice(new ApiExceptionHandler()).build();
        mvc.perform(post("/v1/notifications").header("Idempotency-Key", "used-key")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"userId":"user", "category":"TRANSACTIONAL", "channels":"EMAIL", "templateId":"welcome"}
                                """))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("IDEMPOTENCY_CONFLICT"));
    }

}
