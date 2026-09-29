package com.portfolio.notifyhub.idempotency;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.portfolio.notifyhub.api.CreateNotificationRequest;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.TreeSet;

public final class RequestFingerprint {
    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    private RequestFingerprint() {}

    public static String hash(CreateNotificationRequest request) {
        ObjectNode node = JSON.createObjectNode();
        node.put("userId", request.userId());
        node.put("category", request.category().name());
        node.put("channels", request.channels());
        node.put("templateId", request.templateId());
        node.put("scheduledAt", request.scheduledAt() == null ? null : request.scheduledAt().toString());
        try {
            JsonNode payload = request.payload() == null ? JSON.nullNode() : JSON.readTree(request.payload());
            if (payload == null || payload.isMissingNode()) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "payload must contain valid JSON");
            }
            node.set("payload", canonical(payload));
            byte[] bytes = node.toString().getBytes(StandardCharsets.UTF_8);
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (JsonProcessingException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "payload must contain valid JSON");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private static JsonNode canonical(JsonNode node) {
        if (node.isObject()) {
            ObjectNode sorted = JSON.createObjectNode();
            TreeSet<String> keys = new TreeSet<>();
            node.fieldNames().forEachRemaining(keys::add);
            keys.forEach(key -> sorted.set(key, canonical(node.get(key))));
            return sorted;
        }
        if (node.isArray()) {
            var array = JSON.createArrayNode();
            node.forEach(value -> array.add(canonical(value)));
            return array;
        }
        if (node.isNumber()) return DecimalNode.valueOf(node.decimalValue().stripTrailingZeros());
        return node;
    }
}
