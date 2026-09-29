package com.portfolio.notifyhub.messaging;

import com.portfolio.notifyhub.domain.Category;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class NotificationClaims {
    private final NamedParameterJdbcTemplate jdbc;

    public NotificationClaims(JdbcTemplate jdbc) {
        this.jdbc = new NamedParameterJdbcTemplate(jdbc);
    }

    public List<NotificationRequestedEvent> claim(int batchSize, UUID token, long leaseMs) {
        return jdbc.query("""
                WITH candidates AS (
                    SELECT id FROM notification_request
                    WHERE status = 'ACCEPTED'
                      AND (publisher_claim_until IS NULL OR publisher_claim_until <= clock_timestamp())
                    ORDER BY created_at, id
                    LIMIT :batchSize
                    FOR UPDATE SKIP LOCKED
                ), claimed AS (
                    UPDATE notification_request n
                    SET publisher_claim_token = :token,
                        publisher_claim_until = clock_timestamp() + :leaseMs * INTERVAL '1 millisecond'
                    FROM candidates c WHERE n.id = c.id
                    RETURNING n.*
                )
                SELECT * FROM claimed ORDER BY created_at, id
                """, new MapSqlParameterSource("batchSize", batchSize)
                .addValue("token", token).addValue("leaseMs", leaseMs), (rs, row) -> {
            var scheduled = rs.getTimestamp("scheduled_at");
            return new NotificationRequestedEvent(rs.getObject("id", UUID.class), rs.getString("user_id"),
                    Category.valueOf(rs.getString("category")), rs.getString("channels"),
                    rs.getString("template_id"), rs.getString("payload"),
                    scheduled == null ? null : scheduled.toInstant());
        });
    }

    public List<UUID> renew(UUID token, List<UUID> ids, long leaseMs) {
        if (ids.isEmpty()) return List.of();
        return jdbc.query("""
                UPDATE notification_request
                SET publisher_claim_until = clock_timestamp() + :leaseMs * INTERVAL '1 millisecond'
                WHERE id IN (:ids) AND publisher_claim_token = :token
                  AND status = 'ACCEPTED' AND publisher_claim_until > clock_timestamp()
                RETURNING id
                """, parameters(token, ids).addValue("leaseMs", leaseMs),
                (rs, row) -> rs.getObject("id", UUID.class));
    }

    public int published(UUID token, List<UUID> ids) {
        if (ids.isEmpty()) return 0;
        return jdbc.update("""
                UPDATE notification_request
                SET status = 'PUBLISHED', publisher_claim_token = NULL, publisher_claim_until = NULL
                WHERE id IN (:ids) AND publisher_claim_token = :token
                  AND status = 'ACCEPTED' AND publisher_claim_until > clock_timestamp()
                """, parameters(token, ids));
    }

    public int release(UUID token, List<UUID> ids) {
        if (ids.isEmpty()) return 0;
        return jdbc.update("""
                UPDATE notification_request
                SET publisher_claim_token = NULL, publisher_claim_until = NULL
                WHERE id IN (:ids) AND publisher_claim_token = :token
                  AND status = 'ACCEPTED' AND publisher_claim_until > clock_timestamp()
                """, parameters(token, ids));
    }

    private MapSqlParameterSource parameters(UUID token, List<UUID> ids) {
        return new MapSqlParameterSource("token", token).addValue("ids", ids);
    }
}
