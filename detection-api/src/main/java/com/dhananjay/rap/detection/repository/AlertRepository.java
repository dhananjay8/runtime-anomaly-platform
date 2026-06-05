package com.dhananjay.rap.detection.repository;

import com.dhananjay.rap.common.dto.AlertRecord;
import com.dhananjay.rap.common.dto.PagedResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j
@Repository
@RequiredArgsConstructor
public class AlertRepository {

    private final JdbcTemplate jdbcTemplate;

    private static final RowMapper<AlertRecord> ALERT_ROW_MAPPER = (rs, rowNum) -> {
        AlertRecord alert = new AlertRecord();
        alert.setAlertId(rs.getString("alert_id"));
        alert.setResultId(rs.getString("result_id"));
        alert.setContainerId(rs.getString("container_id"));
        alert.setSeverity(rs.getString("severity"));
        alert.setAnomalyScore(rs.getDouble("anomaly_score"));
        alert.setTitle(rs.getString("title"));
        alert.setDescription(rs.getString("description"));
        Timestamp ts = rs.getTimestamp("created_at");
        if (ts != null) {
            alert.setCreatedAt(ts.toInstant());
            alert.setTriggeredAt(ts.toInstant());
        }
        alert.setAcknowledged(rs.getInt("acknowledged") == 1);
        Timestamp ackTs = rs.getTimestamp("acknowledged_at");
        if (ackTs != null) alert.setAcknowledgedAt(ackTs.toInstant());
        alert.setAcknowledgedBy(rs.getString("acknowledged_by"));
        return alert;
    };

    public void insertAlert(AlertRecord alert) {
        jdbcTemplate.update("""
                INSERT INTO alert_history (
                    alert_id, result_id, container_id, severity, anomaly_score,
                    title, description, acknowledged
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                alert.getAlertId(),
                alert.getResultId(),
                alert.getContainerId(),
                alert.getSeverity(),
                alert.getAnomalyScore(),
                alert.getTitle(),
                alert.getDescription(),
                0
        );
    }

    public PagedResponse<AlertRecord> findAlerts(int page, int size, String severity, Boolean acknowledged) {
        int cappedSize = Math.min(size, 200);

        StringBuilder countSql = new StringBuilder("SELECT COUNT(*) FROM alert_history WHERE 1=1");
        StringBuilder querySql = new StringBuilder(
                "SELECT * FROM (SELECT a.*, ROWNUM rnum FROM (SELECT * FROM alert_history WHERE 1=1");

        List<Object> params = new ArrayList<>();

        if (severity != null && !severity.isBlank()) {
            countSql.append(" AND severity = ?");
            querySql.append(" AND severity = ?");
            params.add(severity.toUpperCase());
        }
        if (acknowledged != null) {
            countSql.append(" AND acknowledged = ?");
            querySql.append(" AND acknowledged = ?");
            params.add(acknowledged ? 1 : 0);
        }

        Long total = jdbcTemplate.queryForObject(countSql.toString(), Long.class, params.toArray());
        long totalElements = total != null ? total : 0;

        querySql.append(" ORDER BY triggered_at DESC) a WHERE ROWNUM <= ?) WHERE rnum > ?");
        List<Object> queryParams = new ArrayList<>(params);
        queryParams.add((page + 1) * cappedSize);
        queryParams.add(page * cappedSize);

        List<AlertRecord> content = jdbcTemplate.query(querySql.toString(), ALERT_ROW_MAPPER, queryParams.toArray());

        int totalPages = (int) Math.ceil((double) totalElements / cappedSize);

        return PagedResponse.<AlertRecord>builder()
                .content(content)
                .page(page)
                .size(cappedSize)
                .totalElements(totalElements)
                .totalPages(totalPages)
                .hasNext(page < totalPages - 1)
                .build();
    }

    public Optional<AlertRecord> findByAlertId(String alertId) {
        String sql = "SELECT * FROM alert_history WHERE alert_id = ?";
        List<AlertRecord> results = jdbcTemplate.query(sql, ALERT_ROW_MAPPER, alertId);
        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }

    public boolean acknowledge(String alertId, String acknowledgedBy) {
        int updated = jdbcTemplate.update("""
                UPDATE alert_history SET acknowledged = 1,
                    acknowledged_at = ?, acknowledged_by = ?
                WHERE alert_id = ? AND acknowledged = 0
                """,
                Timestamp.from(Instant.now()),
                acknowledgedBy,
                alertId
        );
        return updated > 0;
    }
}
