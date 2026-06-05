package com.dhananjay.rap.detection.repository;

import com.dhananjay.rap.common.dto.AnomalyResponse;
import com.dhananjay.rap.common.dto.PagedResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;

@Slf4j
@Repository
@RequiredArgsConstructor
public class AnomalyRepository {

    private final JdbcTemplate jdbcTemplate;

    private static final RowMapper<AnomalyResponse> ANOMALY_ROW_MAPPER = (rs, rowNum) -> {
        AnomalyResponse response = new AnomalyResponse();
        response.setResultId(rs.getString("result_id"));
        response.setContainerId(rs.getString("container_id"));
        response.setAnomalyScore(rs.getDouble("anomaly_score"));
        response.setIsAnomalous(rs.getBoolean("is_anomalous"));
        response.setSeverity(rs.getString("severity"));
        response.setDescription(rs.getString("description"));
        response.setDetectedAt(rs.getTimestamp("detected_at").toInstant());
        Timestamp ws = rs.getTimestamp("window_start");
        if (ws != null) response.setWindowStart(ws.toInstant());
        Timestamp we = rs.getTimestamp("window_end");
        if (we != null) response.setWindowEnd(we.toInstant());
        response.setModelVersion(rs.getString("model_version"));
        String features = rs.getString("contributing_features");
        if (features != null) {
            response.setContributingFeatures(features.split(","));
        }
        return response;
    };

    public PagedResponse<AnomalyResponse> findAnomalies(int page, int size, String severity,
                                                          String containerId, Instant from, Instant to) {
        int cappedSize = Math.min(size, 200);
        StringBuilder countSql = new StringBuilder("SELECT COUNT(*) FROM anomaly_results WHERE detected_at BETWEEN ? AND ?");
        StringBuilder querySql = new StringBuilder(
                "SELECT * FROM (SELECT a.*, ROWNUM rnum FROM (SELECT * FROM anomaly_results WHERE detected_at BETWEEN ? AND ?");

        List<Object> params = new ArrayList<>();
        params.add(Timestamp.from(from));
        params.add(Timestamp.from(to));

        if (severity != null && !severity.isBlank()) {
            countSql.append(" AND severity = ?");
            querySql.append(" AND severity = ?");
            params.add(severity.toUpperCase());
        }
        if (containerId != null && !containerId.isBlank()) {
            countSql.append(" AND container_id = ?");
            querySql.append(" AND container_id = ?");
            params.add(containerId);
        }

        Long total = jdbcTemplate.queryForObject(countSql.toString(), Long.class, params.toArray());
        long totalElements = total != null ? total : 0;

        querySql.append(" ORDER BY detected_at DESC) a WHERE ROWNUM <= ?) WHERE rnum > ?");
        List<Object> queryParams = new ArrayList<>(params);
        int endRow = (page + 1) * cappedSize;
        int startRow = page * cappedSize;
        queryParams.add(endRow);
        queryParams.add(startRow);

        List<AnomalyResponse> content = jdbcTemplate.query(querySql.toString(), ANOMALY_ROW_MAPPER, queryParams.toArray());

        int totalPages = (int) Math.ceil((double) totalElements / cappedSize);

        return PagedResponse.<AnomalyResponse>builder()
                .content(content)
                .page(page)
                .size(cappedSize)
                .totalElements(totalElements)
                .totalPages(totalPages)
                .hasNext(page < totalPages - 1)
                .build();
    }

    public Optional<AnomalyResponse> findById(String resultId) {
        String sql = "SELECT * FROM anomaly_results WHERE result_id = ?";
        List<AnomalyResponse> results = jdbcTemplate.query(sql, ANOMALY_ROW_MAPPER, resultId);
        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }

    public Map<String, Object> getStats(Instant since) {
        Map<String, Object> stats = new LinkedHashMap<>();

        Long totalAnomalies = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM anomaly_results WHERE is_anomalous = 1 AND detected_at > ?",
                Long.class, Timestamp.from(since));
        stats.put("total_anomalies", totalAnomalies != null ? totalAnomalies : 0);

        Long totalEvents = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM anomaly_results WHERE detected_at > ?",
                Long.class, Timestamp.from(since));
        stats.put("total_scored", totalEvents != null ? totalEvents : 0);

        Double avgScore = jdbcTemplate.queryForObject(
                "SELECT AVG(anomaly_score) FROM anomaly_results WHERE detected_at > ?",
                Double.class, Timestamp.from(since));
        stats.put("avg_anomaly_score", avgScore != null ? avgScore : 0.0);

        Long criticalCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM anomaly_results WHERE severity = 'CRITICAL' AND detected_at > ?",
                Long.class, Timestamp.from(since));
        stats.put("critical_count", criticalCount != null ? criticalCount : 0);

        Long highCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM anomaly_results WHERE severity = 'HIGH' AND detected_at > ?",
                Long.class, Timestamp.from(since));
        stats.put("high_count", highCount != null ? highCount : 0);

        Long uniqueContainers = jdbcTemplate.queryForObject(
                "SELECT COUNT(DISTINCT container_id) FROM anomaly_results WHERE is_anomalous = 1 AND detected_at > ?",
                Long.class, Timestamp.from(since));
        stats.put("affected_containers", uniqueContainers != null ? uniqueContainers : 0);

        stats.put("since", since.toString());
        stats.put("generated_at", Instant.now().toString());

        return stats;
    }

    public List<Map<String, Object>> getAnomalyTrend(Instant from, Instant to, int bucketMinutes) {
        String sql = """
                SELECT TRUNC(detected_at, 'MI') - MOD(EXTRACT(MINUTE FROM detected_at), ?) * INTERVAL '1' MINUTE AS bucket,
                       COUNT(*) AS total,
                       SUM(CASE WHEN is_anomalous = 1 THEN 1 ELSE 0 END) AS anomalies,
                       AVG(anomaly_score) AS avg_score
                FROM anomaly_results
                WHERE detected_at BETWEEN ? AND ?
                GROUP BY TRUNC(detected_at, 'MI') - MOD(EXTRACT(MINUTE FROM detected_at), ?) * INTERVAL '1' MINUTE
                ORDER BY bucket ASC
                """;
        return jdbcTemplate.queryForList(sql, bucketMinutes, Timestamp.from(from), Timestamp.from(to), bucketMinutes);
    }

    public List<Map<String, Object>> getSeverityOverTime(Instant from, Instant to, int bucketMinutes) {
        String sql = """
                SELECT TRUNC(detected_at, 'MI') - MOD(EXTRACT(MINUTE FROM detected_at), ?) * INTERVAL '1' MINUTE AS bucket,
                       severity,
                       COUNT(*) AS count
                FROM anomaly_results
                WHERE detected_at BETWEEN ? AND ? AND is_anomalous = 1
                GROUP BY TRUNC(detected_at, 'MI') - MOD(EXTRACT(MINUTE FROM detected_at), ?) * INTERVAL '1' MINUTE, severity
                ORDER BY bucket ASC, severity
                """;
        return jdbcTemplate.queryForList(sql, bucketMinutes, Timestamp.from(from), Timestamp.from(to), bucketMinutes);
    }

    public List<Map<String, Object>> getTopRiskyContainers(Instant since, int limit) {
        int cappedLimit = Math.min(limit, 50);
        String sql = """
                SELECT container_id, COUNT(*) AS anomaly_count,
                       MAX(anomaly_score) AS max_score, AVG(anomaly_score) AS avg_score,
                       MAX(severity) AS worst_severity
                FROM anomaly_results
                WHERE is_anomalous = 1 AND detected_at > ?
                GROUP BY container_id
                ORDER BY anomaly_count DESC, max_score DESC
                FETCH FIRST ? ROWS ONLY
                """;
        return jdbcTemplate.queryForList(sql, Timestamp.from(since), cappedLimit);
    }

    public void insertAnomalyResult(String resultId, String vectorId, String containerId,
                                     double anomalyScore, boolean isAnomalous, String severity,
                                     String modelVersion, String contributingFeatures,
                                     String description, Instant windowStart, Instant windowEnd) {
        jdbcTemplate.update("""
                INSERT INTO anomaly_results (
                    result_id, vector_id, container_id, anomaly_score, is_anomalous,
                    severity, model_version, contributing_features, description,
                    detected_at, window_start, window_end
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                resultId, vectorId, containerId, anomalyScore, isAnomalous ? 1 : 0,
                severity, modelVersion, contributingFeatures, description,
                Timestamp.from(Instant.now()),
                windowStart != null ? Timestamp.from(windowStart) : null,
                windowEnd != null ? Timestamp.from(windowEnd) : null
        );
    }
}
