package com.dhananjay.rap.detection.repository;

import com.dhananjay.rap.common.dto.ContainerProfile;
import com.dhananjay.rap.common.dto.PagedResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.util.*;

@Slf4j
@Repository
@RequiredArgsConstructor
public class ContainerProfileRepository {

    private final JdbcTemplate jdbcTemplate;

    private static final RowMapper<ContainerProfile> PROFILE_ROW_MAPPER = (rs, rowNum) -> {
        ContainerProfile profile = new ContainerProfile();
        profile.setContainerId(rs.getString("container_id"));
        profile.setContainerName(rs.getString("container_name"));
        profile.setImageName(rs.getString("image_name"));
        profile.setNamespace(rs.getString("namespace"));
        Timestamp fs = rs.getTimestamp("first_seen");
        if (fs != null) profile.setFirstSeen(fs.toInstant());
        Timestamp ls = rs.getTimestamp("last_seen");
        if (ls != null) profile.setLastSeen(ls.toInstant());
        profile.setTotalEvents(rs.getLong("total_events"));
        profile.setTotalAnomalies(rs.getLong("total_anomalies"));
        profile.setAnomalyRate(rs.getDouble("anomaly_rate"));
        profile.setAvgAnomalyScore(rs.getDouble("avg_anomaly_score"));
        profile.setMaxAnomalyScore(rs.getDouble("max_anomaly_score"));
        profile.setRiskLevel(rs.getString("risk_level"));
        String processList = rs.getString("baseline_process_list");
        if (processList != null) {
            profile.setBaselineProcessList(processList.split(","));
        }
        String networkDests = rs.getString("baseline_network_destinations");
        if (networkDests != null) {
            profile.setBaselineNetworkDestinations(networkDests.split(","));
        }
        return profile;
    };

    public PagedResponse<ContainerProfile> findContainers(int page, int size, String riskLevel) {
        int cappedSize = Math.min(size, 200);
        StringBuilder countSql = new StringBuilder("SELECT COUNT(*) FROM container_profiles WHERE 1=1");
        StringBuilder querySql = new StringBuilder(
                "SELECT * FROM (SELECT cp.*, ROWNUM rnum FROM (SELECT * FROM container_profiles WHERE 1=1");

        List<Object> params = new ArrayList<>();

        if (riskLevel != null && !riskLevel.isBlank()) {
            countSql.append(" AND risk_level = ?");
            querySql.append(" AND risk_level = ?");
            params.add(riskLevel.toUpperCase());
        }

        Long total = jdbcTemplate.queryForObject(countSql.toString(), Long.class, params.toArray());
        long totalElements = total != null ? total : 0;

        querySql.append(" ORDER BY last_seen DESC) cp WHERE ROWNUM <= ?) WHERE rnum > ?");
        List<Object> queryParams = new ArrayList<>(params);
        queryParams.add((page + 1) * cappedSize);
        queryParams.add(page * cappedSize);

        List<ContainerProfile> content = jdbcTemplate.query(querySql.toString(), PROFILE_ROW_MAPPER, queryParams.toArray());

        int totalPages = (int) Math.ceil((double) totalElements / cappedSize);

        return PagedResponse.<ContainerProfile>builder()
                .content(content)
                .page(page)
                .size(cappedSize)
                .totalElements(totalElements)
                .totalPages(totalPages)
                .hasNext(page < totalPages - 1)
                .build();
    }

    public Optional<ContainerProfile> findByContainerId(String containerId) {
        String sql = "SELECT * FROM container_profiles WHERE container_id = ?";
        List<ContainerProfile> results = jdbcTemplate.query(sql, PROFILE_ROW_MAPPER, containerId);
        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }

    public void upsertProfile(ContainerProfile profile) {
        jdbcTemplate.update("""
                MERGE INTO container_profiles cp
                USING (SELECT ? AS container_id FROM DUAL) src
                ON (cp.container_id = src.container_id)
                WHEN MATCHED THEN UPDATE SET
                    last_seen = ?, total_events = ?, total_anomalies = ?,
                    anomaly_rate = ?, avg_anomaly_score = ?, max_anomaly_score = ?,
                    risk_level = ?
                WHEN NOT MATCHED THEN INSERT (
                    container_id, container_name, image_name, namespace,
                    first_seen, last_seen, total_events, total_anomalies,
                    anomaly_rate, avg_anomaly_score, max_anomaly_score, risk_level
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                profile.getContainerId(),
                // UPDATE params
                Timestamp.from(profile.getLastSeen()),
                profile.getTotalEvents(),
                profile.getTotalAnomalies(),
                profile.getAnomalyRate(),
                profile.getAvgAnomalyScore(),
                profile.getMaxAnomalyScore(),
                profile.getRiskLevel(),
                // INSERT params
                profile.getContainerId(),
                profile.getContainerName(),
                profile.getImageName(),
                profile.getNamespace(),
                Timestamp.from(profile.getFirstSeen()),
                Timestamp.from(profile.getLastSeen()),
                profile.getTotalEvents(),
                profile.getTotalAnomalies(),
                profile.getAnomalyRate(),
                profile.getAvgAnomalyScore(),
                profile.getMaxAnomalyScore(),
                profile.getRiskLevel()
        );
    }
}
