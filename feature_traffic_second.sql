--- feature_traffic_second
--- source: witcher_fin.util_number_with_uid, witcher_fin.util_format_number_with_hash,
---         witcher_fin.util_distinct_calllog

WITH uid_latest AS (
    SELECT
        num,
        MAX(utime) AS utime
    FROM witcher_fin.util_number_with_uid
    WHERE region = 'TW' AND uid <> '' AND num <> ''
    GROUP BY num
),

uid_data AS (
    SELECT
        num_uid.num AS num_886,
        num_uid.uid AS uid
    FROM witcher_fin.util_number_with_uid num_uid
    JOIN uid_latest ON uid_latest.num = num_uid.num
    AND uid_latest.utime = num_uid.utime
    WHERE num_uid.region = 'TW' AND num_uid.uid <> '' AND num_uid.num <> ''
),

num_data AS (
    SELECT DISTINCT
        format_num.num_886 AS num_886,
        format_num.num_e164 AS num_e164,
        numowner.uid AS uid
    FROM witcher_fin.util_format_number_with_hash format_num
    LEFT JOIN uid_data numowner ON numowner.num_886 = format_num.num_886
    WHERE format_num.num_886 IS NOT NULL AND format_num.num_886 <> ''
    AND format_num.region = 'TW'
),

-- 每秒接到的電話數
call_count_by_second AS (
    SELECT
        num_886,
        ts,
        COUNT(*) AS total
    FROM (
        -- 從 remote_num 看
        SELECT DISTINCT
            calllog.did,
            num_data.num_886 AS num_886,
            timestamp 'epoch' + CAST(calllog.remote_call_time AS BIGINT)/1000 * interval '1 second'  AS remote_call_time,
            DATE_TRUNC('second', timestamp 'epoch' + CAST(calllog.remote_call_time AS BIGINT)/1000 * interval '1 second' ) AS ts
        FROM witcher_fin.util_distinct_calllog calllog
        JOIN num_data ON num_data.num_886 = calllog.remote_e164
        WHERE calllog.remote_call_type IN ('in', 'missed')
        AND DATE(calllog.dt) BETWEEN DATE_ADD('day', -1, DATE_ADD('month', -2, DATE('2019-04-05'))) AND DATE('2019-04-05')
            AND DATE(timestamp 'epoch' + CAST(calllog.remote_call_time AS BIGINT)/1000 * interval '1 second' ) BETWEEN
                DATE(DATE_ADD('day', -1, DATE_ADD('month', -2, to_date('2019-04-05', 'YYYY-MM-DD') )))
                AND DATE(DATE_ADD('day', -1, to_date('2019-04-05', 'YYYY-MM-DD') ))

        UNION ALL
        -- 從 uid 看
        SELECT DISTINCT
            calllog.did,
            num_data.num_886 AS num_886,
            timestamp 'epoch' + CAST(calllog.remote_call_time AS BIGINT)/1000 * interval '1 second'  AS remote_call_time,
            DATE_TRUNC('second', timestamp 'epoch' + CAST(calllog.remote_call_time AS BIGINT)/1000 * interval '1 second' ) AS ts
        FROM witcher_fin.util_distinct_calllog calllog
        JOIN num_data ON num_data.uid = calllog.uid
        WHERE calllog.remote_call_type IN ('out')
        AND DATE(calllog.dt) BETWEEN DATE_ADD('day', -1, DATE_ADD('month', -2, DATE('2019-04-05'))) AND DATE('2019-04-05')
            AND DATE(timestamp 'epoch' + CAST(calllog.remote_call_time AS BIGINT)/1000 * interval '1 second' ) BETWEEN
                DATE(DATE_ADD('day', -1, DATE_ADD('month', -2, to_date('2019-04-05', 'YYYY-MM-DD') )))
                AND DATE(DATE_ADD('day', -1, to_date('2019-04-05', 'YYYY-MM-DD') ))
    ) AS call_by_second
    GROUP BY num_886, ts
)

SELECT
    num_886,
    CASE WHEN SUM(
    CASE WHEN call_count_by_second.total >= 20 THEN 1
         ELSE 0
    END
    ) > 0 THEN 1
    ELSE NULL
    END AS has_traffic_pattern_second
FROM call_count_by_second
GROUP BY num_886
