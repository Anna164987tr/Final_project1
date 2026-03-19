SELECT
  *
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`;

WITH RawEvents AS (
    -- Перший крок: Вирівнюємо необхідні параметри з event_params для кожної події
    SELECT
        event_date,
        event_timestamp, 
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
        regexp_extract((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), r'(?:https?://)?([^/]+)(?:/.*)?') AS page_location_host,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS full_page_location, 
        device,
        traffic_source,
        event_name,
        event_params 
    FROM
        `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
    WHERE
        event_date BETWEEN '20201101' AND '20210131' -- Фільтр за діапазоном дат 
        AND (
            event_name = 'session_start' OR
            event_name = 'view_item' OR
            event_name = 'add_to_cart' OR
            event_name = 'begin_checkout' OR
            event_name = 'add_shipping_info' OR
            event_name = 'add_payment_info' OR
            event_name = 'purchase'
        )
),
SessionAggregatedData AS (
    -- Другий крок: Агрегуємо дані на рівні сесії
    SELECT
        event_date,
        user_pseudo_id,
        ga_session_id,
        CONCAT(user_pseudo_id, CAST(ga_session_id AS STRING)) AS session_id_full,
        device,
        traffic_source,
        -- Агрегуємо всі події та їх параметри в масив для подальшої перевірки
        ARRAY_AGG(STRUCT(event_name, event_params)) AS events_in_session,
        ANY_VALUE(CASE WHEN event_name = 'session_start' THEN page_location_host ELSE NULL END) AS landing_page,
        ANY_VALUE(CASE WHEN event_name = 'session_start' THEN full_page_location ELSE NULL END) AS full_landing_page_url -
    FROM
        RawEvents
    GROUP BY
        event_date, user_pseudo_id, ga_session_id, device, traffic_source
)
-- Основний запит: Розраховуємо кінцеві метрики та зрізи
SELECT
    -- 1. Зріз за датою початку сесії
    PARSE_DATE('%Y%m%d', session_data.event_date) AS session_start_date,

    -- 2. Зрізи, пов'язані з джерелом трафіку
    session_data.traffic_source.source AS traffic_source,
    session_data.traffic_source.medium AS traffic_medium,
    session_data.traffic_source.name AS traffic_campaign, 

    -- 3. Зрізи, пов'язані з пристроєм користувача
    session_data.device.category AS device_category,
    session_data.device.operating_system AS device_os,
    session_data.device.language AS device_language,

    -- 4. Зріз за посадковою сторінкою сесії
    session_data.landing_page,
    session_data.full_landing_page_url, -- Додано повний URL посадкової сторінки

    -- 5. Основні метрики відвідувань
    COUNT (session_data.session_id_full) AS total_sessions,
    COUNT (session_data.user_pseudo_id) AS total_users,

    -- 6. Кількість сесій на кожному етапі воронки
    COUNT( CASE WHEN EXISTS(SELECT 1 FROM UNNEST(session_data.events_in_session) WHERE event_name = 'view_item') THEN session_data.session_id_full ELSE NULL END) AS sessions_view_item,
    COUNT( CASE WHEN EXISTS(SELECT 1 FROM UNNEST(session_data.events_in_session) WHERE event_name = 'add_to_cart') THEN session_data.session_id_full ELSE NULL END) AS sessions_add_to_cart,
    COUNT( CASE WHEN EXISTS(SELECT 1 FROM UNNEST(session_data.events_in_session) WHERE event_name = 'begin_checkout') THEN session_data.session_id_full ELSE NULL END) AS sessions_begin_checkout,
    COUNT( CASE WHEN EXISTS(SELECT 1 FROM UNNEST(session_data.events_in_session) WHERE event_name = 'add_shipping_info') THEN session_data.session_id_full ELSE NULL END) AS sessions_add_shipping_info,
    COUNT( CASE WHEN EXISTS(SELECT 1 FROM UNNEST(session_data.events_in_session) WHERE event_name = 'add_payment_info') THEN session_data.session_id_full ELSE NULL END) AS sessions_add_payment_info,
    COUNT( CASE WHEN EXISTS(SELECT 1 FROM UNNEST(session_data.events_in_session) WHERE event_name = 'purchase') THEN session_data.session_id_full ELSE NULL END) AS sessions_purchase,

    -- 7. Загальний дохід від покупок
    -- Використовуємо вкладений UNNEST для доступу до event_params всередині подій сесії
    SUM(
        (SELECT
            ep.value.double_value
        FROM
            UNNEST(session_data.events_in_session) AS s_event,
            UNNEST(s_event.event_params) AS ep 
        WHERE
            s_event.event_name = 'purchase' AND ep.key = 'value'
        LIMIT 1) 
    ) AS total_revenue

FROM
    SessionAggregatedData AS session_data
GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8, 9
ORDER BY
    session_start_date, traffic_source, traffic_medium, traffic_campaign, device_category, device_os, device_language, landing_page;
