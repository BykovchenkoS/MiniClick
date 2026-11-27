SELECT category, count() AS n, sum(amount) AS total FROM manual_events WHERE event_date = '2026-10-01' GROUP BY category ORDER BY total DESC, category ASC LIMIT 2
