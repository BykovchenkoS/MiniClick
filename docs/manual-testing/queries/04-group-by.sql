SELECT category, count() AS n, sum(amount) AS total FROM manual_events GROUP BY category ORDER BY total DESC, category ASC
