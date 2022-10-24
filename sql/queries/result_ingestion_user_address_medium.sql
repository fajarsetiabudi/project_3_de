SELECT COUNT(*), MIN(created_at), MAX(created_at)
FROM user_address_master
WHERE DATE(created_at) BETWEEN '2018-02-01' AND '2018-12-31';