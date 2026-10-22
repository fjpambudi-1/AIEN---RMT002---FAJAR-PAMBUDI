# Sample Engine Queries

## 1. Net revenue per sales channel
**Question:** What is the total net revenue by sales channel?

**Generated SQL:**
```sql
SELECT sales_channel, SUM(net_revenue) AS net_rev
FROM gold.order_360
GROUP BY sales_channel;
```

**Result (sample):**
| sales_channel | net_rev |
|---------------|---------|
| WEB           | 12500.00 |
| STORE         | 8420.00 |

## 2. Daily customer order count
**Question:** How many orders did each customer place today?

**Generated SQL:**
```sql
SELECT customer_id, COUNT(*) AS orders_today
FROM gold.order_360
WHERE order_date = CURRENT_DATE
GROUP BY customer_id;
```

**Result (sample):**
| customer_id | orders_today |
|-------------|--------------|
| C001        | 3 |
| C017        | 1 |
