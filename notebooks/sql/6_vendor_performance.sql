-- Vendor performance scorecard
WITH vendor_perf AS (
  SELECT 
      dv.vendor_key,
      dv.vendor_id,
      dv.vendor_name,
      dv.vendor_category,
      dv.vendor_tier,
      dv.country_code AS vendor_country,

      -- Volume / value
      COUNT(DISTINCT fol.order_id)              AS total_orders,
      COUNT(*)                                  AS total_line_items,
      SUM(fol.quantity)                         AS total_quantity,
      SUM(fol.line_total_aud)                   AS total_spend_aud,

      -- Value stats
      AVG(fol.unit_price)                       AS avg_unit_price,
      AVG(fol.line_total_aud)                   AS avg_line_value,
      MIN(fol.unit_price)                       AS min_unit_price,
      MAX(fol.unit_price)                       AS max_unit_price,

      -- Diversity / reach
      COUNT(DISTINCT dp.sku_id)                 AS unique_products_supplied,
      COUNT(DISTINCT dp.product_category)       AS product_categories_count,
      COUNT(DISTINCT dc.cost_centre_id)         AS cost_centres_served
  FROM dev_gold.fct_order_line fol
  JOIN dev_gold.dim_vendor dv       ON fol.vendor_key = dv.vendor_key
  JOIN dev_gold.dim_product dp      ON fol.product_key = dp.product_key
  JOIN dev_gold.dim_cost_centre dc  ON fol.cost_centre_key = dc.cost_centre_key
  JOIN dev_gold.dim_date dd         ON fol.date_key = dd.date_key
  WHERE dd.date_actual >= CURRENT_DATE() - INTERVAL 12 MONTH
  GROUP BY 1,2,3,4,5,6
),
exceptions AS (
  SELECT 
      dq.vendor_key,
      COUNT(*) AS exception_count
  FROM dev_gold.fct_data_quality dq
  JOIN dev_gold.dim_date dd ON dq.date_key = dd.date_key
  WHERE dd.date_actual >= CURRENT_DATE() - INTERVAL 12 MONTH   -- align window
  GROUP BY 1
),
vendor_rankings AS (
  SELECT 
      vp.*,
      COALESCE(ex.exception_count, 0) AS data_quality_issues,

      ROW_NUMBER() OVER (ORDER BY total_spend_aud DESC)                                 AS spend_rank,
      ROW_NUMBER() OVER (ORDER BY total_orders DESC)                                     AS volume_rank,
      ROW_NUMBER() OVER (ORDER BY unique_products_supplied DESC)                         AS diversity_rank,
      ROW_NUMBER() OVER (ORDER BY COALESCE(ex.exception_count,0) ASC, total_spend_aud DESC) AS quality_rank,

      ROUND((total_spend_aud / SUM(total_spend_aud) OVER ()) * 100, 2)                   AS spend_concentration_percent
  FROM vendor_perf vp
  LEFT JOIN exceptions ex ON vp.vendor_key = ex.vendor_key
)
SELECT 
    vendor_name,
    vendor_category,
    vendor_tier,
    vendor_country,

    total_spend_aud,
    total_orders,
    unique_products_supplied,
    cost_centres_served,
    data_quality_issues,

    spend_rank,
    volume_rank,
    diversity_rank,
    quality_rank,
    spend_concentration_percent,

    ROUND(avg_line_value, 2)                                   AS avg_line_value_aud,
    ROUND(total_spend_aud / NULLIF(total_orders, 0), 2)        AS avg_order_value_aud,

    CASE 
      WHEN spend_rank <= 2 AND quality_rank <= 3 THEN 'Strategic Partner'
      WHEN spend_rank <= 3 AND diversity_rank <= 2 THEN 'Key Supplier'
      WHEN total_orders >= 50 AND data_quality_issues <= 5 THEN 'Reliable Supplier'
      WHEN spend_concentration_percent < 1 AND total_orders < 10 THEN 'Consolidation Candidate'
      ELSE 'Standard Supplier'
    END AS vendor_classification
FROM vendor_rankings
ORDER BY spend_rank;
