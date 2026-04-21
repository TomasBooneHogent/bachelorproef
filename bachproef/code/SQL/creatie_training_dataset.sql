CREATE OR REPLACE TABLE `corpscore-be.staging.ml_training_data_3yr_window`
AS
WITH
  base_data AS (
    SELECT * FROM `corpscore-be.staging.master_table_all_years_2`
    WHERE company_segment IN ('Micro', 'SME', 'Large')
  ),

  rolling_window AS (
    SELECT
      -- TARGET VARIABLES: YEAR 3
      accounting_year AS target_year_y3,
      enterprise_number,
      company_segment,
      nace_primary_code,

      -- Targets
      net_profit_n AS target_net_profit_y3,
      roe_n AS target_roe_y3,
      roa_n AS target_roa_y3,
      solvency_pct_n AS target_solvency_y3,
      current_ratio_custom_n AS target_liquidity_y3,

      -- FEATURE VARIABLES: YEAR 2 (Last year)
      LAG(accounting_year, 1) OVER(w) AS year_y2,

      -- Ratios Y2 (Profitability)
      LAG(roa_n, 1) OVER(w) AS roa_y2,
      LAG(roe_n, 1) OVER(w) AS roe_y2,
      LAG(current_ccf_revenue_n, 1) OVER(w) AS ccf_revenue_y2,

      -- Ratios Y2 (Solvency & Structure)
      LAG(solvency_pct_n, 1) OVER(w) AS solvency_y2,
      LAG(debt_to_equity_n, 1) OVER(w) AS debt_to_equity_y2,

      -- Ratios Y2 (Liquidity & Working Capital)
      LAG(current_ratio_custom_n, 1) OVER(w) AS liquidity_custom_y2,
      LAG(liquidity_ratio_n, 1) OVER(w) AS liquidity_standard_y2,
      LAG(inventory_turnover_days_n, 1) OVER(w) AS inventory_days_y2,
      LAG(customer_credit_days_n, 1) OVER(w) AS customer_days_y2,
      LAG(supplier_credit_days_n, 1) OVER(w) AS supplier_days_y2,

      -- Absolute values Y2 (For calculating relative growth)
      LAG(revenue_n, 1) OVER(w) AS revenue_y2,
      LAG(total_assets_n, 1) OVER(w) AS assets_y2,
      LAG(equity_n, 1) OVER(w) AS equity_y2,
      LAG(current_cashflow_pre_tax_n, 1) OVER(w) AS cashflow_y2,
      LAG(ebit_n, 1) OVER(w) AS ebit_y2, -- Miste nog _n
      LAG(net_profit_n, 1) OVER(w) AS net_profit_y2,
      LAG(personnel_fte_n, 1) OVER(w) AS fte_y2, -- Miste nog _n

      -- FEATURE VARIABLES: YEAR 1 (Two years ago)
      LAG(accounting_year, 2) OVER(w) AS year_y1,

      -- Ratios Y1 (Profitability)
      LAG(roa_n, 2) OVER(w) AS roa_y1,
      LAG(roe_n, 2) OVER(w) AS roe_y1,
      LAG(current_ccf_revenue_n, 2) OVER(w) AS ccf_revenue_y1,

      -- Ratios Y1 (Solvency & Structure)
      LAG(solvency_pct_n, 2) OVER(w) AS solvency_y1,
      LAG(debt_to_equity_n, 2) OVER(w) AS debt_to_equity_y1,

      -- Ratios Y1 (Liquidity & Working Capital)
      LAG(current_ratio_custom_n, 2) OVER(w) AS liquidity_custom_y1,
      LAG(liquidity_ratio_n, 2) OVER(w) AS liquidity_standard_y1,
      LAG(inventory_turnover_days_n, 2) OVER(w) AS inventory_days_y1,
      LAG(customer_credit_days_n, 2) OVER(w) AS customer_days_y1,
      LAG(supplier_credit_days_n, 2) OVER(w) AS supplier_days_y1,

      -- Absolute values Y1 (For calculating relative growth)
      LAG(revenue_n, 2) OVER(w) AS revenue_y1,
      LAG(total_assets_n, 2) OVER(w) AS assets_y1,
      LAG(equity_n, 2) OVER(w) AS equity_y1,
      LAG(current_cashflow_pre_tax_n, 2) OVER(w) AS cashflow_y1,
      LAG(ebit_n, 2) OVER(w) AS ebit_y1,
      LAG(net_profit_n, 2) OVER(w) AS net_profit_y1,
      LAG(personnel_fte_n, 2) OVER(w) AS fte_y1

    FROM base_data
    WINDOW w AS (PARTITION BY enterprise_number ORDER BY accounting_year)
  ),

  ml_features AS (
    SELECT
      *,
      -- TRENDS: ABSOLUTE DIFFERENCES (Y2 - Y1)
      -- Use for ratios and percentages
      (roa_y2 - roa_y1) AS trend_delta_roa,
      (roe_y2 - roe_y1) AS trend_delta_roe,
      (ccf_revenue_y2 - ccf_revenue_y1) AS trend_delta_ccf_revenue,

      (solvency_y2 - solvency_y1) AS trend_delta_solvency,
      (debt_to_equity_y2 - debt_to_equity_y1) AS trend_delta_debt_to_equity,

      (liquidity_custom_y2 - liquidity_custom_y1) AS trend_delta_liquidity_custom,
      (liquidity_standard_y2 - liquidity_standard_y1) AS trend_delta_liquidity_standard,

      (inventory_days_y2 - inventory_days_y1) AS trend_delta_inventory_days,
      (customer_days_y2 - customer_days_y1) AS trend_delta_customer_days,
      (supplier_days_y2 - supplier_days_y1) AS trend_delta_supplier_days,

      -- TRENDS: RELATIVE GROWTH % ((Y2 - Y1) / ABS(Y1))
      -- Use for absolute values (currency, FTEs)
      SAFE_DIVIDE(revenue_y2 - revenue_y1, ABS(NULLIF(revenue_y1, 0))) AS trend_growth_revenue,
      SAFE_DIVIDE(assets_y2 - assets_y1, ABS(NULLIF(assets_y1, 0))) AS trend_growth_assets,
      SAFE_DIVIDE(equity_y2 - equity_y1, ABS(NULLIF(equity_y1, 0))) AS trend_growth_equity,
      SAFE_DIVIDE(cashflow_y2 - cashflow_y1, ABS(NULLIF(cashflow_y1, 0))) AS trend_growth_cashflow,
      SAFE_DIVIDE(ebit_y2 - ebit_y1, ABS(NULLIF(ebit_y1, 0))) AS trend_growth_ebit,
      SAFE_DIVIDE(net_profit_y2 - net_profit_y1, ABS(NULLIF(net_profit_y1, 0))) AS trend_growth_net_profit,
      SAFE_DIVIDE(fte_y2 - fte_y1, ABS(NULLIF(fte_y1, 0))) AS trend_growth_fte

    FROM rolling_window
    WHERE year_y1 IS NOT NULL
      AND year_y2 = target_year_y3 - 1
      AND year_y1 = target_year_y3 - 2
  )

SELECT * FROM ml_features;