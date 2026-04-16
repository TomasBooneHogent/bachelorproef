CREATE OR REPLACE TABLE `corpscore-be.staging.master_table_all_years_2`
PARTITION BY RANGE_BUCKET(accounting_year, GENERATE_ARRAY(2015, 2030, 1))
CLUSTER BY enterprise_number

AS
WITH
  -- DEDUPLICATE ACCOUNTS
  preferred_accounts AS (
    SELECT
      * EXCEPT (rn)
    FROM
      (
        SELECT
          *,
          EXTRACT(YEAR FROM SAFE_CAST(EndDate AS DATE)) AS accounting_year,
          REPLACE(EnterpriseNumber, '.', '') AS clean_enterprise_number,
          ROW_NUMBER()
            OVER (
              PARTITION BY EnterpriseNumber, EXTRACT(YEAR FROM SAFE_CAST(EndDate AS DATE))
              ORDER BY
                CASE SPLIT(ModelType, '-')[SAFE_OFFSET(0)]
                  WHEN 'm02' THEN 1 WHEN 'm82' THEN 1 WHEN 'm05' THEN 1
                  WHEN 'm01' THEN 2 WHEN 'm81' THEN 2 WHEN 'm04' THEN 2
                  WHEN 'm07' THEN 3 WHEN 'm87' THEN 3 WHEN 'm08' THEN 3
                  ELSE 4
                END ASC,
                CASE
                  WHEN Language = 'NL' THEN 1
                  WHEN Language = 'FR' THEN 2
                  ELSE 3
                END ASC,
                ImprovementDate DESC,
                COALESCE(ARRAY_LENGTH(Rubrics), 0) DESC
            ) AS rn
        FROM `corpscore-be.accounts.*`
        WHERE _TABLE_SUFFIX BETWEEN '2020' AND '2025'
          AND ModelType NOT LIKE "%p"
      )
    WHERE rn = 1
  ),

  -- PRE-CLEAN KBO DIMENSIONS
  clean_dim_enterprise AS (
    SELECT *, REPLACE(EnterpriseNumber, '.', '') AS clean_enterprise_number
    FROM `corpscore-be.kbo.dim_enterprise`
  ),

  clean_dim_address AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT
        *,
        REPLACE(EntityNumber, '.', '') AS clean_enterprise_number,
        ROW_NUMBER() OVER (
          PARTITION BY EntityNumber
          ORDER BY
            CASE WHEN DateStrikingOff IS NULL THEN 1 ELSE 2 END ASC,
            TypeOfAddress ASC
        ) AS rn
      FROM `kbo.dim_address`
    )
    WHERE rn = 1
  ),

  -- RANKING: Identify main activities
  ranked_activities AS (
    SELECT
      EnterpriseNumber,
      NaceCode,
      ActivityGroup,
      ROW_NUMBER() OVER (
        PARTITION BY EnterpriseNumber
        ORDER BY
          CASE
            WHEN ActivityGroup = '001' THEN 1
            WHEN ActivityGroup = '003' THEN 2
            WHEN ActivityGroup = '006' THEN 3
            ELSE 99
          END ASC,
          NaceCode ASC
      ) AS rn
    FROM `corpscore-be.kbo.dim_activity`
    WHERE NaceVersion = '2025' AND Classification = 'MAIN' AND ActivityGroup IN ('001', '003', '006')
  ),

  -- PIVOTING: Flatten top 3 NACE codes to columns
  pivoted_activities AS (
    SELECT
      EnterpriseNumber,
      REPLACE(EnterpriseNumber, '.', '') AS clean_enterprise_number,
      MAX(CASE WHEN rn = 1 THEN NaceCode END) AS nace_primary_code,
      SUBSTR(REPLACE(MAX(CASE WHEN rn = 1 THEN NaceCode END), '.', ''), 1, 2) AS nace_primary_division,
      MAX(CASE WHEN rn = 2 THEN NaceCode END) AS nace_secondary_code,
      SUBSTR(REPLACE(MAX(CASE WHEN rn = 2 THEN NaceCode END), '.', ''), 1, 2) AS nace_secondary_division,
      MAX(CASE WHEN rn = 3 THEN NaceCode END) AS nace_tertiary_code,
      SUBSTR(REPLACE(MAX(CASE WHEN rn = 3 THEN NaceCode END), '.', ''), 1, 2) AS nace_tertiary_division
    FROM ranked_activities
    GROUP BY EnterpriseNumber
  ),

  -- FINANCIALS BASE: Extract all needed NBB codes for N and NM1
  financials_base AS (
    SELECT
      acc.ReferenceNumber,
      acc.EnterpriseNumber,
      DATE(acc.EndDate) AS balance_sheet_date,

      -- PERIOD N (Current Year)
      SUM(CASE WHEN r.Code IN ('20/58', '2058') AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS total_assets_n,
      SUM(CASE WHEN r.Code = '9087' AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_9087_n,
      SUM(CASE WHEN r.Code IN ('10/15', '1015') AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS equity_n,
      SUM(CASE WHEN r.Code = '70' AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS revenue_n,
      SUM(CASE WHEN r.Code = '9904' AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS net_profit_n,
      SUM(CASE WHEN r.Code IN ('29/58', '2958') AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS current_assets_n,
      SUM(CASE WHEN r.Code IN ('42/48', '4248') AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS short_term_debt_n,
      SUM(CASE WHEN r.Code = '9901' AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS ebit_n,

      SUM(CASE WHEN r.Code = '9905' AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_9905_n,
      SUM(CASE WHEN r.Code = '16' AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_16_n,
      SUM(CASE WHEN r.Code = '17/49' AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_17_49_n,
      SUM(CASE WHEN r.Code = '290' AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_290_n,
      SUM(CASE WHEN r.Code = '175' AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_175_n,
      SUM(CASE WHEN r.Code = '42' AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_42_n,
      SUM(CASE WHEN r.Code = '10' AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_10_n,
      SUM(CASE WHEN r.Code = '60' AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_60_n,
      SUM(CASE WHEN r.Code = '61' AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_61_n,
      SUM(CASE WHEN r.Code = '630' AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_630_n,
      SUM(CASE WHEN r.Code = '631/4' AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_631_4_n,
      SUM(CASE WHEN r.Code = '635/8' AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_635_8_n,
      SUM(CASE WHEN r.Code = '66A' AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_66A_n,
      SUM(CASE WHEN r.Code = '76A' AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_76A_n,
      SUM(CASE WHEN r.Code = '3' AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_3_n,
      SUM(CASE WHEN r.Code = '40' AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_40_n,
      SUM(CASE WHEN r.Code = '44' AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_44_n,
      SUM(CASE WHEN r.Code = '490/1' AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_490_1_n,
      SUM(CASE WHEN r.Code = '9900' AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_9900_n,
      SUM(CASE WHEN r.Code = '1003' AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_1003_n,
      SUM(CASE WHEN r.Code = '9903' AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_9903_n,
      SUM(CASE WHEN r.Code IN ('65/66B', '65_66B') AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_65_66B_n,
      SUM(CASE WHEN r.Code = '66B' AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_66B_n,
      SUM(CASE WHEN r.Code = '76B' AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_76B_n,
      SUM(CASE WHEN r.Code = '20' AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_20_n,
      SUM(CASE WHEN r.Code = '21' AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_21_n,
      SUM(CASE WHEN r.Code = '12' AND r.Period = 'N' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_12_n,

      -- PERIOD NM1 (Previous Year)
      SUM(CASE WHEN r.Code IN ('20/58', '2058') AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS total_assets_nm1,
      SUM(CASE WHEN r.Code = '9087' AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_9087_nm1,
      SUM(CASE WHEN r.Code IN ('10/15', '1015') AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS equity_nm1,
      SUM(CASE WHEN r.Code = '70' AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS revenue_nm1,
      SUM(CASE WHEN r.Code = '9904' AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS net_profit_nm1,
      SUM(CASE WHEN r.Code IN ('29/58', '2958') AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS current_assets_nm1,
      SUM(CASE WHEN r.Code IN ('42/48', '4248') AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS short_term_debt_nm1,
      SUM(CASE WHEN r.Code = '9901' AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS ebit_nm1,

      SUM(CASE WHEN r.Code = '9905' AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_9905_nm1,
      SUM(CASE WHEN r.Code = '16' AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_16_nm1,
      SUM(CASE WHEN r.Code = '17/49' AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_17_49_nm1,
      SUM(CASE WHEN r.Code = '290' AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_290_nm1,
      SUM(CASE WHEN r.Code = '175' AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_175_nm1,
      SUM(CASE WHEN r.Code = '42' AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_42_nm1,
      SUM(CASE WHEN r.Code = '10' AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_10_nm1,
      SUM(CASE WHEN r.Code = '60' AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_60_nm1,
      SUM(CASE WHEN r.Code = '61' AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_61_nm1,
      SUM(CASE WHEN r.Code = '630' AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_630_nm1,
      SUM(CASE WHEN r.Code = '631/4' AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_631_4_nm1,
      SUM(CASE WHEN r.Code = '635/8' AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_635_8_nm1,
      SUM(CASE WHEN r.Code = '66A' AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_66A_nm1,
      SUM(CASE WHEN r.Code = '76A' AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_76A_nm1,
      SUM(CASE WHEN r.Code = '3' AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_3_nm1,
      SUM(CASE WHEN r.Code = '40' AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_40_nm1,
      SUM(CASE WHEN r.Code = '44' AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_44_nm1,
      SUM(CASE WHEN r.Code = '490/1' AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_490_1_nm1,
      SUM(CASE WHEN r.Code = '9900' AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_9900_nm1,
      SUM(CASE WHEN r.Code = '1003' AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_1003_nm1,
      SUM(CASE WHEN r.Code = '9903' AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_9903_nm1,
      SUM(CASE WHEN r.Code IN ('65/66B', '65_66B') AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_65_66B_nm1,
      SUM(CASE WHEN r.Code = '66B' AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_66B_nm1,
      SUM(CASE WHEN r.Code = '76B' AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_76B_nm1,
      SUM(CASE WHEN r.Code = '20' AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_20_nm1,
      SUM(CASE WHEN r.Code = '21' AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_21_nm1,
      SUM(CASE WHEN r.Code = '12' AND r.Period = 'NM1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE NULL END) AS c_12_nm1

    FROM preferred_accounts acc
    LEFT JOIN UNNEST(ARRAY(SELECT AS STRUCT * FROM UNNEST(acc.Rubrics) WHERE Period IN ('N', 'NM1'))) AS r
    GROUP BY 1, 2, 3
  ),

  -- FINANCIALS COMPUTED
  financials AS (
    SELECT
      *,
      COALESCE(c_9087_n, c_1003_n) AS personnel_fte_n,

      (COALESCE(c_9903_n, 0) + COALESCE(c_630_n, 0) + COALESCE(c_631_4_n, 0) + COALESCE(c_635_8_n, 0) + COALESCE(c_61_n, 0) + COALESCE(c_65_66B_n, 0) + COALESCE(c_66A_n, 0) + COALESCE(c_66B_n, 0) - COALESCE(c_76A_n, 0) - COALESCE(c_76B_n, 0)) AS current_cashflow_pre_tax_n,
      (COALESCE(c_9903_nm1, 0) + COALESCE(c_630_nm1, 0) + COALESCE(c_631_4_nm1, 0) + COALESCE(c_635_8_nm1, 0) + COALESCE(c_61_nm1, 0) + COALESCE(c_65_66B_nm1, 0) + COALESCE(c_66A_nm1, 0) + COALESCE(c_66B_nm1, 0) - COALESCE(c_76A_nm1, 0) - COALESCE(c_76B_nm1, 0)) AS current_cashflow_pre_tax_nm1,

      (COALESCE(equity_n, 0) - COALESCE(c_20_n, 0) - COALESCE(c_21_n, 0) - COALESCE(c_12_n, 0)) AS adjusted_equity_n,
      (COALESCE(equity_nm1, 0) - COALESCE(c_20_nm1, 0) - COALESCE(c_21_nm1, 0) - COALESCE(c_12_nm1, 0)) AS adjusted_equity_nm1,

      (COALESCE(c_9900_n, 0) - COALESCE(c_76A_n, 0)) AS current_value_added_n,
      (COALESCE(c_9900_nm1, 0) - COALESCE(c_76A_nm1, 0)) AS current_value_added_nm1
    FROM financials_base
  )

-- MAIN SELECT & JOINS
SELECT
  acc.accounting_year,
  acc.ReferenceNumber AS reference_number,
  acc.EnterpriseNumber AS enterprise_number,
  acc.EnterpriseName AS enterprise_name,
  acc.EnterpriseName_historic AS enterprise_name_historic,

  -- Model Types
  acc.Language AS account_language,
  acc.ModelType AS raw_model_type,
  CASE SPLIT(acc.ModelType, '-')[SAFE_OFFSET(0)]
    WHEN 'm02' THEN 'Full Model (With Capital)'
    WHEN 'm01' THEN 'Abbreviated Model (With Capital)'
    WHEN 'm07' THEN 'Micro Model (With Capital)'
    WHEN 'm82' THEN 'Full Model (Without Capital)'
    WHEN 'm81' THEN 'Abbreviated Model (Without Capital)'
    WHEN 'm87' THEN 'Micro Model (Without Capital)'
    WHEN 'm05' THEN 'Full Model (Association/Foundation)'
    WHEN 'm04' THEN 'Abbreviated Model (Association/Foundation)'
    WHEN 'm08' THEN 'Micro Model (Association/Foundation)'
    WHEN 'm120' THEN 'Consolidated Annual Accounts'
    WHEN 'm121' THEN 'Consolidated Annual Accounts'
    WHEN 'm122' THEN 'Consolidated Annual Accounts'
    ELSE 'Specific/Sector Model (e.g., Financial/Healthcare)'
  END AS model_category,

  acc.StartDate AS account_start_date,
  acc.EndDate AS account_end_date,

  -- CBE (KBO) Data
  act.nace_primary_code,
  act.nace_primary_division,
  act.nace_secondary_code,
  act.nace_tertiary_code,
  address.Zipcode,
  address.MunicipalityNL,

  -- Legal information & Target
  e.JuridicalSituation AS legal_status_code_current,
  e.Status AS enterprise_status_current,
  acc.LegalSituation AS legal_situation_historic,
  CASE
    WHEN e.JuridicalSituation IN ('010', '012', '013', '030', '040', '048', '049', '050', '051') THEN 1
    ELSE 0
  END AS is_failed_CURRENT_STATUS,
  -- BASE FINANCIALS (Absolute waarden)
  f.revenue_n,
  f.revenue_nm1,
  f.total_assets_n,
  f.total_assets_nm1,
  f.ebit_n,
  f.ebit_nm1,
  f.net_profit_n,
  f.net_profit_nm1,
  f.personnel_fte_n,
  -- PROFITABILITY (Rentabiliteit)
  f.current_cashflow_pre_tax_n,
  f.current_cashflow_pre_tax_nm1,
  SAFE_DIVIDE(f.current_cashflow_pre_tax_n, f.revenue_n) AS current_ccf_revenue_n,
  SAFE_DIVIDE(f.current_cashflow_pre_tax_nm1, f.revenue_nm1) AS current_ccf_revenue_nm1,

  SAFE_DIVIDE(f.net_profit_n, f.total_assets_n) AS roa_n,
  SAFE_DIVIDE(f.net_profit_nm1, f.total_assets_nm1) AS roa_nm1,

  SAFE_DIVIDE(f.c_9905_n, f.equity_n) AS roe_n,
  SAFE_DIVIDE(f.c_9905_nm1, f.equity_nm1) AS roe_nm1,

  f.current_value_added_n,
  f.current_value_added_nm1,
  -- SOLVENCY (Solvabiliteit)
  f.equity_n AS equity_n,
  f.equity_nm1 AS equity_nm1,
  f.adjusted_equity_n,
  f.adjusted_equity_nm1,

  SAFE_DIVIDE((COALESCE(f.c_16_n, 0) + COALESCE(f.c_17_49_n, 0)), f.equity_n) AS debt_to_equity_n,
  SAFE_DIVIDE((COALESCE(f.c_16_nm1, 0) + COALESCE(f.c_17_49_nm1, 0)), f.equity_nm1) AS debt_to_equity_nm1,

  SAFE_DIVIDE(f.equity_n, f.total_assets_n) AS solvency_pct_n,
  SAFE_DIVIDE(f.equity_nm1, f.total_assets_nm1) AS solvency_pct_nm1,

  -- LIQUIDITY (Liquiditeit)
  SAFE_DIVIDE(f.c_490_1_n, f.c_10_n) AS current_ratio_custom_n,
  SAFE_DIVIDE(f.c_490_1_nm1, f.c_10_nm1) AS current_ratio_custom_nm1,

  SAFE_DIVIDE(f.c_3_n, f.revenue_n) * 365 AS inventory_turnover_days_n,
  SAFE_DIVIDE(f.c_3_nm1, f.revenue_nm1) * 365 AS inventory_turnover_days_nm1,

  SAFE_DIVIDE((COALESCE(f.c_290_n, 0) + COALESCE(f.c_40_n, 0)), f.revenue_n) * 365 AS customer_credit_days_n,
  SAFE_DIVIDE((COALESCE(f.c_290_nm1, 0) + COALESCE(f.c_40_nm1, 0)), f.revenue_nm1) * 365 AS customer_credit_days_nm1,

  SAFE_DIVIDE((COALESCE(f.c_175_n, 0) + COALESCE(f.c_44_n, 0)), (COALESCE(f.c_60_n, 0) + COALESCE(f.c_61_n, 0))) * 365 AS supplier_credit_days_n,
  SAFE_DIVIDE((COALESCE(f.c_175_nm1, 0) + COALESCE(f.c_44_nm1, 0)), (COALESCE(f.c_60_nm1, 0) + COALESCE(f.c_61_nm1, 0))) * 365 AS supplier_credit_days_nm1,

  (f.current_cashflow_pre_tax_n - COALESCE(f.c_42_n, 0)) AS repayment_pre_tax_n,
  (f.current_cashflow_pre_tax_nm1 - COALESCE(f.c_42_nm1, 0)) AS repayment_pre_tax_nm1,

  SAFE_DIVIDE(f.current_assets_n, f.short_term_debt_n) AS liquidity_ratio_n,
  SAFE_DIVIDE(f.current_assets_nm1, f.short_term_debt_nm1) AS liquidity_ratio_nm1,

  -- SEGMENTATION
  CASE
    WHEN COALESCE(f.total_assets_n, 0) < 1000 AND COALESCE(f.revenue_n, 0) < 1000 THEN 'Inactive/Empty'
    WHEN COALESCE(f.personnel_fte_n, 0) = 0 AND COALESCE(f.total_assets_n, 0) > 500000 THEN 'Holding'
    WHEN ((CASE WHEN COALESCE(f.total_assets_n, 0) > 350000 THEN 1 ELSE 0 END) + (CASE WHEN COALESCE(f.revenue_n, 0) > 700000 THEN 1 ELSE 0 END) + (CASE WHEN COALESCE(f.personnel_fte_n, 0) > 10 THEN 1 ELSE 0 END)) <= 1 THEN 'Micro'
    WHEN ((CASE WHEN COALESCE(f.total_assets_n, 0) > 4500000 THEN 1 ELSE 0 END) + (CASE WHEN COALESCE(f.revenue_n, 0) > 9000000 THEN 1 ELSE 0 END) + (CASE WHEN COALESCE(f.personnel_fte_n, 0) > 50 THEN 1 ELSE 0 END)) <= 1 THEN 'SME'
    ELSE 'Large'
  END AS company_segment

FROM
  preferred_accounts acc
LEFT JOIN financials f
  ON acc.ReferenceNumber = f.ReferenceNumber
LEFT JOIN clean_dim_enterprise e
  ON acc.clean_enterprise_number = e.clean_enterprise_number
LEFT JOIN pivoted_activities act
  ON acc.clean_enterprise_number = act.clean_enterprise_number
LEFT JOIN clean_dim_address address
  ON acc.clean_enterprise_number = address.clean_enterprise_number