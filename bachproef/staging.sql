CREATE OR REPLACE TABLE `corpscore-be.staging.master_table_2021` AS

WITH
  -- 1. RANKING: Identify main activities
  ranked_activities AS (
    SELECT
      EnterpriseNumber,
      NaceCode,
      ActivityGroup,
      ROW_NUMBER() OVER (
        PARTITION BY EnterpriseNumber
        ORDER BY
          -- Priority: VAT (001) > KBO (003) > Social Security (006)
          CASE
            WHEN ActivityGroup = '001' THEN 1
            WHEN ActivityGroup = '003' THEN 2
            WHEN ActivityGroup = '006' THEN 3
            ELSE 99
          END ASC,
          NaceCode ASC
      ) AS rn
    FROM `corpscore-be.kbo.dim_activity`
    WHERE
      NaceVersion = '2025'
      AND Classification = 'MAIN'
      AND ActivityGroup IN ('001', '003', '006')
  ),

  -- 2. PIVOTING: Flatten top 3 NACE codes to columns
  pivoted_activities AS (
    SELECT
      EnterpriseNumber,
      MAX(CASE WHEN rn = 1 THEN NaceCode END) AS nace_primary_code,
      SUBSTR(REPLACE(MAX(CASE WHEN rn = 1 THEN NaceCode END), '.', ''), 1, 2) AS nace_primary_division,
      MAX(CASE WHEN rn = 2 THEN NaceCode END) AS nace_secondary_code,
      SUBSTR(REPLACE(MAX(CASE WHEN rn = 2 THEN NaceCode END), '.', ''), 1, 2) AS nace_secondary_division,
      MAX(CASE WHEN rn = 3 THEN NaceCode END) AS nace_tertiary_code,
      SUBSTR(REPLACE(MAX(CASE WHEN rn = 3 THEN NaceCode END), '.', ''), 1, 2) AS nace_tertiary_division
    FROM ranked_activities
    GROUP BY EnterpriseNumber
  ),

  -- 3a. FINANCIALS BASE: Extract all needed NBB codes (Full + Micro/Abbrev) in one pass
  financials_base AS (
    SELECT
      acc.ReferenceNumber,
      acc.EnterpriseNumber,
      PARSE_DATE('%Y-%m-%d', LEFT(CAST(MAX(acc.EndDate) AS STRING), 10)) AS balance_sheet_date,

      -- Core Codes
      SUM(CASE WHEN r.Code IN ('20/58', '2058') THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS total_assets,
      SUM(CASE WHEN r.Code = '9087' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_9087,
      SUM(CASE WHEN r.Code IN ('10/15', '1015') THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS equity,
      SUM(CASE WHEN r.Code = '70' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS revenue,
      SUM(CASE WHEN r.Code = '9904' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS net_profit,
      SUM(CASE WHEN r.Code IN ('29/58', '2958') THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS current_assets,
      SUM(CASE WHEN r.Code IN ('42/48', '4248') THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS short_term_debt,
      SUM(CASE WHEN r.Code = '9901' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS ebit,

      -- Standard NBB Codes (Full Model)
      SUM(CASE WHEN r.Code = '71' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_71,
      SUM(CASE WHEN r.Code = '72' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_72,
      SUM(CASE WHEN r.Code = '74' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_74,
      SUM(CASE WHEN r.Code = '740' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_740,
      SUM(CASE WHEN r.Code = '60' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_60,
      SUM(CASE WHEN r.Code = '61' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_61,
      SUM(CASE WHEN r.Code = '62' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_62,
      SUM(CASE WHEN r.Code = '630' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_630,
      SUM(CASE WHEN r.Code = '631/4' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_631_4,
      SUM(CASE WHEN r.Code = '635/8' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_635_8,
      SUM(CASE WHEN r.Code = '635' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_635,
      SUM(CASE WHEN r.Code = '640/8' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_640_8,
      SUM(CASE WHEN r.Code = '649' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_649,
      SUM(CASE WHEN r.Code = '66A' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_66A,
      SUM(CASE WHEN r.Code = '76A' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_76A,
      SUM(CASE WHEN r.Code = '9125' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_9125,
      SUM(CASE WHEN r.Code = '650' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_650,
      SUM(CASE WHEN r.Code = '653' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_653,
      SUM(CASE WHEN r.Code = '6501' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_6501,
      SUM(CASE WHEN r.Code = '651' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_651,
      SUM(CASE WHEN r.Code = '6560' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_6560,
      SUM(CASE WHEN r.Code = '6561' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_6561,
      SUM(CASE WHEN r.Code = '660' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_660,
      SUM(CASE WHEN r.Code = '661' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_661,
      SUM(CASE WHEN r.Code = '662' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_662,
      SUM(CASE WHEN r.Code = '760' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_760,
      SUM(CASE WHEN r.Code = '761' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_761,
      SUM(CASE WHEN r.Code = '762' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_762,
      SUM(CASE WHEN r.Code = '663' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_663,
      SUM(CASE WHEN r.Code = '780' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_780,
      SUM(CASE WHEN r.Code = '680' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_680,
      SUM(CASE WHEN r.Code = '9126' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_9126,
      SUM(CASE WHEN r.Code = '9134' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_9134,
      SUM(CASE WHEN r.Code = '3' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_3,
      SUM(CASE WHEN r.Code = '40/41' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_40_41,
      SUM(CASE WHEN r.Code = '50/53' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_50_53,
      SUM(CASE WHEN r.Code = '54/58' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_54_58,
      SUM(CASE WHEN r.Code = '490/1' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_490_1,
      SUM(CASE WHEN r.Code = '492/3' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_492_3,
      SUM(CASE WHEN r.Code = '30/31' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_30_31,
      SUM(CASE WHEN r.Code = '34' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_34,
      SUM(CASE WHEN r.Code = '35' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_35,
      SUM(CASE WHEN r.Code = '36' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_36,
      SUM(CASE WHEN r.Code = '32' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_32,
      SUM(CASE WHEN r.Code = '33' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_33,
      SUM(CASE WHEN r.Code = '37' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_37,
      SUM(CASE WHEN r.Code = '40' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_40,
      SUM(CASE WHEN r.Code = '44' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_44,
      SUM(CASE WHEN r.Code = '600/8' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_600_8,
      SUM(CASE WHEN r.Code = '9150' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_9150,
      SUM(CASE WHEN r.Code = '9146' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_9146,
      SUM(CASE WHEN r.Code = '9145' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_9145,
      SUM(CASE WHEN r.Code = '10/49' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_10_49,
      SUM(CASE WHEN r.Code = '8199P' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_8199P,
      SUM(CASE WHEN r.Code = '8199' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_8199,
      SUM(CASE WHEN r.Code = '8169' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_8169,
      SUM(CASE WHEN r.Code = '8229' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_8229,
      SUM(CASE WHEN r.Code = '8299' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_8299,
      SUM(CASE WHEN r.Code = '8259P' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_8259P,
      SUM(CASE WHEN r.Code = '8329P' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_8329P,

      -- Abbreviated & Micro Model Specific Codes
      SUM(CASE WHEN r.Code = '9900' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_9900,
      SUM(CASE WHEN r.Code = '60/61' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_60_61,
      SUM(CASE WHEN r.Code = '1003' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_1003,
      SUM(CASE WHEN r.Code = '8079' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_8079,
      SUM(CASE WHEN r.Code = '8279' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_8279,
      SUM(CASE WHEN r.Code = '8475' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_8475,
      SUM(CASE WHEN r.Code = '8089' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_8089,
      SUM(CASE WHEN r.Code = '8289' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_8289,
      SUM(CASE WHEN r.Code = '8485' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_8485,
      SUM(CASE WHEN r.Code = '67/77' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_67_77,
      SUM(CASE WHEN r.Code = '753' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_753,
      SUM(CASE WHEN r.Code = '1079' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_1079,

      -- NEW: Custom Request Codes Extracted
      SUM(CASE WHEN r.Code = '9903' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_9903,
      SUM(CASE WHEN r.Code IN ('65/66B', '65_66B') THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_65_66B,
      SUM(CASE WHEN r.Code = '66B' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_66B,
      SUM(CASE WHEN r.Code = '76B' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_76B,
      SUM(CASE WHEN r.Code = '20' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_20,
      SUM(CASE WHEN r.Code = '21' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_21,
      SUM(CASE WHEN r.Code = '12' THEN SAFE_CAST(r.Value AS FLOAT64) ELSE 0 END) AS c_12

    FROM `corpscore-be.accounts.2021` acc
    LEFT JOIN UNNEST(acc.Rubrics) AS r
    GROUP BY 1, 2
  ),

  -- 3b. FINANCIALS COMPUTED: Add the custom formulas here
  financials AS (
    SELECT
      *,
      -- FTE Logic: Use 9087, fallback to 1003 for micro companies
      CASE WHEN c_9087 > 0 THEN c_9087 ELSE c_1003 END AS personnel_fte,

      -- Value Added Logic: Full Model (Revenue) vs Abbrev/Micro (Gross Margin)
      IF(revenue > 0,
         (revenue + c_71 + c_72 + c_74 - c_740 - c_60 - c_61),
         (c_9900 - c_76A)) AS value_added,

      -- Operating Income Logic: Full vs Abbrev/Micro

      IF(revenue > 0,
         (revenue + c_71 + c_72 + c_74 - c_740),
         (c_9900 - c_76A + c_60_61)) AS operating_income,

      -- Your Custom Calculations mapped to our exact column names
      (c_9903 + c_630 + c_631_4 + c_635_8 + c_61 + c_65_66B + c_66A + c_66B - c_76A - c_76B) AS calc_cashflow_pretax,
      (equity - c_20 - c_21 - c_12) AS calc_adjusted_equity

    FROM financials_base
  )

-- 4. MAIN SELECT & JOINS
SELECT
  -- Identifiers & Core Account Info
  acc.ReferenceNumber AS reference_number,
  acc.EnterpriseNumber AS enterprise_number,
  acc.EnterpriseName AS enterprise_name,
  acc.EnterpriseName_historic AS enterprise_name_historic,
  acc.Language AS account_language,
  acc.AccountingDataURL AS accounting_data_url,
  acc.CorrectedData AS corrected_data,
  acc.ModelType AS model_type,
  acc.DepositType AS deposit_type,
  acc.Currency AS currency,
  acc.FullFillLegalValidation AS full_legal_validation,
  acc.ImprovementDate AS improvement_date,
  acc.DataVersion AS data_version,
  acc.StartDate AS account_start_date,
  acc.EndDate AS account_end_date,
  acc.Rubrics,

  -- Legal Information
  e.JuridicalSituation AS legal_status_code_current,
  e.Status AS enterprise_status_current,
  acc.LegalSituation AS legal_situation_historic,
  acc.LegalForm_historic AS legal_situation_historic_2,
  e.JuridicalForm AS legal_form_current,
  DATE_DIFF(f.balance_sheet_date, e.StartDate, YEAR) AS company_age,

  -- Sector Data
  acc.ActivityCode AS activity_historic,
  act.nace_primary_code,
  map1.analysis_sector AS sector_primary,
  act.nace_secondary_code,
  map2.analysis_sector AS sector_secondary,
  act.nace_tertiary_code,
  map3.analysis_sector AS sector_tertiary,

  -- Target Variable (1 = Failed/Bankruptcy/Dissolution)
  CASE
    WHEN e.JuridicalSituation IN ('010', '012', '013', '030', '040', '048', '049', '050', '051') THEN 1
    ELSE 0
  END AS is_failed,

  -- Geo Data
  address.TypeOfAddress AS type_of_address,
  address.CountryNL AS country_dutch,
  address.CountryFR AS country_french,
  address.Zipcode   AS zipcode,
  address.MunicipalityNL  AS municipality_dutch,
  address.MunicipalityFR AS municipality_french,
  address.StreetNL  AS street_dutch,
  address.StreetFR  AS street_french,
  address.HouseNumber AS house_number,
  address.Box as address_box,
  address.ExtraAddressInfo  AS extra_address_info,
  address.DateStrikingOff AS date_striking_off,
  acc.Address_historic AS address_historic,

  -- Basic Financials
  f.total_assets,
  f.personnel_fte,
  f.equity,
  f.revenue,
  f.net_profit,
  f.current_assets,
  f.short_term_debt,
  f.ebit,

  -- NEW: Your requested calculated fields
  f.calc_cashflow_pretax,
  f.calc_adjusted_equity,

  -- Baseline Original Ratios
  SAFE_DIVIDE(f.equity, f.total_assets) AS solvency_ratio,
  SAFE_DIVIDE(f.current_assets, f.short_term_debt) AS liquidity_ratio,
  SAFE_DIVIDE(f.net_profit, f.total_assets) AS roa_ratio,
  SAFE_DIVIDE(f.personnel_fte, f.revenue) * 100000 AS labor_intensity,

  -- 21 Standard NBB Financial Ratios (Dynamic for Full & Abbreviated/Micro)
  IF(f.revenue > 0, SAFE_DIVIDE((f.ebit - f.c_76A + f.c_66A + f.c_630 + f.c_631_4 + f.c_635_8), (f.revenue + f.c_74 - f.c_740)) * 100, NULL) AS ratio_01_gross_sales_margin,
  IF(f.revenue > 0, SAFE_DIVIDE((f.ebit - f.c_76A + f.c_66A + f.c_9125), (f.revenue + f.c_74 - f.c_740)) * 100, NULL) AS ratio_02_net_sales_margin,
  SAFE_DIVIDE(f.value_added, f.operating_income) * 100 AS ratio_03_value_added_operating_income,
  SAFE_DIVIDE(f.value_added, f.personnel_fte) AS ratio_04_value_added_per_employee_eur,
  SAFE_DIVIDE(f.value_added, (f.c_8199P + f.c_8199)) * 200 AS ratio_05_value_added_gross_tangible_assets,
  SAFE_DIVIDE(IF(f.revenue > 0, f.c_62 + f.c_635, f.c_62), f.value_added) * 100 AS ratio_06_personnel_costs_value_added,
  SAFE_DIVIDE(IF(f.revenue > 0, (f.c_630 + f.c_631_4 + f.c_635_8 - f.c_635), (f.c_630 + f.c_631_4 + f.c_635_8)), f.value_added) * 100 AS ratio_07_depreciations_value_added,
  SAFE_DIVIDE((f.c_650 + f.c_653), f.value_added) * 100 AS ratio_08_cost_of_debt_value_added,
  SAFE_DIVIDE(f.net_profit, f.equity) * 100 AS ratio_09_roe,
  SAFE_DIVIDE(IF(f.revenue > 0,
    (f.net_profit + f.c_630 + f.c_631_4 + f.c_6501 + f.c_635_8 + f.c_651 + f.c_6560 - f.c_6561 + f.c_660 + f.c_661 + f.c_662 - f.c_760 - f.c_761 - f.c_762 + f.c_663 - f.c_9125 - f.c_780 + f.c_680),
    (f.net_profit + f.c_631_4 + f.c_635_8 + f.c_8079 + f.c_8279 + f.c_8475 - f.c_8089 - f.c_8289 - f.c_8485 - f.c_780 + f.c_680)
  ), f.equity) * 100 AS ratio_10_cashflow_equity,
  SAFE_DIVIDE(IF(f.revenue > 0,
    (f.net_profit + f.c_650 + f.c_653 - f.c_9125 - f.c_9126 + f.c_630 + f.c_631_4 + f.c_635_8 + f.c_651 + f.c_6560 - f.c_6561 + f.c_660 + f.c_661 + f.c_662 - f.c_760 - f.c_761 - f.c_762 + f.c_663 + f.c_9134 - f.c_780 + f.c_680),
    (f.net_profit + f.c_650 + f.c_653 - f.c_753 + f.c_631_4 + f.c_635_8 + f.c_1079 + f.c_8279 + f.c_8475 - f.c_8089 - f.c_8289 - f.c_8485 + f.c_67_77 - f.c_780 + f.c_680)
  ), f.total_assets) * 100 AS ratio_11_gross_roa,
  SAFE_DIVIDE(IF(f.revenue > 0,
    (f.net_profit + f.c_650 + f.c_653 - f.c_9126 + f.c_9134),
    (f.net_profit + f.c_650 + f.c_653 + f.c_67_77)
  ), f.total_assets) * 100 AS ratio_12_net_roa,
  SAFE_DIVIDE((f.c_3 + f.c_40_41 + f.c_50_53 + f.c_54_58 + f.c_490_1), (f.short_term_debt + f.c_492_3)) AS ratio_13_current_ratio,
  SAFE_DIVIDE((f.c_40_41 + f.c_50_53 + f.c_54_58), f.short_term_debt) AS ratio_14_quick_ratio,
  IF(f.revenue > 0, SAFE_DIVIDE(f.c_60, (f.c_30_31 + f.c_34 + f.c_35 + f.c_36)), NULL) AS ratio_15_inventory_turnover_raw_materials,
  IF(f.revenue > 0, SAFE_DIVIDE((f.c_60 + f.c_61 + f.c_62 + f.c_630 + f.c_631_4 + f.c_635_8 + f.c_640_8 + f.c_649 - f.c_71 - f.c_72 - f.c_740 - f.c_9125), (f.c_32 + f.c_33 + f.c_35 + f.c_37)), NULL) AS ratio_16_inventory_turnover_wip_finished,
  SAFE_DIVIDE((f.c_40 + f.c_9150), IF(f.revenue > 0, (f.revenue + f.c_74 - f.c_740 + f.c_9146), (f.operating_income + f.c_9146))) * 365 AS ratio_17_days_sales_outstanding,
  SAFE_DIVIDE(f.c_44, IF(f.revenue > 0, (f.c_600_8 + f.c_61 + f.c_9145), f.c_60_61)) * 365 AS ratio_18_days_payable_outstanding,
  SAFE_DIVIDE(f.equity, f.c_10_49) * 100 AS ratio_19_solvency,
  SAFE_DIVIDE((f.c_8169 + f.c_8229 - f.c_8299), f.value_added) * 100 AS ratio_20_tangible_asset_acquisitions_value_added,
  SAFE_DIVIDE((f.c_8169 + f.c_8229 - f.c_8299), (f.c_8199P + f.c_8259P - f.c_8329P)) * 100 AS ratio_21_renewal_degree_tangible_assets,

  -- Segmentation based on Official Belgian WVV Criteria (2021 Thresholds)
  CASE
    -- 1. Practical Heuristics for Modeling
    WHEN COALESCE(f.total_assets, 0) < 1000 AND COALESCE(f.revenue, 0) < 1000 THEN 'Inactive/Empty'
    WHEN COALESCE(f.personnel_fte, 0) = 0 AND COALESCE(f.total_assets, 0) > 500000 THEN 'Holding'

    -- 2. Micro Company Check (Cannot exceed > 1 of these limits)
    WHEN (
      (CASE WHEN COALESCE(f.total_assets, 0) > 350000 THEN 1 ELSE 0 END) +
      (CASE WHEN COALESCE(f.revenue, 0) > 700000 THEN 1 ELSE 0 END) +
      (CASE WHEN COALESCE(f.personnel_fte, 0) > 10 THEN 1 ELSE 0 END)
    ) <= 1 THEN 'Micro'

    -- 3. SME / Small Company Check (Cannot exceed > 1 of these limits)
    WHEN (
      (CASE WHEN COALESCE(f.total_assets, 0) > 4500000 THEN 1 ELSE 0 END) +
      (CASE WHEN COALESCE(f.revenue, 0) > 9000000 THEN 1 ELSE 0 END) +
      (CASE WHEN COALESCE(f.personnel_fte, 0) > 50 THEN 1 ELSE 0 END)
    ) <= 1 THEN 'SME'

    -- 4. Default to Large
    ELSE 'Large'
  END AS company_segment

FROM
    `corpscore-be.accounts.2021` acc
  -- Clean joins mapping accounts to financials via ReferenceNumber
  LEFT JOIN financials f
    ON acc.ReferenceNumber = f.ReferenceNumber
  LEFT JOIN `corpscore-be.kbo.dim_enterprise` e
    ON REPLACE(acc.EnterpriseNumber, '.', '') = REPLACE(e.EnterpriseNumber, '.', '')
  LEFT JOIN pivoted_activities act
    ON REPLACE(acc.EnterpriseNumber, '.', '') = REPLACE(act.EnterpriseNumber, '.', '')
  LEFT JOIN `kbo.dim_address` address
    ON REPLACE(acc.EnterpriseNumber, '.', '') = REPLACE(address.EntityNumber, '.', '')

  -- Mapping NACE codes to sectors
  LEFT JOIN `corpscore-be.kbo.dim_nace_mapping` map1
    ON act.nace_primary_division = map1.nace_division
  LEFT JOIN `corpscore-be.kbo.dim_nace_mapping` map2
    ON act.nace_secondary_division = map2.nace_division
  LEFT JOIN `corpscore-be.kbo.dim_nace_mapping` map3
    ON act.nace_tertiary_division = map3.nace_division;