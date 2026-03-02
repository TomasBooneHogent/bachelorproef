  -- RATIO CALCULATIONS
        (f.Code_9903 + f.Cost_Depreciation + f.Code_631_4 + f.Code_635_8 + f.Cost_Services +
         f.Code_65_66B + f.Code_66A + f.Code_66B - f.Code_76A - f.Code_76B) AS Calc_CashFlow_PreTax,

        (f.Equity - f.Code_20 - f.Code_21 - f.Code_12) AS Calc_Adjusted_Equity