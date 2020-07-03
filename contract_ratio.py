sql_adjust_detail = """
    SELECT
      customer_id,
      business_id,
      DATE_FORMAT(kpi_start_date, '%%Y-%%m-%%d') kpi_start_date,
      DATE_FORMAT(kpi_end_date, '%%Y-%%m-%%d') kpi_end_date,
      IFNULL(adjust_amount, 0) AS adjust_amount
    FROM
      pig_dd_billed_process_flow
    WHERE
      customer_id IS NOT NULL
    AND (
      kpi_end_date >= kpi_start_date
    )
    AND `result` = 'agree' AND `status` = 'COMPLETED' AND adjust_amount <> 0
"""
billed_adjust = pd.read_sql(sql_adjust_detail, conn)
billed_adjust = billed_adjust.melt(id_vars=['customer_id', 'business_id', 'adjust_amount'], value_vars = ['kpi_start_date', 'kpi_end_date'], var_name = 'kpi_date_type', value_name = 'kpi_date')
billed_adjust['kpi_date'] = billed_adjust['kpi_date'].apply(pd.to_datetime)
billed_adjust = billed_adjust.set_index('kpi_date').groupby(['customer_id', 'business_id']).apply(lambda x : x.resample('D').ffill().drop(['customer_id', 'business_id'], axis = 1)).reset_index(level=1)
billed_adjust = billed_adjust.reset_index()
billed_adjust.loc[billed_adjust.groupby(['customer_id', 'business_id'])['kpi_date'].idxmin(), 'indicator'] = 'gm'
billed_adjust.loc[billed_adjust['indicator'].isna(), 'adjust_amount'] = 0
billed_adjust['adjust_amount'] = billed_adjust.groupby(['customer_id', 'business_id'])['adjust_amount'].transform('mean')
billed_adjust.drop('indicator', axis = 1, inplace = True)
billed_adjust['kpi_date'] = billed_adjust['kpi_date'].dt.strftime("%Y-%m-%d")
billed_adjust_dict = billed_adjust.to_dict('records')
