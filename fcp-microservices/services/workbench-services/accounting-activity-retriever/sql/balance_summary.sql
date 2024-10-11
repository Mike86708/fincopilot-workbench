SELECT 
    -- t.TRANSACTION_ACCOUNTING_PERIOD_ID,
    -- t.ACCOUNT_ID,
    -- t.SUBSIDIARY_ID,
    a.GL_ACCOUNT_NAME,
    t.transaction_type,
    t.transaction_date,
    t.transaction_document_number,
    t.customer_name,
    t.amount_usd
FROM 
    FINCOPILOT_CDM.WORKBENCH.FCT_NETSUITE_BALANCE_SHEET t
LEFT JOIN 
    FINCOPILOT_CDM.WORKBENCH_COMMMON.DIM_WB_GENERAL_LEDGER_ACCOUNT a
ON 
    t.ACCOUNT_ID = a.GL_ACCOUNT_ID
WHERE 
    t.TRANSACTION_ACCOUNTING_PERIOD_ID BETWEEN %(from_period_id)s AND %(to_period_id)s
    AND t.SUBSIDIARY_ID = %(subsidiary_id)s
    AND t.ACCOUNT_ID = %(account_id)s;