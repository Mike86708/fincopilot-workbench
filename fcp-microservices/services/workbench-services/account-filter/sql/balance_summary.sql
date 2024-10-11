SELECT 
    balance_sheet.ACCOUNT_ID AS account_id,
    ledger_account.GL_ACCOUNT_NAME AS account_name,
    ledger_account.GL_ACCOUNT_PARENT_ID AS parent_id
FROM 
    FINCOPILOT_CDM.WORKBENCH.fct_netsuite_balance_sheet balance_sheet
LEFT JOIN 
    FINCOPILOT_CDM.WORKBENCH_COMMMON.DIM_WB_GENERAL_LEDGER_ACCOUNT ledger_account
ON 
    balance_sheet.ACCOUNT_ID = ledger_account.GL_ACCOUNT_ID
WHERE 
    balance_sheet.SUBSIDIARY_ID = %(subsidiary_id)s;