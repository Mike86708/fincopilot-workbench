SELECT 
    trial_balance.ACCOUNT_ID AS account_id,
    ledger_account.GL_ACCOUNT_NAME AS account_name,
    ledger_account.GL_ACCOUNT_PARENT_ID AS parent_id
FROM 
    FINCOPILOT_CDM.WORKBENCH.FCT_NETSUITE_TRIAL_BALANCE trial_balance
LEFT JOIN 
    FINCOPILOT_CDM.WORKBENCH_COMMMON.DIM_WB_GENERAL_LEDGER_ACCOUNT ledger_account
ON 
    trial_balance.ACCOUNT_ID = ledger_account.GL_ACCOUNT_ID
WHERE 
    trial_balance.SUBSIDIARY_ID = %(subsidiary_id)s;