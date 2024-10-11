select account_id, account_name, parent_id from fincopilot_cdm.workbench.fct_netsuite_income_statement
where subsidiary_id = %(subsidiary_id)s