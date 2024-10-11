WITH RECURSIVE hierarchy AS (
    SELECT
        level_3_account,
        level_2_account,
        level_1_account,
        level_0_rollup,
        account_name AS original_account_name,
        account_name AS hierarchy_account_name,
        ending_balance_usd AS total_amount,
        parent_id,
        account_id,
        class_id,
        cost_center_id,
        transaction_accounting_period_id
    FROM
        FINCOPILOT_CDM.WORKBENCH_STAGING.FCT_NETSUITE_TRIAL_BALANCE_SUMMARY
    WHERE
        accounting_period_name = :period_id
        AND subsidiary_id = :subsidiary_id
        AND parent_id IS NULL
    
    UNION ALL

    SELECT
        child.level_3_account,
        child.level_2_account,
        child.level_1_account,
        child.level_0_rollup,
        child.account_name AS original_account_name,
        parent.hierarchy_account_name || ' -> ' || child.account_name AS hierarchy_account_name,
        child.ending_balance_usd AS total_amount,
        child.parent_id,
        child.account_id,
        child.class_id,
        child.cost_center_id,
        child.transaction_accounting_period_id
    FROM
        FINCOPILOT_CDM.WORKBENCH_STAGING.FCT_NETSUITE_TRIAL_BALANCE_SUMMARY child
    JOIN hierarchy parent ON child.parent_id = parent.account_id
    WHERE
        child.accounting_period_name = :period_id
        AND child.subsidiary_id = :subsidiary_id
),


distinct_hierarchy as (
    select  
        level_3_account,
        level_2_account,
        level_1_account,
        level_0_rollup,
        account_id,
        class_id,
        original_account_name,
        hierarchy_account_name,
        total_amount,
        parent_id
    from 
        hierarchy 
    group by 
        level_3_account,
        level_2_account,
        level_1_account,
        level_0_rollup,
        account_id,
        class_id,
        cost_center_id,
        transaction_accounting_period_id,
        original_account_name,
        hierarchy_account_name,
        total_amount,
        parent_id
)
SELECT
    level_3_account,
    level_2_account,
    level_1_account,
    level_0_rollup,
    hierarchy_account_name AS account_hierarchy,
    SUM(total_amount) AS total_amount,
    parent_id,
    account_id
FROM
    distinct_hierarchy
GROUP BY
    level_3_account,
    level_2_account,
    level_1_account,
    level_0_rollup,
    hierarchy_account_name,
    parent_id,
    account_id
ORDER BY
    account_hierarchy;

