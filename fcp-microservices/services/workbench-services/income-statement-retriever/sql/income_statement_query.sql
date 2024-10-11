WITH pre_aggregated_parent AS (
    -- Pre-aggregate credit, debit, and total_amount for each account_id across all rows
    SELECT
        account_id,
        parent_id,
        account_name,
        level_3_account,
        level_2_account,
        level_1_account,
        level_0_rollup,
        SUM(amount_usd) AS total_amount
    FROM
        FINCOPILOT_CDM.WORKBENCH.FCT_NETSUITE_INCOME_STATEMENT
    WHERE
        ACCOUNTING_PERIOD_ID BETWEEN %(from_period_id)s AND %(to_period_id)s
        AND subsidiary_id = %(subsidiary_id)s
		And Parent_id is null
    GROUP BY
        account_id,
        parent_id,
        account_name,
        level_3_account,
        level_2_account,
        level_1_account,
        level_0_rollup
),
pre_aggregated_child AS (
    -- Pre-aggregate credit, debit, and total_amount for each account_id across all rows
    SELECT
        account_id,
        parent_id,
        account_name,
        level_3_account,
        level_2_account,
        level_1_account,
        level_0_rollup,        
        SUM(amount_usd) AS total_amount
    FROM
        FINCOPILOT_CDM.WORKBENCH.FCT_NETSUITE_INCOME_STATEMENT
    WHERE
        ACCOUNTING_PERIOD_ID BETWEEN %(from_period_id)s AND %(to_period_id)s
        AND subsidiary_id = %(subsidiary_id)s
		And Parent_id is Not null
    GROUP BY
        account_id,
        parent_id,
        account_name,
        level_3_account,
        level_2_account,
        level_1_account,
        level_0_rollup
),
hierarchy AS (
    -- Base case: Select the top-level accounts (parent accounts)
    SELECT
        level_3_account,
        level_2_account,
        level_1_account,
        level_0_rollup,
        account_name AS original_account_name,
        account_name AS hierarchy_account_name,
        total_amount,
        parent_id,
        account_id
    FROM
        pre_aggregated_parent
    UNION ALL
    -- Recursive case: Join child accounts to their parents, but use the pre-aggregated amounts
    SELECT
        child.level_3_account,
        child.level_2_account,
        child.level_1_account,
        child.level_0_rollup,
        child.account_name AS original_account_name,
        parent.hierarchy_account_name || ' -> ' || child.account_name AS hierarchy_account_name,
        child.total_amount,
        child.parent_id,
        child.account_id
    FROM
        pre_aggregated_child child
    JOIN hierarchy parent ON child.parent_id = parent.account_id
)
SELECT
    level_3_account,
    level_2_account,
    level_1_account,
    level_0_rollup,
    hierarchy_account_name AS account_hierarchy,
    SUM(total_amount) AS total_amount,  -- Sum only the pre-aggregated amount for each account
    parent_id,
    account_id
FROM
    hierarchy
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