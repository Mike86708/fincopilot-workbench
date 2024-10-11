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
        SUM(converted_amount_credit) AS total_credit,
        SUM(converted_amount_debit) AS total_debit,
        SUM(converted_amount) AS total_amount
    FROM
        FINCOPILOT_CDM.WORKBENCH.FCT_NETSUITE_BALANCE_SHEET_SUMMARY
    WHERE
        accounting_period_name = %(period_id)s
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
        SUM(converted_amount_credit) AS total_credit,
        SUM(converted_amount_debit) AS total_debit,
        SUM(converted_amount) AS total_amount
    FROM
        FINCOPILOT_CDM.WORKBENCH.FCT_NETSUITE_BALANCE_SHEET_SUMMARY
    WHERE
        accounting_period_name = %(period_id)s
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
        total_credit AS credit,
        total_debit AS debit,
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
        child.total_credit AS credit,
        child.total_debit AS debit,
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
    SUM(credit) AS total_credit,
    SUM(debit) AS total_debit,
    SUM(total_amount) AS total_amount,
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


