--Reviewed/Performed
--query for entity, period & Personal Tasks
select
SUM(CASE WHEN approval_status=1 THEN 1 ELSE 0 END) total_tasks_reviewed,
SUM(CASE WHEN task_status=1 THEN 1 ELSE 0 END) total_tasks_prepared
from workbench.tasks t join user_data.fincopilot_user u on t.actual_approver_id=u.user_id
where cast(entity_id as varchar)= COALESCE(%s ,cast(entity_id as varchar))  --UI filter value to be passedssed
and period_id= %s --UI filter value to be passed
and u.email= %s  --UI filter value to be passed