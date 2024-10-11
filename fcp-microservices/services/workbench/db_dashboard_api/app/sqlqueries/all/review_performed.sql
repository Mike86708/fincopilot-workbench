--Reviewed/Performed
--query for entity, period & All Tasks
select
SUM(CASE WHEN approval_status=1 THEN 1 ELSE 0 END) total_tasks_reviewed,
SUM(CASE WHEN task_status=1 THEN 1 ELSE 0 END) total_tasks_prepared
from workbench.tasks
where cast(entity_id as varchar)= COALESCE(%s ,cast(entity_id as varchar))  --UI filter value to be passedsed
and period_id= %s --UI filter value to be passed
-- and assigned_performer_id=1 --UI filter value to be passed