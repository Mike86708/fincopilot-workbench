--Prepared/Total
--query for entity, period & All Tasks
select SUM(CASE WHEN task_status=1 THEN 1 ELSE 0 END) total_tasks_prepared,
count(*) total_tasks
from workbench.tasks
where cast(entity_id as varchar)= COALESCE(%s ,cast(entity_id as varchar))  --UI filter value to be passed
and period_id= %s --UI filter value to be passed