Select SUM(CASE WHEN task_status=1 THEN 1 ELSE 0 END) total_tasks_prepared,

count(*) total_tasks

from workbench.tasks t join user_data.fincopilot_user u on t.assigned_performer_id=u.user_id

where cast(entity_id as varchar)= COALESCE(%s ,cast(entity_id as varchar))  --UI filter value to be passed

and period_id = %s --UI filter value to be passed

and u.email= %s;  --UI filter value to be passed

 