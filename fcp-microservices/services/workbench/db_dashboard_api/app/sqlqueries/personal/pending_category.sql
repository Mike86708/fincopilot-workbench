--Pending tasks by category
--query for entity, period & Personal Tasks
select 
CASE 
        WHEN l.description = 'Close Checklist' THEN 'Close'
        ELSE l.description
   END AS description,
l.description, count(*)
from workbench.tasks t join user_data.fincopilot_user u on t.assigned_performer_id=u.user_id
     JOIN workbench.lookup_code l on cast(t.category as INT8)=l.id
where
l.type='TASK_CATEGORY'
and t.task_status=0 --Pending
and cast(entity_id as varchar)= COALESCE(%s ,cast(entity_id as varchar))  --UI filter value to be passed
and period_id= %s --UI filter value to be passed
and cast(assigned_performer_id as varchar)= COALESCE(%s ,cast(assigned_performer_id as varchar)) --UI filter value to be passed
and u.email= %s  --UI filter value to be passed
group by l.description