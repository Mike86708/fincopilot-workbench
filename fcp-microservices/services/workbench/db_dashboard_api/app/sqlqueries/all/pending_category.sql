--Pending tasks by category
--query for entity, period & All Tasks
select 
CASE 
        WHEN l.description = 'Close Checklist' THEN 'Close'
        ELSE l.description
   END AS description,
l.description, count(*)
from workbench.tasks t, workbench.lookup_code l
where
cast(t.category as INT8)=l.id
and l.type='TASK_CATEGORY'
and t.task_status=0
and cast(entity_id as varchar)= COALESCE(%s ,cast(entity_id as varchar))  --UI filter value to be passedsed
and period_id= %s --UI filter value to be passed
group by l.description