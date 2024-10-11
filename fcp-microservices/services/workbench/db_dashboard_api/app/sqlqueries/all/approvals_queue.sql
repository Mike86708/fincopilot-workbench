--Approvals in Queue
--query for entity, period & All Tasks
select  count(*) approvals_in_queue
from workbench.tasks t
where
t.task_status=1
and t.approval_status=0 --pending approval
and cast(entity_id as varchar)= COALESCE(%s ,cast(entity_id as varchar)) --UI filter value to be passed
and period_id= %s --UI filter value to be passed
 