--Prepared/Total
--query for entity, period & Personal Tasks
select SUM(CASE WHEN task_status=1 THEN 1 ELSE 0 END) total_tasks_prepared,
count(*) total_tasks
from workbench.tasks t join workbench.user u on t.assigned_performer_id=u.id
where entity_id=0  --UI filter value to be passed
and period_id=8  --UI filter value to be passed
and u.email='jane.doe@doordash.com'  --UI filter value to be passed
 
--Prepared/Total
--query for entity, period & All Tasks
select SUM(CASE WHEN task_status=1 THEN 1 ELSE 0 END) total_tasks_prepared,
count(*) total_tasks
from workbench.tasks
where entity_id=0 --UI filter value to be passed
and period_id=8 --UI filter value to be passed
 
--Reviewed/Performed
--query for entity, period & All Tasks
select
SUM(CASE WHEN approval_status=1 THEN 1 ELSE 0 END) total_tasks_reviewed,
SUM(CASE WHEN task_status=1 THEN 1 ELSE 0 END) total_tasks_prepared
from workbench.tasks
where entity_id=0 --UI filter value to be passed
and period_id=8 --UI filter value to be passed
and assigned_performer_id=1 --UI filter value to be passed
 
--Reviewed/Performed
--query for entity, period & Personal Tasks
select
SUM(CASE WHEN approval_status=1 THEN 1 ELSE 0 END) total_tasks_reviewed,
SUM(CASE WHEN task_status=1 THEN 1 ELSE 0 END) total_tasks_prepared
from workbench.tasks t join workbench.user u on t.assigned_performer_id=u.id
where entity_id=0 --UI filter value to be passed
and period_id=8 --UI filter value to be passed
and u.email='jane.doe@doordash.com'  --UI filter value to be passed
 
--Pending tasks by category
--query for entity, period & Personal Tasks
select l.description, count(*)
from workbench.tasks t join workbench.user u on t.assigned_performer_id=u.id
     JOIN workbench.lookup_code l on cast(t.category as INT8)=l.id
where
l.type='TASK_CATEGORY'
and t.task_status=0 --Pending
and entity_id=0 --UI filter value to be passed
and period_id=8 --UI filter value to be passed
and assigned_performer_id=1 --UI filter value to be passed
and u.email='jane.doe@doordash.com'  --UI filter value to be passed
group by l.description
 
 
--Pending tasks by category
--query for entity, period & All Tasks
select l.description, count(*)
from workbench.tasks t, workbench.lookup_code l
where
cast(t.category as INT8)=l.id
and l.type='TASK_CATEGORY'
and t.task_status=0
and entity_id=0 --UI filter value to be passed
and period_id=8 --UI filter value to be passed
group by l.description
 
--Approvals in Queue
--query for entity, period & Personal Tasks
select  count(*) approvals_in_queue
from workbench.tasks t join workbench.user u on t.assigned_performer_id=u.id
where
t.task_status=1
and t.approval_status=0  --pending approval
and entity_id=0 --UI filter value to be passed
and period_id=8 --UI filter value to be passed
and assigned_approver_id=1  --UI filter value to be passed
and u.email='jane.doe@doordash.com'  --UI filter value to be passed
 
 
 
--Approvals in Queue
--query for entity, period & All Tasks
select  count(*) approvals_in_queue
from workbench.tasks t
where
t.task_status=1
and t.approval_status=0 --pending approval
and entity_id=0 --UI filter value to be passed
and period_id=8 --UI filter value to be passed
 
 
--My overdue and due today
select  description,performer_due_date, current_date - performer_due_date  delayed_by
from workbench.tasks t join workbench.user u on t.assigned_performer_id=u.id
where
t.task_status=0
and t.performer_due_date<=current_date
and u.email="" --Looged in User ID to be passed as filter