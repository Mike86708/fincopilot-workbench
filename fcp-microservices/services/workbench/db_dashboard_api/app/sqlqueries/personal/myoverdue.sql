--My overdue and due today
select  description,performer_due_date, current_date - performer_due_date  delayed_by
from workbench.tasks t join user_data.fincopilot_user u on t.assigned_performer_id=u.user_id
where
t.task_status=0
and t.performer_due_date<=current_date
and u.email= %s --Looged in User ID to be passed as filter