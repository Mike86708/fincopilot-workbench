import os
import traceback
from fastapi import HTTPException
current_dir = os.path.dirname(os.path.abspath(__file__))
allpath = os.path.join(current_dir, 'sqlqueries', 'all')
# the path for the SQL Folder that runs the ALL queries
personalpath = os.path.join(current_dir, 'sqlqueries', 'personal')
# the path for the SQL folder that runs persona(for a particular email)

def run_personal(entity_id: int, period_id: int, assigned_performer_id: int, email: str, conn):
    # This runs all needed queries for personal tasks/dashboard info. It is ran when an email is provided
    response_dict = {}
    response_dict["prepare_total"] = {} # Our Json Field for "perpare total", we will have
    try:
        prep_total_file = open(personalpath+"/prep_total.sql","r") # get first sql file
    except Exception as e:
        raise HTTPException(detail=str(e), status_code=500)

    prep_total_script = prep_total_file.read()
    prep_total_file.close()

    with conn.cursor() as cursor:
        # If your SQL file has placeholders like %s for parameters, you can pass them directly.
        # Ensure that your SQL file is using %s placeholders for parameters.
        if entity_id == -1000:
            cursor.execute(prep_total_script, (None, period_id,email))
        else:
            cursor.execute(prep_total_script, (str(entity_id), period_id,email))

        # If you expect results, you can fetch them
        results = cursor.fetchall()
        response_dict["prepare_total"] = {}
        if len(results) > 0:
            row_dict = {}
            for row in results:
                row_dict['total_tasks_prepared'] = row[0]
                row_dict['total_tasks'] = row[1]
            response_dict['prepare_total'] = row_dict

    #review performed
    response_dict["review_performed"] = {}
    review_performed_file = open(personalpath+"/review_performed.sql","r") # get the sql file
    review_performed_script = review_performed_file.read()
    review_performed_file.close()

    with conn.cursor() as cursor:
        # If your SQL file has placeholders like %s for parameters, you can pass them directly.
        # Ensure that your SQL file is using %s placeholders for parameters.
        if entity_id == -1000:
            cursor.execute(review_performed_script, (None, period_id, email))
        else:
            cursor.execute(review_performed_script, (str(entity_id), period_id, email))      
        results = cursor.fetchall()   
        if len(results) > 0:
            row_dict = {}
            for row in results:
                row_dict['total_tasks_reviewed'] = row[0]
                row_dict['total_tasks_prepared'] = row[1]
            response_dict["review_performed"] = row_dict
        else:
            response_dict['review_performed'] = {}      
    #pending_category
    try:
        pending_category_file = open(personalpath+"/pending_category.sql","r") # get the sql file
    except Exception as e:
        raise HTTPException(detail=str(e), status_code=500)
    
    pending_category_script = pending_category_file.read()
    pending_category_file.close()

    with conn.cursor() as cursor:
        # If your SQL file has placeholders like %s for parameters, you can pass them directly.
        # Ensure that your SQL file is using %s placeholders for parameters.
        if entity_id == -1000:
            if assigned_performer_id == -1000:
                cursor.execute(pending_category_script, (None, period_id, None, email))
            else:
                cursor.execute(pending_category_script, (None, period_id, str(assigned_performer_id), email))
        else:
            if assigned_performer_id == -1000:
                cursor.execute(pending_category_script, (str(entity_id), period_id, None, email))
            else:
                cursor.execute(pending_category_script, (str(entity_id), period_id, str(assigned_performer_id), email))


        # If you expect results, you can fetch them
        results = cursor.fetchall()
        print(results)
        if len(results) > 0:
            response_dict['pending_category'] = []
            for row in results:
                row_dict = {}
                row_dict['key'] = row[0]
                row_dict['value'] = row[2]
                response_dict["pending_category"].append(row_dict)

    #Approvals Queue
    response_dict["approvals_queue"] = {}
    try:
        approvals_queue_file = open(personalpath+"/approvals_queue.sql","r") # get the sql file
    except Exception as e:
        raise HTTPException(detail=str(e), status_code=500)

    approvals_queue_script = approvals_queue_file.read()
    approvals_queue_file.close()

    with conn.cursor() as cursor:
        # If your SQL file has placeholders like %s for parameters, you can pass them directly.
        # Ensure that your SQL file is using %s placeholders for parameters.
       

        if entity_id == -1000:
            try:
                cursor.execute(approvals_queue_script, (None, period_id,email))
            except Exception as e:
                print("An error occurred:", e)
                print("Traceback:")
                traceback.print_exc()
        else:
            try:
                cursor.execute(approvals_queue_script, (str(entity_id), period_id, email))
            except Exception as e:
                print("An error occurred:", e)
                print("Traceback:")
                traceback.print_exc()

        
        # If you expect results, you can fetch them
        results = cursor.fetchall()
        if len(results) > 0:
            row_dict = {}
            for row in results:
                row_dict['approvals_in_queue'] = row[0]
            response_dict['approvals_queue'] = row_dict
        else:
            response_dict["approvals_queue"] = {}

    #My OverDue
    response_dict["overdue"] = {}
    my_over_due_file = open(personalpath+"/myoverdue.sql","r") # get the sql file
    my_over_due_script = my_over_due_file.read()
    my_over_due_file.close()

    with conn.cursor() as cursor:
        # If your SQL file has placeholders like %s for parameters, you can pass them directly.
        # Ensure that your SQL file is using %s placeholders for parameters.
        try:
            cursor.execute(my_over_due_script, (email,))
        except Exception as e:
            print(e)
            raise HTTPException(status_code=500, detail=str(e))

        # If you expect results, you can fetch them
        results = cursor.fetchall()

        if len(results) == 0:
            response_dict["overdue"] = []
            return response_dict

        response_dict['overdue'] = []       
        for row in results:
          row_dict = {}
          row_dict['delayed_by'] = row[2]
          row_dict['description'] = row[0]
          row_dict['performer_due_date'] = row[1]
          response_dict["overdue"].append(row_dict)
        
    return(response_dict)

def run_all(entity_id: int, period_id: int, assigned_performer_id: int, assigned_approver_id: int, conn):
    response_dict = {}
    # PREPARE/TOTAL
    response_dict["prepare_total"] = {}
    try:
        prep_total_file = open(allpath+"/prep_total.sql","r") # get first sql file
    except Exception as e:
        raise HTTPException(status_code=500,detail=str(e))
    prep_total_script = prep_total_file.read()
    prep_total_file.close()


    with conn.cursor() as cursor:
        # If your SQL file has placeholders like %s for parameters, you can pass them directly.
        # Ensure that your SQL file is using %s placeholders for parameters.
        if entity_id == -1000:
            cursor.execute(prep_total_script, (None, period_id))
        else:
            cursor.execute(prep_total_script, (str(entity_id), period_id))

        # If you expect results, you can fetch them
        results = cursor.fetchall()
        # column_names = [desc[0] for desc in cursor.description]
        # for column in column_names:
        #     response_dict["prepare_total"][column] = []

        response_dict["prepare_total"] = {}
        if len(results) > 0:
            row_dict = {}
            for row in results:
                row_dict['total_tasks_prepared'] = row[0]
                row_dict['total_tasks'] = row[1]
            response_dict['prepare_total'] = row_dict
        

    
    #Approvals Queue
    response_dict["approvals_queue"] = {}
    try:
        approvals_queue_file = open(allpath+"/approvals_queue.sql","r") # get the sql file
    except:
        raise HTTPException(detail=str(e), status_code=500)
    
    approvals_queue_script = approvals_queue_file.read()
    approvals_queue_file.close()

    with conn.cursor() as cursor:
        # If your SQL file has placeholders like %s for parameters, you can pass them directly.
        # Ensure that your SQL file is using %s placeholders for parameters.
        if entity_id == -1000:
            cursor.execute(approvals_queue_script, (None, period_id))
        else:
            cursor.execute(approvals_queue_script, (str(entity_id), period_id))

        # If you expect results, you can fetch them
        results = cursor.fetchall()
        if len(results) > 0:
            row_dict = {}
            for row in results:
                row_dict['approvals_in_queue'] = row[0]
            response_dict['approvals_queue'] = row_dict
        else:
            response_dict["approvals_queue"] = {}

    
    #pending_category
    response_dict["pending_category"] = {}
    try:
        pending_category_file = open(allpath+"/pending_category.sql","r") # get the sql file
    except Exception as e:
        raise HTTPException(detail=str(e), status_code=500)
    pending_category_script = pending_category_file.read()
    pending_category_file.close()

    with conn.cursor() as cursor:
        # If your SQL file has placeholders like %s for parameters, you can pass them directly.
        # Ensure that your SQL file is using %s placeholders for parameters.
        if entity_id == -1000:
            cursor.execute(pending_category_script, (None, period_id))
        else:
            cursor.execute(pending_category_script, (str(entity_id), period_id))

        # If you expect results, you can fetch them
        results = cursor.fetchall()
        response_dict['pending_category'] = []
        if len(results) > 0:
            for row in results:
                row_dict = {}
                row_dict['key'] = row[0]
                row_dict['value'] = row[2]
                response_dict["pending_category"].append(row_dict)
        else:
            response_dict['pending_category'] = {}

    #review performed
    response_dict["review_performed"] = {}
    try:
        review_performed_file = open(allpath+"/review_performed.sql","r") # get the sql file
    except Exception as e:
        raise HTTPException(detail=str(e), status_code=500)

    review_performed_script = review_performed_file.read()
    review_performed_file.close()

    with conn.cursor() as cursor:
        # If your SQL file has placeholders like %s for parameters, you can pass them directly.
        # Ensure that your SQL file is using %s placeholders for parameters.
        if entity_id == -1000:
            cursor.execute(review_performed_script, (None, period_id))
        else:
            cursor.execute(review_performed_script, (str(entity_id), period_id))


        # If you expect results, you can fetch them
        results = cursor.fetchall()
        if len(results) > 0:
            row_dict = {}
            for row in results:
                row_dict['total_tasks_reviewed'] = row[0]
                row_dict['total_tasks_prepared'] = row[1]
            response_dict["review_performed"] = row_dict
        else:
            response_dict['review_performed'] = {}
        



    return (response_dict)