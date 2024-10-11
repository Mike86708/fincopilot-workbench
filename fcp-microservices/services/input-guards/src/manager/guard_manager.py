import os
import asyncio
from datetime import datetime
from typing import List, Dict, Any
from exceptions.exception import InputGuardException
from exceptions.exception_codes import Reason
from guard.nsfw_guard import NSFWGuard
from guard.off_topic_guard import OffTopicGuard
from guard.sql_injection_guard import SQLInjectionGuard
from guard.pii_guard import PIIGuard

class GuardManager:
    def __init__(self):
        """
        Initialize the GuardManager with mode and filters specified by environment variables.

        - `FAIL_FAST_MODE`: If set to "true", the execution stops on the first guard failure.
        - `FILTERS`: A comma-separated list of guards to be applied. 
        """
        self.fail_fast_mode = os.getenv("FAIL_FAST_MODE", "False").lower() == "true"  # Default to "False" if not set
        self.filters = os.getenv("FILTERS", "").split(",")  # Default to empty list if not set

        # Initialize available guards
        all_guards = {
            "nsfw": NSFWGuard(),
            "off_topic": OffTopicGuard(),
            "sql_injection": SQLInjectionGuard(),
            "pii": PIIGuard()
        }

        # Select only the guards specified in the FILTERS environment variable, maintaining the order
        self.guards = {name: all_guards[name] for name in self.filters if name in all_guards}
        
        # Ensure there is always a PII guard in the filters
        if "pii" not in self.guards:
            self.guards["pii"] = all_guards["pii"]

    # Run a single guard asynchronously.
    async def _run_guard(self, guard_name: str, prompt: str, domain: str, subject_area: str,
                         prompt_id: str, conversation_id: str, session_id:str,user_id: str,language: str) -> Dict[str, Any]:
        """
        Run a single guard asynchronously.

        Args:
            guard_name (str): The name of the guard to run.
            prompt (str): The user prompt to be checked.
            domain (str): The domain of the prompt.
            subject_area (str): The subject area of the prompt.
            prompt_id (str): The unique identifier for the prompt.
            user_id (str): The ID of the user.
            conversation_id (str): The ID of the conversation.
            language (str): The language of the prompt.

        Returns:
            Dict[str, Any]: The result and details from the guard.
        """
        guard = self.guards.get(guard_name)
        if not guard:
            raise InputGuardException(
                message=f"Guard '{guard_name}' not found.",
                reason=Reason.INVALID_CONFIGURATION
            )

        start_time = datetime.now()
        try:
            # Execute the guard's check method
            result, details = await guard.check(prompt, domain, subject_area, prompt_id, conversation_id,session_id,user_id, language)
        except Exception as e:
            print('Guard manager -e', type(e), e)
            raise InputGuardException(
                message=f"Error occurred while running the guard '{guard_name}'.",
                reason=Reason.PROCESSING_FAILED,
                e=e
            ) from e
        execution_time = (datetime.now() - start_time).total_seconds()

        return {
            "result": result,
            "details": details,
            "execution_time": execution_time
        }

    #Run guards in "Fail Fast" mode. 
    async def _fail_fast(self, prompt: str, domain: str, subject_area: str,
                        prompt_id: str, conversation_id: str,session_id:str,user_id: str, language: str) -> Dict[str, Any]:
        """
        Run guards in "Fail Fast" mode. If any guard fails, stop and return the result.
        This method now accurately calculates the overall execution time by measuring the
        time from the start of the first guard to the completion of the last guard.
        """
        start_time = datetime.now()  # Record the start time of the entire process

        # Run the first guard
        first_guard_name = self.filters[0]
        first_guard_result = await self._run_guard(first_guard_name, prompt, domain, subject_area, prompt_id, conversation_id,session_id,user_id,language)

        # Prepare a dictionary to store the result details of the guards
        result_details = {
            first_guard_name: first_guard_result["details"]
        }

        # Check the result of the first guard
        if not first_guard_result["result"]:
            # If the first guard fails, record the end time and return the result
            end_time = datetime.now()  # Record end time
            return {
                "result": False,
                "mode": "Fail Fast",
                "details": result_details,
                "overall_execution_time": (end_time - start_time).total_seconds()  # Calculate the total execution time
            }

        # Proceed with the remaining guards if the first guard passes
        remaining_guards = self.filters[1:]
        if not remaining_guards:
            # If there are no remaining guards, record the end time and return the result
            end_time = datetime.now()  # Record end time
            return {
                "result": True,
                "mode": "Fail Fast",
                "details": result_details,
                "overall_execution_time": (end_time - start_time).total_seconds()  # Calculate the total execution time
            }

        # Create a list of tasks to run the remaining guards in parallel
        tasks = [self._run_guard(guard_name, prompt, domain, subject_area, prompt_id, conversation_id, session_id, user_id,language)
                for guard_name in remaining_guards]
        
        # Run all remaining guards in parallel and gather the results
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Record the end time after all remaining guards have completed
        end_time = datetime.now()

        # Process the results of the remaining guards
        for i, res in enumerate(results):
            if isinstance(res, Exception):
                # If an exception occurs during guard execution, raise an InputGuardException
                raise InputGuardException(
                    message=f"Guard '{remaining_guards[i]}' failed during execution.",
                    reason=Reason.UNKNOWN_ERROR,
                    e=res
                )

            if not res["result"]:
                # If any of the remaining guards fail, update the result_details and return the failure result
                result_details[remaining_guards[i]] = res["details"]
                return {
                    "result": False,
                    "mode": "Fail Fast",
                    "details": result_details,
                    "overall_execution_time": (end_time - start_time).total_seconds()  # Calculate the total execution time
                }

            # Update the result_details with the result of the remaining guard
            result_details[remaining_guards[i]] = res["details"]

        # If all remaining guards pass, return the success result with the details of all guards
        return {
            "result": True,
            "mode": "Fail Fast",
            "details": result_details,
            "overall_execution_time": (end_time - start_time).total_seconds()  # Calculate the total execution time
        }


    
    async def _complete_all(self, prompt: str, domain: str, subject_area: str,
                            prompt_id: str, conversation_id: str, session_id:str,user_id: str, language: str) -> Dict[str, Any]:
        """
        Run guards in "Complete All" mode. Execute all guards and return aggregated results.

        Args:
            prompt (str): The user prompt to be checked.
            domain (str): The domain of the prompt.
            subject_area (str): The subject area of the prompt.
            prompt_id (str): The unique identifier for the prompt.
            user_id (str): The ID of the user.
            conversation_id (str): The ID of the conversation.
            language (str): The language of the prompt.

        Returns:
            Dict[str, Any]: Aggregated result and details from all guards.
        """
        start_time = datetime.now()  # Start time before running any guards

        tasks = [
            self._run_guard(guard_name, prompt, domain, subject_area, prompt_id, conversation_id,session_id,user_id, language)
            for guard_name in self.guards
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        end_time = datetime.now()  # End time after all guards have completed

        # Handle any exceptions raised during guard execution
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                raise InputGuardException(
                    message=f"Guard '{list(self.guards.keys())[i]}' failed during execution.",
                    reason=Reason.UNKNOWN_ERROR,
                    e=result
                )

        return {
            "result": all(result["result"] for result in results),
            "mode": "Complete All",
            "details": {
                guard_name: result["details"]
                for guard_name, result in zip(self.guards.keys(), results)
            },
            "overall_execution_time": (end_time - start_time).total_seconds()  # Total time for all guards
        }

    async def run(self, prompt: str, domain: str, subject_area: str,
                  prompt_id: str, conversation_id: str, session_id:str,user_id: str, language: str) -> Dict[str, Any]:
        """
        Run the guards based on the configured mode.

        Args:
            prompt (str): The user prompt to be checked.
            domain (str): The domain of the prompt.
            subject_area (str): The subject area of the prompt.
            prompt_id (str): The unique identifier for the prompt.
            user_id (str): The ID of the user.
            conversation_id (str): The ID of the conversation.
            language (str): The language of the prompt.

        Returns:
            Dict[str, Any]: Aggregated result and details from the guards.
        """
        if not self.filters:
            raise InputGuardException(
                message="No guards configured. Please set the FILTERS environment variable.",
                reason=Reason.INVALID_CONFIGURATION
            )

        first_guard_name = self.filters[0]
        result = await self._run_guard(first_guard_name, prompt, domain, subject_area, prompt_id, conversation_id,session_id,user_id,  language)

        if not result["result"]:
            return {
                "result": False,
                "mode": "Fail Fast",
                "details": {
                    first_guard_name: result["details"]
                },
                "overall_execution_time": result["execution_time"]
            }

        remaining_guards = self.filters[1:]
        if self.fail_fast_mode:
            return await self._fail_fast(prompt, domain, subject_area, prompt_id,  conversation_id,session_id, user_id,language)
        else:
            return await self._complete_all(prompt, domain, subject_area, prompt_id,  conversation_id,session_id, user_id,language)

