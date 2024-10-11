from abc import ABC, abstractmethod
import time

class Guard(ABC):
    """Abstract base class for all guards."""
    
    @abstractmethod
    async def check(self, prompt, domain, subject_area, prompt_id, conversation_id,session_id, user_id, language):
        """
        Perform the check for the guard.

        :param prompt: The input prompt to check.
        :param domain: The domain of the prompt.
        :param subject_area: The subject area of the prompt.
        :param prompt_id: The ID of the prompt.
        :param conversation_id: The ID of the conversation.
        :param session_id : The ID of session.
        :param user_id: The ID of the user.
        :param language: The language of the prompt.
        :return: A dictionary with the result of the guard check.
        """
        pass
    
    async def execute(self, *args, **kwargs):
        """
        Execute the guard and measure its execution time.

        :return: A dictionary containing the result and the execution time.
        """
        start_time = time.time()
        result = await self.check(*args, **kwargs)
        execution_time = time.time() - start_time

        return {
            "result": result['result'],
            "details": {
                "result": [result['result'], result],
                "execution_time": execution_time
            }
        }
